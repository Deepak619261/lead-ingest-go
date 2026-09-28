package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"leadingest/internal/metrics"
	"leadingest/internal/pipeline"
	"leadingest/internal/retry"
	"leadingest/internal/sink"
)

// server runs imports over HTTP: the upload body streams straight into the pipeline (no temp file).
type server struct {
	c        common
	sink     sink.Sink
	metrics  *metrics.Metrics
	slots    chan struct{} // limits concurrent imports
	hardCtx  context.Context
	draining atomic.Bool

	mu      sync.Mutex
	running map[string]chan struct{} // import id -> soft-stop channel
	seq     atomic.Int64
}

// serveOpts are the serve-only settings.
type serveOpts struct {
	maxImports   int
	drain, grace time.Duration
}

func serveCmd(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	var c common
	c.register(fs)
	addr := fs.String("addr", ":8080", "listen address")
	var o serveOpts
	fs.IntVar(&o.maxImports, "max-imports", 2, "concurrent imports; more get 429")
	fs.DurationVar(&o.drain, "drain", 30*time.Second, "on shutdown: let running imports finish for this long")
	fs.DurationVar(&o.grace, "grace", 15*time.Second, "then soft-stop them and allow this long to flush + checkpoint")
	_ = fs.Parse(args)
	c.logger()

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	s, _, err := c.openSink(sigCtx)
	if err != nil {
		return err
	}
	defer s.Close()
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	return runServer(sigCtx, c, s, ln, o)
}

// runServer serves until ctx is cancelled, then shuts down gracefully.
func runServer(ctx context.Context, c common, s sink.Sink, ln net.Listener, o serveOpts) error {
	hardCtx, hardCancel := context.WithCancel(context.Background())
	defer hardCancel()

	srv := &server{c: c, sink: s, metrics: metrics.New(), slots: make(chan struct{}, o.maxImports),
		hardCtx: hardCtx, running: map[string]chan struct{}{}}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /imports", srv.handleImport)
	mux.HandleFunc("DELETE /imports/{id}", srv.handleCancel)
	mux.HandleFunc("GET /imports", srv.handleList)
	mux.HandleFunc("GET /healthz", srv.handleHealth)
	mux.Handle("GET /metrics", srv.metrics.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)

	httpSrv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- httpSrv.Serve(ln) }()
	slog.Info("ingest service listening", "addr", ln.Addr().String(), "sink", c.sinkKind)

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// Graceful shutdown: (1) not ready + stop accepting, (2) drain, (3) soft stop, (4) hard stop.
	slog.Info("shutdown: draining", "drain", o.drain, "grace", o.grace)
	srv.draining.Store(true)
	shutdownDone := make(chan error, 1)
	go func() {
		sctx, cancel := context.WithTimeout(context.Background(), o.drain+o.grace+5*time.Second)
		defer cancel()
		shutdownDone <- httpSrv.Shutdown(sctx) // waits for in-flight import requests
	}()
	timer := time.NewTimer(o.drain)
	select {
	case err := <-shutdownDone:
		slog.Info("shutdown complete: all imports finished")
		return ignoreClosed(err)
	case <-timer.C:
	}
	slog.Warn("drain timeout: soft-stopping running imports (they flush and checkpoint)")
	srv.stopAll()
	timer.Reset(o.grace)
	select {
	case err := <-shutdownDone:
		slog.Info("shutdown complete after soft stop")
		return ignoreClosed(err)
	case <-timer.C:
	}
	slog.Error("grace timeout: aborting imports")
	hardCancel()
	return ignoreClosed(<-shutdownDone)
}

func ignoreClosed(err error) error {
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// POST /imports?account=1&on_exists=update&resume=0 with a raw CSV body or multipart field "file".
func (s *server) handleImport(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		w.Header().Set("Retry-After", "30")
		http.Error(w, "too many concurrent imports", http.StatusTooManyRequests)
		return
	}

	q := r.URL.Query()
	account, err := strconv.ParseInt(q.Get("account"), 10, 64)
	if err != nil || account <= 0 {
		http.Error(w, "account query parameter is required", http.StatusBadRequest)
		return
	}
	resume, _ := strconv.Atoi(q.Get("resume"))
	body, err := csvBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	id := q.Get("import_id")
	if id == "" {
		id = fmt.Sprintf("imp-%d-%d", time.Now().Unix(), s.seq.Add(1))
	}
	stop := make(chan struct{})
	s.mu.Lock()
	s.running[id] = stop
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.running, id); s.mu.Unlock() }()

	cfg := s.c.pipelineConfig(id, account)
	cfg.Metrics, cfg.Stop, cfg.ResumeAfter = s.metrics, stop, resume
	if v := q.Get("on_exists"); v != "" {
		cfg.Mode = sink.Mode(v)
	}

	// hardCtx (not r.Context()) so a client disconnect is a soft event, not a mid-batch abort.
	ctx, cancel := context.WithCancel(s.hardCtx)
	defer cancel()
	go func() {
		select {
		case <-r.Context().Done():
			closeOnce(stop)
		case <-ctx.Done():
		}
	}()

	rep, err := pipeline.Run(ctx, cfg, body, s.sink)
	status := http.StatusOK
	switch {
	case err == nil:
	case retry.IsFatal(err):
		status = http.StatusUnprocessableEntity
	default:
		status = http.StatusInternalServerError
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(rep)
}

// csvBody returns the upload as a stream: the multipart "file" part, or the raw body.
func csvBody(r *http.Request) (io.Reader, error) {
	mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mt != "multipart/form-data" {
		return r.Body, nil
	}
	mr, err := r.MultipartReader()
	if err != nil {
		return nil, err
	}
	for {
		part, err := mr.NextPart()
		if err != nil {
			return nil, fmt.Errorf("multipart: no \"file\" part: %w", err)
		}
		if part.FormName() == "file" {
			return part, nil
		}
	}
}

func (s *server) handleCancel(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	stop, ok := s.running[r.PathValue("id")]
	s.mu.Unlock()
	if !ok {
		http.Error(w, "no such running import", http.StatusNotFound)
		return
	}
	closeOnce(stop)
	w.WriteHeader(http.StatusAccepted)
}

func (s *server) handleList(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	ids := make([]string, 0, len(s.running))
	for id := range s.running {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{"running": ids})
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	if s.draining.Load() {
		http.Error(w, "draining", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ok"))
}

func (s *server) stopAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, stop := range s.running {
		closeOnce(stop)
	}
}

var closeMu sync.Mutex

// closeOnce closes ch unless already closed (closing twice panics).
func closeOnce(ch chan struct{}) {
	closeMu.Lock()
	defer closeMu.Unlock()
	select {
	case <-ch:
	default:
		close(ch)
	}
}
