// Package httpsink posts batches to an HTTP lead API: 429/5xx/timeout retryable, 401/403 fatal, other 4xx permanent.
package httpsink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"leadingest/internal/lead"
	"leadingest/internal/retry"
	"leadingest/internal/sink"
)

// Config for the client.
type Config struct {
	BaseURL  string        // e.g. http://localhost:8090
	Token    string        // optional bearer token
	Timeout  time.Duration // per request, e.g. 10s
	MaxConns int           // keep-alive connections to the engine (>= writer concurrency)
}

// Sink implements sink.Sink and sink.Lookuper.
type Sink struct {
	cfg    Config
	client *http.Client
}

var (
	_ sink.Sink     = (*Sink)(nil) // compile-time interface checks
	_ sink.Lookuper = (*Sink)(nil)
)

// New tunes the pool: the default Transport keeps only 2 idle conns per host.
func New(cfg Config) *Sink {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxConns == 0 {
		cfg.MaxConns = 32
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = cfg.MaxConns
	tr.MaxIdleConnsPerHost = cfg.MaxConns
	tr.MaxConnsPerHost = cfg.MaxConns
	return &Sink{cfg: cfg, client: &http.Client{Transport: tr, Timeout: cfg.Timeout}}
}

// Write POSTs one batch.
func (s *Sink) Write(ctx context.Context, b sink.Batch) (sink.Result, error) {
	body, err := json.Marshal(b)
	if err != nil {
		return sink.Result{}, fmt.Errorf("encode batch: %w", err) // a bug, not retryable
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.BaseURL+"/v1/leads/bulk", bytes.NewReader(body))
	if err != nil {
		return sink.Result{}, retry.Fatal(fmt.Errorf("build request: %w", err)) // bad BaseURL = config error
	}
	req.Header.Set("Content-Type", "application/json")

	var res sink.Result
	err = s.do(ctx, req, &res)
	return res, err
}

// LookupOne is one round-trip existence check (legacy runner only).
func (s *Sink) LookupOne(ctx context.Context, _ int64, l lead.Lead) error {
	u := s.cfg.BaseURL + "/v1/leads/lookup?key=" + url.QueryEscape(l.Key())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return retry.Fatal(err)
	}
	var out struct{ Found bool }
	return s.do(ctx, req, &out)
}

// do sends req, classifies the outcome, and decodes a 200 body into out.
func (s *Sink) do(ctx context.Context, req *http.Request, out any) error {
	if s.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.cfg.Token)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err() // we were cancelled: not the server's fault, don't retry
		}
		return retry.Retryable(fmt.Errorf("request: %w", err), 0) // timeout, reset, refused
	}
	defer func() {
		// Drain + close so the keep-alive connection can be reused.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		_ = resp.Body.Close()
	}()

	switch code := resp.StatusCode; {
	case code == http.StatusOK:
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return retry.Retryable(fmt.Errorf("decode response: %w", err), 0)
		}
		return nil
	case code == http.StatusTooManyRequests:
		return retry.Retryable(statusErr(resp), retryAfter(resp.Header.Get("Retry-After")))
	case code >= 500:
		return retry.Retryable(statusErr(resp), 0)
	case code == http.StatusUnauthorized || code == http.StatusForbidden:
		return retry.Fatal(statusErr(resp))
	default:
		return statusErr(resp) // permanent for this batch
	}
}

// ErrStatus lets callers inspect the HTTP status with errors.As.
type ErrStatus struct {
	Code int
	Body string
}

func (e *ErrStatus) Error() string { return fmt.Sprintf("lead api: HTTP %d: %s", e.Code, e.Body) }

// HTTPStatus lets other packages read the code without importing httpsink.
func (e *ErrStatus) HTTPStatus() int { return e.Code }

func statusErr(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return &ErrStatus{Code: resp.StatusCode, Body: string(bytes.TrimSpace(b))}
}

// retryAfter parses the seconds form of Retry-After; anything else falls back to normal backoff.
func retryAfter(h string) time.Duration {
	if secs, err := strconv.Atoi(h); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return 0
}

// Close releases idle connections.
func (s *Sink) Close() error {
	s.client.CloseIdleConnections()
	return nil
}

// IsStatus reports whether err carries the given HTTP status.
func IsStatus(err error, code int) bool {
	var es *ErrStatus
	return errors.As(err, &es) && es.Code == code
}
