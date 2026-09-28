// mockengine runs the fake lead API: go run ./cmd/mockengine -addr :8090 -latency 30ms -per-lead 50us
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"leadingest/internal/mockengine"
)

func main() {
	var c mockengine.Config
	addr := flag.String("addr", ":8090", "listen address")
	flag.DurationVar(&c.BaseLatency, "latency", 30*time.Millisecond, "fixed cost per bulk request")
	flag.DurationVar(&c.PerLead, "per-lead", 50*time.Microsecond, "extra cost per lead in a bulk request")
	flag.DurationVar(&c.LookupLatency, "lookup-latency", time.Millisecond, "cost of one single-row lookup (one DB round-trip)")
	flag.Float64Var(&c.P429, "p429", 0, "probability of 429")
	flag.Float64Var(&c.P500, "p500", 0, "probability of 500")
	flag.Float64Var(&c.PSlow, "pslow", 0, "probability of a hung request (client timeout)")
	flag.DurationVar(&c.SlowFor, "slow-for", 30*time.Second, "how long a hung request hangs")
	flag.DurationVar(&c.RetryAfter, "retry-after", time.Second, "Retry-After sent with 429")
	flag.IntVar(&c.MaxInFlight, "max-inflight", 0, "429 above this many concurrent bulk requests (0 = unlimited)")
	flag.StringVar(&c.Token, "token", "", "required bearer token (empty = no auth)")
	flag.IntVar(&c.Capacity, "capacity", 0, "requests served at once, others queue (0 = unlimited)")
	flag.Parse()

	srv := &http.Server{Addr: *addr, Handler: mockengine.New(c), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("mock lead API listening", "addr", *addr, "config", c)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
}
