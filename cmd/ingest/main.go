// ingest imports lead CSVs: `ingest run -file leads.csv ...` (CLI) or `ingest serve` (HTTP service).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"time"

	"leadingest/internal/pipeline"
	"leadingest/internal/retry"
	"leadingest/internal/sink"
	"leadingest/internal/sink/httpsink"
	"leadingest/internal/sink/pgsink"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: ingest run|serve [flags]   (ingest run -h for flags)")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = runCmd(os.Args[2:])
	case "serve":
		err = serveCmd(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		slog.Error("ingest failed", "err", err)
		os.Exit(1)
	}
}

// common holds flags shared by run and serve.
type common struct {
	sinkKind, engineURL, token, pgURL string
	pgReset                           bool
	timeout                           time.Duration
	workers, shards, batch, queue     int
	flush                             time.Duration
	retries, maxFails                 int
	onExists, logLevel                string
}

func (c *common) register(fs *flag.FlagSet) {
	fs.StringVar(&c.sinkKind, "sink", "mock", "mock (HTTP lead API) | pg (Postgres)")
	fs.StringVar(&c.engineURL, "engine-url", "http://localhost:8090", "HTTP lead API base URL")
	fs.StringVar(&c.token, "token", "", "bearer token for the HTTP lead API")
	fs.StringVar(&c.pgURL, "pg-url", os.Getenv("LEADINGEST_PG_URL"), "Postgres URL (default $LEADINGEST_PG_URL)")
	fs.BoolVar(&c.pgReset, "pg-reset", false, "TRUNCATE prospect tables first (benchmarks)")
	fs.DurationVar(&c.timeout, "timeout", 10*time.Second, "per-request timeout to the lead API")
	fs.IntVar(&c.workers, "workers", runtime.NumCPU(), "validation workers")
	fs.IntVar(&c.shards, "shards", 4, "concurrent writers (max in-flight batches to the engine)")
	fs.IntVar(&c.batch, "batch", 500, "leads per batch")
	fs.IntVar(&c.queue, "queue", 1000, "channel capacity (backpressure bound)")
	fs.DurationVar(&c.flush, "flush", time.Second, "flush partial batches after this long")
	fs.IntVar(&c.retries, "retries", 5, "max attempts per batch")
	fs.IntVar(&c.maxFails, "max-consecutive-failures", 5, "abort after this many failed batches in a row")
	fs.StringVar(&c.onExists, "on-exists", "update", "update (fill missing fields) | skip")
	fs.StringVar(&c.logLevel, "log", "info", "debug|info|warn|error")
}

func (c *common) logger() {
	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(c.logLevel))
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))
}

// openSink builds the configured sink; lookuper is non-nil for legacy mode.
func (c *common) openSink(ctx context.Context) (sink.Sink, sink.Lookuper, error) {
	switch c.sinkKind {
	case "mock":
		s := httpsink.New(httpsink.Config{BaseURL: c.engineURL, Token: c.token, Timeout: c.timeout, MaxConns: c.shards * 2})
		return s, s, nil
	case "pg":
		if c.pgURL == "" {
			return nil, nil, retry.Fatal(fmt.Errorf("-pg-url or LEADINGEST_PG_URL is required for -sink pg"))
		}
		s, err := pgsink.New(ctx, c.pgURL, int32(c.shards*2))
		if err != nil {
			return nil, nil, err
		}
		if err := s.EnsureSchema(ctx); err != nil {
			s.Close()
			return nil, nil, err
		}
		if c.pgReset {
			if err := s.Reset(ctx); err != nil {
				s.Close()
				return nil, nil, err
			}
		}
		return s, s, nil
	}
	return nil, nil, retry.Fatal(fmt.Errorf("unknown -sink %q", c.sinkKind))
}

func (c *common) pipelineConfig(importID string, account int64) pipeline.Config {
	return pipeline.Config{
		ImportID: importID, AccountID: account, Mode: sink.Mode(c.onExists), SinkName: c.sinkKind,
		Workers: c.workers, Shards: c.shards, BatchSize: c.batch, QueueSize: c.queue, FlushInterval: c.flush,
		Retry:                  retry.Policy{MaxAttempts: c.retries, BaseDelay: 100 * time.Millisecond, MaxDelay: 5 * time.Second},
		MaxConsecutiveFailures: c.maxFails,
		Logger:                 slog.Default(),
	}
}
