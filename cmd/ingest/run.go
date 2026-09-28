package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"leadingest/internal/checkpoint"
	"leadingest/internal/legacy"
	"leadingest/internal/metrics"
	"leadingest/internal/pipeline"
	"leadingest/internal/report"
)

func runCmd(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var c common
	c.register(fs)
	file := fs.String("file", "", "CSV file to import (required)")
	account := fs.Int64("account", 1, "account id (tenant)")
	mode := fs.String("mode", "pipeline", "pipeline | legacy (models a per-row legacy importer)")
	lookups := fs.Int("lookups", 8, "legacy: sequential round-trips per row")
	importID := fs.String("import-id", "", "import id (default: file name)")
	ckptPath := fs.String("checkpoint", "", "checkpoint file; enables resume after a crash or Ctrl-C")
	rejectsPath := fs.String("rejects", "", "write rejected rows (row,stage,key,reason) to this CSV")
	metricsAddr := fs.String("metrics-addr", "", "serve /metrics and /debug/pprof here during the run, e.g. :9091")
	reportJSON := fs.String("report-json", "", "also write the report as JSON here (benchmarks)")
	_ = fs.Parse(args)
	c.logger()

	if *file == "" {
		return errors.New("-file is required")
	}
	if *importID == "" {
		*importID = strings.TrimSuffix(filepath.Base(*file), filepath.Ext(*file))
	}

	// First Ctrl-C = soft stop (flush + checkpoint); second = hard cancel.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := make(chan struct{})
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		slog.Warn("stopping: finishing in-flight batches (Ctrl-C again to abort)")
		close(stop)
		<-sigs
		slog.Warn("aborting")
		cancel()
	}()

	var m *metrics.Metrics
	if *metricsAddr != "" {
		m = metrics.New()
		mux := http.DefaultServeMux // pprof registers itself here
		mux.Handle("/metrics", m.Handler())
		go func() { _ = http.ListenAndServe(*metricsAddr, mux) }()
		slog.Info("metrics and pprof", "addr", *metricsAddr)
	}

	s, lk, err := c.openSink(ctx)
	if err != nil {
		return err
	}
	defer s.Close()

	f, err := os.Open(*file)
	if err != nil {
		return err
	}
	defer f.Close()

	var rep report.Report
	if *mode == "legacy" {
		rep, err = legacy.Run(ctx, legacy.Config{
			ImportID: *importID, AccountID: *account, SinkName: c.sinkKind, ChunkSize: c.batch,
			LookupsPerRow: *lookups, Collector: report.New(m),
		}, f, s, lk)
	} else {
		cfg := c.pipelineConfig(*importID, *account)
		cfg.Metrics, cfg.Stop = m, stop
		if *ckptPath != "" {
			st, err := checkpoint.Load(*ckptPath)
			if err != nil {
				return err
			}
			if st.ImportID == *importID && st.AccountID == *account && st.Watermark > 0 {
				cfg.ResumeAfter = st.Watermark
				slog.Info("resuming from checkpoint", "after_row", st.Watermark)
			}
			cfg.OnCheckpoint = func(w int) {
				if err := checkpoint.Save(*ckptPath, checkpoint.State{ImportID: *importID, AccountID: *account, Watermark: w}); err != nil {
					slog.Warn("checkpoint save failed", "err", err)
				}
			}
		}
		if *rejectsPath != "" {
			rf, err := os.Create(*rejectsPath)
			if err != nil {
				return err
			}
			defer rf.Close()
			cfg.Rejects = rf
		}
		rep, err = pipeline.Run(ctx, cfg, f, s)
	}

	rep.Print(os.Stdout)
	if *reportJSON != "" {
		if b, jerr := json.MarshalIndent(rep, "", "  "); jerr == nil {
			_ = os.WriteFile(*reportJSON, b, 0o644)
		}
	}
	if rep.Cancelled && err == nil {
		fmt.Printf("stopped early: re-run the same command with -checkpoint %s to resume after row %d\n", *ckptPath, rep.Checkpoint)
	}
	return err
}
