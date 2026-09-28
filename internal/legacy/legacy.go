// Package legacy models a typical per-row importer as a fair benchmark baseline.
package legacy

import (
	"context"
	"errors"
	"io"
	"time"

	"leadingest/internal/csvstream"
	"leadingest/internal/lead"
	"leadingest/internal/report"
	"leadingest/internal/sink"
)

// Config for a legacy run.
type Config struct {
	ImportID      string
	AccountID     int64
	Mode          sink.Mode
	SinkName      string
	ChunkSize     int // rows per chunk (500)
	LookupsPerRow int // sequential single-row DB round-trips per row (dedupe, permissions, field lookups)
	Collector     *report.Collector
}

// Run: load ALL rows into memory, then for each 500-row chunk process rows one by one, then bulk-write.
func Run(ctx context.Context, cfg Config, src io.Reader, s sink.Sink, lk sink.Lookuper) (report.Report, error) {
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 500
	}
	if cfg.Mode == "" {
		cfg.Mode = sink.ModeUpdateMissing
	}
	col := cfg.Collector
	if col == nil {
		col = report.New(nil)
	}
	rep := report.Report{ImportID: cfg.ImportID, Mode: "legacy", Sink: cfg.SinkName, Workers: 1, Shards: 1, BatchSize: cfg.ChunkSize}
	stop := col.StartSampler(100*time.Millisecond, func() map[string]int { return nil })
	defer stop()

	rd, err := csvstream.NewReader(src)
	if err != nil {
		rep.Error = err.Error()
		return col.Finish(rep), err
	}
	mapping, err := lead.NewMapping(rd.Header())
	if err != nil {
		rep.Error = err.Error()
		return col.Finish(rep), err
	}

	// Step 1: whole file -> list of rows before any processing.
	t0 := time.Now()
	var all []csvstream.Row
	for {
		row, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			rep.Error = err.Error()
			return col.Finish(rep), err
		}
		all = append(all, row)
		col.RowRead()
	}
	col.Time(report.ReadBusy, time.Since(t0))

	// Step 2: offset/limit chunk loop, one row at a time.
	for start := 0; start < len(all); start += cfg.ChunkSize {
		if err := ctx.Err(); err != nil {
			rep.Error = err.Error()
			rep.Cancelled = true
			return col.Finish(rep), err
		}
		chunk := all[start:min(start+cfg.ChunkSize, len(all))]
		var batch []lead.Lead

		for _, row := range chunk {
			if row.Err != nil {
				col.Row(report.Malformed, 1)
				continue
			}
			tw := time.Now()
			l := mapping.Build(cfg.AccountID, row)
			l.Normalize()
			verr := l.Validate()
			col.Time(report.WorkerBusy, time.Since(tw))
			if verr != nil {
				col.Row(report.Invalid, 1)
				continue
			}
			// Per-row sequential round-trips: dedupe by email/linkedin/phone, permission, field values, tags...
			for range cfg.LookupsPerRow {
				tl := time.Now()
				if err := lk.LookupOne(ctx, cfg.AccountID, l); err != nil {
					rep.Error = err.Error()
					return col.Finish(rep), err
				}
				col.Lookup(time.Since(tl))
			}
			batch = append(batch, l) // no in-file dedupe
		}

		if len(batch) == 0 {
			continue
		}
		ts := time.Now()
		res, err := s.Write(ctx, sink.Batch{ImportID: cfg.ImportID, AccountID: cfg.AccountID, Mode: cfg.Mode, Leads: batch})
		col.SinkAttempt(time.Since(ts), err)
		if err != nil { // no retries in the legacy path
			col.Row(report.Failed, len(batch))
			continue
		}
		col.Batch(len(batch))
		col.Row(report.Inserted, res.Inserted)
		col.Row(report.Updated, res.Updated)
		col.Row(report.Skipped, res.Skipped)
	}
	rep.Checkpoint = len(all)
	return col.Finish(rep), nil
}
