// Package pipeline is the concurrent import engine.
package pipeline

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"leadingest/internal/checkpoint"
	"leadingest/internal/csvstream"
	"leadingest/internal/lead"
	"leadingest/internal/metrics"
	"leadingest/internal/report"
	"leadingest/internal/retry"
	"leadingest/internal/sink"
)

// ErrDownstreamUnavailable: too many consecutive batch failures.
var ErrDownstreamUnavailable = errors.New("pipeline: downstream unavailable (too many consecutive batch failures)")

// Config for one import.
type Config struct {
	ImportID  string
	AccountID int64
	Mode      sink.Mode
	SinkName  string // label for the report

	Workers       int           // validation goroutines (CPU work)
	Shards        int           // concurrent writers = max in-flight requests to the downstream
	BatchSize     int           // leads per sink.Write
	FlushInterval time.Duration // flush a partial batch after this long (slow/streaming uploads)
	QueueSize     int           // capacity of rowsCh and resultsCh

	Retry                  retry.Policy
	MaxConsecutiveFailures int

	ResumeAfter     int                 // skip rows <= this (from a checkpoint)
	OnCheckpoint    func(watermark int) // called every CheckpointEvery and at the end (may be nil)
	CheckpointEvery time.Duration
	Rejects         io.Writer       // CSV of rejected rows: row,stage,key,reason (may be nil)
	Stop            <-chan struct{} // close for a soft stop (may be nil)

	Metrics *metrics.Metrics // may be nil
	Logger  *slog.Logger     // may be nil
}

func (c *Config) setDefaults() {
	if c.Workers == 0 {
		c.Workers = runtime.NumCPU()
	}
	if c.Shards == 0 {
		c.Shards = 4
	}
	if c.BatchSize == 0 {
		c.BatchSize = 500
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = time.Second
	}
	if c.QueueSize == 0 {
		c.QueueSize = 1000
	}
	if c.Retry.MaxAttempts == 0 {
		c.Retry = retry.DefaultPolicy
	}
	if c.MaxConsecutiveFailures == 0 {
		c.MaxConsecutiveFailures = 5
	}
	if c.CheckpointEvery == 0 {
		c.CheckpointEvery = time.Second
	}
	if c.Mode == "" {
		c.Mode = sink.ModeUpdateMissing
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Validate rejects bad configuration. That's a FATAL error: fail before reading any row.
func (c Config) Validate() error {
	var errs []error
	check := func(ok bool, msg string) {
		if !ok {
			errs = append(errs, errors.New(msg))
		}
	}
	check(c.AccountID > 0, "account id must be > 0")
	check(c.Workers >= 1 && c.Workers <= 1024, "workers must be 1..1024")
	check(c.Shards >= 1 && c.Shards <= 256, "shards must be 1..256")
	check(c.BatchSize >= 1 && c.BatchSize <= 10_000, "batch size must be 1..10000")
	check(c.QueueSize >= 1, "queue size must be >= 1")
	check(c.Mode == sink.ModeUpdateMissing || c.Mode == sink.ModeSkipExisting, "mode must be update|skip")
	check(c.ResumeAfter >= 0, "resume row must be >= 0")
	return errors.Join(errs...)
}

// result is what a worker hands to the router.
type result struct {
	row       int
	lead      lead.Lead
	err       error
	malformed bool
}

// Run imports src into s; the report is filled in even when an error is returned.
func Run(ctx context.Context, cfg Config, src io.Reader, s sink.Sink) (report.Report, error) {
	cfg.setDefaults()
	col := report.New(cfg.Metrics)
	rep := report.Report{
		ImportID: cfg.ImportID, Mode: "pipeline", Sink: cfg.SinkName,
		Workers: cfg.Workers, Shards: cfg.Shards, BatchSize: cfg.BatchSize,
	}
	fail := func(err error) (report.Report, error) {
		rep.Error = err.Error()
		return col.Finish(rep), err
	}

	if err := cfg.Validate(); err != nil {
		return fail(retry.Fatal(fmt.Errorf("invalid config: %w", err)))
	}
	if cfg.Metrics != nil {
		cfg.Metrics.ImportsInFlight.Inc()
		defer cfg.Metrics.ImportsInFlight.Dec()
	}

	// Header problems are fatal and detected before any goroutine starts.
	rd, err := csvstream.NewReader(src)
	if err != nil {
		return fail(retry.Fatal(err))
	}
	mapping, err := lead.NewMapping(rd.Header())
	if err != nil {
		return fail(retry.Fatal(err))
	}

	p := &run{
		cfg:     cfg,
		col:     col,
		sink:    s,
		mapping: mapping,
		tracker: checkpoint.NewTracker(cfg.ResumeAfter),
		rejects: newRejects(cfg.Rejects),
		rowsCh:  make(chan csvstream.Row, cfg.QueueSize),
		resCh:   make(chan result, cfg.QueueSize),
		shardCh: make([]chan lead.Lead, cfg.Shards),
	}
	for i := range p.shardCh {
		p.shardCh[i] = make(chan lead.Lead, cfg.BatchSize)
	}

	stopSampler := col.StartSampler(100*time.Millisecond, p.queueDepths)
	stopCkpt := p.startCheckpointer()

	// errgroup: the first goroutine to return an error cancels gctx for all the others.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return p.produce(gctx, rd) })
	g.Go(func() error { p.runWorkers(gctx); return nil })
	g.Go(func() error { return p.route(gctx) })
	for i := range cfg.Shards {
		g.Go(func() error { return p.write(gctx, p.shardCh[i]) })
	}
	err = g.Wait()

	stopCkpt()
	stopSampler()
	p.rejects.flush()

	rep.Checkpoint = p.tracker.Watermark()
	if cfg.OnCheckpoint != nil {
		cfg.OnCheckpoint(rep.Checkpoint)
	}
	rep.Cancelled = p.softStopped.Load() || ctx.Err() != nil
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		rep.Error = err.Error()
	}
	return col.Finish(rep), err
}

type run struct {
	cfg     Config
	col     *report.Collector
	sink    sink.Sink
	mapping lead.Mapping
	tracker *checkpoint.Tracker
	rejects *rejects

	rowsCh  chan csvstream.Row
	resCh   chan result
	shardCh []chan lead.Lead

	softStopped         atomic.Bool
	consecutiveFailures atomic.Int64
}

func (p *run) produce(ctx context.Context, rd *csvstream.Reader) error {
	defer close(p.rowsCh) // closing tells the workers "no more rows"
	for {
		if isClosed(p.cfg.Stop) {
			p.softStopped.Store(true)
			return nil
		}

		t0 := time.Now()
		row, err := rd.Next()
		p.col.Time(report.ReadBusy, time.Since(t0))
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return retry.Fatal(fmt.Errorf("read csv: %w", err)) // the stream itself broke
		}
		if row.Num <= p.cfg.ResumeAfter {
			continue // already imported before the restart
		}

		// Backpressure: a full queue blocks here, so reading pauses until workers catch up.
		t1 := time.Now()
		select {
		case p.rowsCh <- row:
			p.col.RowRead()
		case <-p.cfg.Stop: // nil channel => never ready
			p.softStopped.Store(true)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
		p.col.Time(report.ReadBlocked, time.Since(t1))
	}
}

func (p *run) runWorkers(ctx context.Context) {
	var wg sync.WaitGroup
	for range p.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.work(ctx)
		}()
	}
	wg.Wait()
	close(p.resCh) // only after ALL workers are done; closing earlier => "send on closed channel" panic
}

func (p *run) work(ctx context.Context) {
	for {
		var row csvstream.Row
		var ok bool
		select {
		case <-ctx.Done():
			return
		case row, ok = <-p.rowsCh:
			if !ok {
				return // producer finished and channel drained
			}
		}

		p.col.ActiveWorker(1)
		t0 := time.Now()
		res := p.process(row)
		p.col.Time(report.WorkerBusy, time.Since(t0))
		p.col.ActiveWorker(-1)

		select {
		case p.resCh <- res:
		case <-ctx.Done():
			return
		}
	}
}

func (p *run) process(row csvstream.Row) result {
	if row.Err != nil {
		return result{row: row.Num, err: row.Err, malformed: true}
	}
	l := p.mapping.Build(p.cfg.AccountID, row)
	l.Normalize()
	if err := l.Validate(); err != nil {
		return result{row: row.Num, err: err}
	}
	return result{row: row.Num, lead: l}
}

func (p *run) route(ctx context.Context) error {
	defer func() {
		for _, ch := range p.shardCh {
			close(ch)
		}
	}()
	seen := make(map[string]int, 4096) // key -> first row. ~100 B/key => 150k rows ≈ 15 MB

	for {
		var res result
		var ok bool
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res, ok = <-p.resCh:
			if !ok {
				return nil
			}
		}

		switch {
		case res.malformed:
			p.reject(res.row, report.Malformed, "parse", "", res.err.Error())
		case res.err != nil:
			var ve *lead.ValidationError
			if errors.As(res.err, &ve) {
				p.col.InvalidCode(string(ve.Code))
			}
			p.reject(res.row, report.Invalid, "validate", "", res.err.Error())
		default:
			k := res.lead.Key()
			if first, dup := seen[k]; dup {
				p.reject(res.row, report.Duplicate, "dedupe", k, "duplicate of row "+strconv.Itoa(first))
				continue
			}
			seen[k] = res.row

			t0 := time.Now()
			select {
			case p.shardCh[shardOf(k, len(p.shardCh))] <- res.lead:
			case <-ctx.Done():
				return ctx.Err()
			}
			p.col.Time(report.RouterBlocked, time.Since(t0))
		}
	}
}

func (p *run) reject(row int, outcome, stage, key, reason string) {
	p.col.Row(outcome, 1)
	p.rejects.add(row, stage, key, reason)
	p.tracker.Done(row)
}

// shardOf is FNV-1a over the key, inlined so it doesn't allocate.
func shardOf(key string, n int) int {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h % uint32(n))
}

func (p *run) write(ctx context.Context, in <-chan lead.Lead) error {
	batch := make([]lead.Lead, 0, p.cfg.BatchSize)
	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := p.writeBatch(ctx, batch)
		batch = make([]lead.Lead, 0, p.cfg.BatchSize) // new slice: the old one may still be referenced by a slow sink
		return err
	}

	for {
		select {
		case l, ok := <-in:
			if !ok {
				return flush() // router closed the shard: flush the tail and exit
			}
			batch = append(batch, l)
			if len(batch) >= p.cfg.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *run) writeBatch(ctx context.Context, leads []lead.Lead) error {
	b := sink.Batch{ImportID: p.cfg.ImportID, AccountID: p.cfg.AccountID, Mode: p.cfg.Mode, Leads: leads}
	var res sink.Result

	onRetry := func(attempt int, wait time.Duration, err error) {
		p.col.Retry(retryReason(err), wait)
		p.cfg.Logger.Debug("retrying batch", "attempt", attempt, "wait", wait, "err", err)
	}
	_, err := retry.Do(ctx, p.cfg.Retry, onRetry, func(ctx context.Context) error {
		t0 := time.Now()
		r, err := p.sink.Write(ctx, b)
		p.col.SinkAttempt(time.Since(t0), err)
		res = r
		return err
	})

	rows := make([]int, len(leads))
	for i, l := range leads {
		rows[i] = l.Row
	}

	switch {
	case err == nil:
		p.consecutiveFailures.Store(0)
		p.col.Batch(len(leads))
		p.col.Row(report.Inserted, res.Inserted)
		p.col.Row(report.Updated, res.Updated)
		p.col.Row(report.Skipped, res.Skipped)
		p.tracker.Done(rows...)
		return nil

	case ctx.Err() != nil:
		return ctx.Err() // cancelled: rows are NOT marked done, so a resume re-sends them

	case retry.IsFatal(err):
		return err

	default:
		p.col.Row(report.Failed, len(leads))
		for _, l := range leads {
			p.rejects.add(l.Row, "sink", l.Key(), err.Error())
		}
		// Permanent rejection => mark done; transient (retries exhausted) => not done, so a resume retries it.
		if !retry.IsRetryable(err) {
			p.tracker.Done(rows...)
		}
		n := p.consecutiveFailures.Add(1)
		p.cfg.Logger.Warn("batch failed", "size", len(leads), "consecutive", n, "err", err)
		if n >= int64(p.cfg.MaxConsecutiveFailures) {
			return retry.Fatal(fmt.Errorf("%w: last error: %v", ErrDownstreamUnavailable, err))
		}
		return nil
	}
}

// retryReason labels retries for metrics: "http_429", "http_503", or "transient".
func retryReason(err error) string {
	var hs interface{ HTTPStatus() int }
	if errors.As(err, &hs) {
		return "http_" + strconv.Itoa(hs.HTTPStatus())
	}
	return "transient"
}

func (p *run) queueDepths() map[string]int {
	shards := 0
	for _, ch := range p.shardCh {
		shards += len(ch)
	}
	return map[string]int{"rows": len(p.rowsCh), "results": len(p.resCh), "shards": shards}
}

func (p *run) startCheckpointer() (stop func()) {
	if p.cfg.OnCheckpoint == nil {
		return func() {}
	}
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(p.cfg.CheckpointEvery)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				p.cfg.OnCheckpoint(p.tracker.Watermark())
			}
		}
	}()
	return func() { close(done); <-finished }
}

func isClosed(ch <-chan struct{}) bool {
	if ch == nil {
		return false
	}
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// rejects writes rejected rows as CSV. Shared by the router and all writers => mutex.
type rejects struct {
	mu sync.Mutex
	w  *csv.Writer
}

func newRejects(w io.Writer) *rejects {
	r := &rejects{}
	if w != nil {
		r.w = csv.NewWriter(w)
		_ = r.w.Write([]string{"row", "stage", "key", "reason"})
	}
	return r
}

func (r *rejects) add(row int, stage, key, reason string) {
	if r.w == nil {
		return
	}
	r.mu.Lock()
	_ = r.w.Write([]string{strconv.Itoa(row), stage, key, reason})
	r.mu.Unlock()
}

func (r *rejects) flush() {
	if r.w == nil {
		return
	}
	r.mu.Lock()
	r.w.Flush()
	r.mu.Unlock()
}
