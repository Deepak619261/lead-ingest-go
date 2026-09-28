// Package report collects counters and stage timings: how long it took, and where the time went.
package report

import (
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"runtime/metrics"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	promm "leadingest/internal/metrics"
)

// Outcomes for Collector.Row.
const (
	Inserted  = "inserted"
	Updated   = "updated"
	Skipped   = "skipped_existing"
	Invalid   = "invalid"
	Malformed = "malformed"
	Duplicate = "duplicate"
	Failed    = "failed"
)

// Collector is updated concurrently by every stage (atomics + mutexes); Prometheus is updated alongside.
type Collector struct {
	m     *promm.Metrics
	start time.Time

	rows     sync.Map // outcome -> *atomic.Int64
	invalid  sync.Map // code -> *atomic.Int64
	read     atomic.Int64
	batches  atomic.Int64
	retries  atomic.Int64
	lookups  atomic.Int64
	timings  [numStages]atomic.Int64 // nanoseconds
	sinkLat  latencies
	lookLat  latencies
	peakHeap atomic.Uint64
	peakMem  atomic.Uint64
	peakQ    atomic.Int64

	cpuStart, gcStart        float64
	numGCStart, pauseStartNs uint64
}

// Stage identifies where time was spent.
type Stage int

const (
	ReadBusy      Stage = iota // parsing CSV
	ReadBlocked                // producer waiting on a full channel = BACKPRESSURE
	WorkerBusy                 // map + normalize + validate + key (sum over workers)
	RouterBlocked              // router waiting for writers = writers are the bottleneck
	SinkBusy                   // time inside sink.Write attempts (sum over writers)
	RetryWait                  // backoff sleeps
	LookupBusy                 // legacy per-row lookups
	numStages
)

var stageNames = [numStages]string{"read_busy", "read_blocked", "worker_busy", "router_blocked", "sink_busy", "retry_wait", "lookup_busy"}

// New starts the clock and snapshots runtime counters.
func New(m *promm.Metrics) *Collector {
	c := &Collector{m: m, start: time.Now()}
	c.cpuStart, c.gcStart = cpuSeconds()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	c.numGCStart, c.pauseStartNs = uint64(ms.NumGC), ms.PauseTotalNs
	return c
}

func counter(m *sync.Map, k string) *atomic.Int64 {
	if v, ok := m.Load(k); ok {
		return v.(*atomic.Int64)
	}
	v, _ := m.LoadOrStore(k, new(atomic.Int64))
	return v.(*atomic.Int64)
}

// RowRead counts a row taken from the CSV.
func (c *Collector) RowRead() { c.read.Add(1) }

// Row records the final outcome of n rows.
func (c *Collector) Row(outcome string, n int) {
	if n == 0 {
		return
	}
	counter(&c.rows, outcome).Add(int64(n))
	if c.m != nil {
		c.m.Rows.WithLabelValues(outcome).Add(float64(n))
	}
}

// InvalidCode records a validation failure reason (in addition to Row(Invalid)).
func (c *Collector) InvalidCode(code string) {
	counter(&c.invalid, code).Add(1)
	if c.m != nil {
		c.m.ValidationFailures.WithLabelValues(code).Inc()
	}
}

// Time adds d to a stage.
func (c *Collector) Time(s Stage, d time.Duration) { c.timings[s].Add(int64(d)) }

// SinkAttempt records one sink.Write attempt.
func (c *Collector) SinkAttempt(d time.Duration, err error) {
	c.Time(SinkBusy, d)
	c.sinkLat.add(d)
	if c.m != nil {
		res := "ok"
		if err != nil {
			res = "error"
		}
		c.m.SinkLatency.WithLabelValues(res).Observe(d.Seconds())
	}
}

// Batch records a completed batch of size n.
func (c *Collector) Batch(n int) {
	c.batches.Add(1)
	if c.m != nil {
		c.m.BatchSize.Observe(float64(n))
	}
}

// Retry records a retry and its wait.
func (c *Collector) Retry(reason string, wait time.Duration) {
	c.retries.Add(1)
	c.Time(RetryWait, wait)
	if c.m != nil {
		c.m.Retries.WithLabelValues(reason).Inc()
	}
}

// Lookup records one legacy single-row lookup.
func (c *Collector) Lookup(d time.Duration) {
	c.lookups.Add(1)
	c.Time(LookupBusy, d)
	c.lookLat.add(d)
	if c.m != nil {
		c.m.LookupLatency.Observe(d.Seconds())
	}
}

// ActiveWorker adjusts the active-workers gauge.
func (c *Collector) ActiveWorker(delta int) {
	if c.m != nil {
		c.m.ActiveWorkers.Add(float64(delta))
	}
}

// Sample records queue depths and memory. Call it periodically (see StartSampler).
func (c *Collector) Sample(queues map[string]int) {
	total := 0
	for name, n := range queues {
		total += n
		if c.m != nil {
			c.m.QueueDepth.WithLabelValues(name).Set(float64(n))
		}
	}
	maxI64(&c.peakQ, int64(total))
	heap, mem := memory()
	maxU64(&c.peakHeap, heap)
	maxU64(&c.peakMem, mem)
}

// StartSampler calls Sample every interval until the returned stop func is called.
func (c *Collector) StartSampler(every time.Duration, queues func() map[string]int) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			c.Sample(queues())
			select {
			case <-done:
				return
			case <-t.C:
			}
		}
	}()
	return func() { close(done); wg.Wait() }
}

func maxU64(a *atomic.Uint64, v uint64) {
	for old := a.Load(); v > old && !a.CompareAndSwap(old, v); old = a.Load() {
	}
}
func maxI64(a *atomic.Int64, v int64) {
	for old := a.Load(); v > old && !a.CompareAndSwap(old, v); old = a.Load() {
	}
}

// latencies keeps a bounded reservoir sample for percentiles.
type latencies struct {
	mu   sync.Mutex
	n    int
	vals []time.Duration
}

const maxSamples = 50_000

func (l *latencies) add(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.n++
	if len(l.vals) < maxSamples {
		l.vals = append(l.vals, d)
		return
	}
	if i := rand.IntN(l.n); i < maxSamples { // reservoir sampling: each sample kept with p = max/n
		l.vals[i] = d
	}
}

func (l *latencies) percentiles() (p50, p95, p99, max time.Duration) {
	l.mu.Lock()
	v := slices.Clone(l.vals)
	l.mu.Unlock()
	if len(v) == 0 {
		return
	}
	slices.Sort(v)
	at := func(p float64) time.Duration { return v[min(len(v)-1, int(p*float64(len(v))))] }
	return at(0.50), at(0.95), at(0.99), v[len(v)-1]
}

var memSamples = []metrics.Sample{
	{Name: "/memory/classes/heap/objects:bytes"}, // live + not-yet-swept heap objects
	{Name: "/memory/classes/total:bytes"},        // everything the Go runtime has mapped ≈ RSS
}

func memory() (heap, total uint64) {
	s := make([]metrics.Sample, len(memSamples))
	copy(s, memSamples)
	metrics.Read(s) // unlike runtime.ReadMemStats, this does not stop the world
	return s[0].Value.Uint64(), s[1].Value.Uint64()
}

func cpuSeconds() (used, gc float64) {
	s := []metrics.Sample{
		{Name: "/cpu/classes/total:cpu-seconds"},
		{Name: "/cpu/classes/idle:cpu-seconds"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
	}
	metrics.Read(s)
	return s[0].Value.Float64() - s[1].Value.Float64(), s[2].Value.Float64()
}

// Report is the end-of-run summary (also written as JSON for the benchmark script).
type Report struct {
	ImportID  string `json:"importId"`
	Mode      string `json:"mode"` // pipeline | legacy
	Sink      string `json:"sink"`
	Workers   int    `json:"workers"`
	Shards    int    `json:"shards"`
	BatchSize int    `json:"batchSize"`

	RowsRead      int64            `json:"rowsRead"`
	Outcomes      map[string]int64 `json:"outcomes"`
	InvalidByCode map[string]int64 `json:"invalidByCode"`
	Batches       int64            `json:"batches"`
	Retries       int64            `json:"retries"`
	Lookups       int64            `json:"lookups"`
	Checkpoint    int              `json:"checkpoint"`
	Cancelled     bool             `json:"cancelled"`
	Error         string           `json:"error,omitempty"`

	DurationSec  float64            `json:"durationSec"`
	RowsPerSec   float64            `json:"rowsPerSec"`
	StageSeconds map[string]float64 `json:"stageSeconds"`
	SinkP50Ms    float64            `json:"sinkP50Ms"`
	SinkP95Ms    float64            `json:"sinkP95Ms"`
	SinkP99Ms    float64            `json:"sinkP99Ms"`
	SinkMaxMs    float64            `json:"sinkMaxMs"`
	LookupP50Ms  float64            `json:"lookupP50Ms"`
	LookupP99Ms  float64            `json:"lookupP99Ms"`
	PeakQueue    int64              `json:"peakQueue"`
	PeakHeapMB   float64            `json:"peakHeapMB"`
	PeakMemMB    float64            `json:"peakMemMB"`
	CPUSeconds   float64            `json:"cpuSeconds"`
	CPUPercent   float64            `json:"cpuPercentOfOneCore"`
	GCCPUSeconds float64            `json:"gcCpuSeconds"`
	NumGC        uint32             `json:"numGC"`
	GCPauseMs    float64            `json:"gcPauseTotalMs"`
}

// Finish builds the Report. Call once, after all stages have stopped.
func (c *Collector) Finish(r Report) Report {
	c.Sample(nil)
	dur := time.Since(c.start)
	if c.m != nil {
		c.m.ImportDuration.Observe(dur.Seconds())
	}

	r.RowsRead = c.read.Load()
	r.Outcomes = map[string]int64{}
	c.rows.Range(func(k, v any) bool { r.Outcomes[k.(string)] = v.(*atomic.Int64).Load(); return true })
	r.InvalidByCode = map[string]int64{}
	c.invalid.Range(func(k, v any) bool { r.InvalidByCode[k.(string)] = v.(*atomic.Int64).Load(); return true })
	r.Batches, r.Retries, r.Lookups = c.batches.Load(), c.retries.Load(), c.lookups.Load()

	r.DurationSec = dur.Seconds()
	if r.DurationSec > 0 {
		r.RowsPerSec = float64(r.RowsRead) / r.DurationSec
	}
	r.StageSeconds = map[string]float64{}
	for s := range numStages {
		if ns := c.timings[s].Load(); ns > 0 {
			r.StageSeconds[stageNames[s]] = time.Duration(ns).Seconds()
		}
	}
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	p50, p95, p99, mx := c.sinkLat.percentiles()
	r.SinkP50Ms, r.SinkP95Ms, r.SinkP99Ms, r.SinkMaxMs = ms(p50), ms(p95), ms(p99), ms(mx)
	lp50, _, lp99, _ := c.lookLat.percentiles()
	r.LookupP50Ms, r.LookupP99Ms = ms(lp50), ms(lp99)

	r.PeakQueue = c.peakQ.Load()
	r.PeakHeapMB = float64(c.peakHeap.Load()) / (1 << 20)
	r.PeakMemMB = float64(c.peakMem.Load()) / (1 << 20)
	cpu, gc := cpuSeconds()
	r.CPUSeconds, r.GCCPUSeconds = cpu-c.cpuStart, gc-c.gcStart
	if r.DurationSec > 0 {
		r.CPUPercent = 100 * r.CPUSeconds / r.DurationSec
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	r.NumGC = m.NumGC - uint32(c.numGCStart)
	r.GCPauseMs = float64(m.PauseTotalNs-c.pauseStartNs) / 1e6
	return r
}

// Print writes a human-readable summary.
func (r Report) Print(w io.Writer) {
	line := strings.Repeat("─", 64)
	fmt.Fprintln(w, line)
	fmt.Fprintf(w, "import %s  mode=%s sink=%s workers=%d shards=%d batch=%d\n",
		r.ImportID, r.Mode, r.Sink, r.Workers, r.Shards, r.BatchSize)
	fmt.Fprintln(w, line)
	fmt.Fprintf(w, "rows read        %d in %.2fs  =>  %.0f rows/s\n", r.RowsRead, r.DurationSec, r.RowsPerSec)
	for _, k := range []string{Inserted, Updated, Skipped, Duplicate, Invalid, Malformed, Failed} {
		if v := r.Outcomes[k]; v > 0 {
			fmt.Fprintf(w, "  %-16s %d\n", k, v)
		}
	}
	if len(r.InvalidByCode) > 0 {
		fmt.Fprintf(w, "  invalid by code  %v\n", r.InvalidByCode)
	}
	fmt.Fprintf(w, "batches %d  retries %d  lookups %d  checkpoint(row) %d  cancelled %v\n",
		r.Batches, r.Retries, r.Lookups, r.Checkpoint, r.Cancelled)
	if r.Error != "" {
		fmt.Fprintf(w, "ERROR: %s\n", r.Error)
	}
	fmt.Fprintln(w, "where the time went (seconds, summed across goroutines):")
	for _, s := range stageNames {
		if v, ok := r.StageSeconds[s]; ok {
			fmt.Fprintf(w, "  %-16s %8.2f\n", s, v)
		}
	}
	if r.Batches > 0 {
		fmt.Fprintf(w, "sink latency ms  p50 %.1f  p95 %.1f  p99 %.1f  max %.1f\n", r.SinkP50Ms, r.SinkP95Ms, r.SinkP99Ms, r.SinkMaxMs)
	}
	if r.Lookups > 0 {
		fmt.Fprintf(w, "lookup latency ms p50 %.2f  p99 %.2f\n", r.LookupP50Ms, r.LookupP99Ms)
	}
	fmt.Fprintf(w, "memory  peak heap %.1f MB  peak runtime %.1f MB  peak queued %d\n", r.PeakHeapMB, r.PeakMemMB, r.PeakQueue)
	fmt.Fprintf(w, "cpu     %.2fs (%.0f%% of one core)  gc cpu %.2fs  gc cycles %d  gc pause %.1fms\n",
		r.CPUSeconds, r.CPUPercent, r.GCCPUSeconds, r.NumGC, r.GCPauseMs)
	fmt.Fprintln(w, line)
}
