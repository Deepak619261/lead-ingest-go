package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"leadingest/internal/mockengine"
	"leadingest/internal/report"
	"leadingest/internal/retry"
	"leadingest/internal/sink/httpsink"
)

// csvSpec builds a CSV with known counts of each row kind.
type csvSpec struct{ valid, invalid, dup, malformed int }

func (s csvSpec) build() string {
	var b strings.Builder
	b.WriteString("First Name,Email,Phone\n")
	for i := range s.valid {
		fmt.Fprintf(&b, "Ann,ann%d@acme.com,+1415%07d\n", i, i)
	}
	for range s.invalid {
		b.WriteString("Bob,not-an-email,\n")
	}
	for i := range s.dup {
		fmt.Fprintf(&b, "Ann,ANN%d@acme.com,\n", i) // same key as a valid row, different case
	}
	for range s.malformed {
		b.WriteString("only-one-column\n")
	}
	return b.String()
}

var fastRetry = retry.Policy{MaxAttempts: 8, BaseDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond}

func setup(t *testing.T, cfg mockengine.Config) (*mockengine.Engine, *httpsink.Sink) {
	t.Helper()
	eng := mockengine.New(cfg)
	srv := httptest.NewServer(eng)
	t.Cleanup(srv.Close)
	return eng, httpsink.New(httpsink.Config{BaseURL: srv.URL, Timeout: 2 * time.Second})
}

func baseCfg() Config {
	return Config{ImportID: "t", AccountID: 1, Workers: 4, Shards: 4, BatchSize: 100, QueueSize: 64, Retry: fastRetry}
}

func TestHappyPathCounts(t *testing.T) {
	eng, s := setup(t, mockengine.Config{})
	spec := csvSpec{valid: 5000, invalid: 70, dup: 30, malformed: 5}
	var rejects bytes.Buffer
	cfg := baseCfg()
	cfg.Rejects = &rejects

	rep, err := Run(context.Background(), cfg, strings.NewReader(spec.build()), s)
	if err != nil {
		t.Fatal(err)
	}
	o := rep.Outcomes
	if o[report.Inserted] != 5000 || o[report.Invalid] != 70 || o[report.Duplicate] != 30 || o[report.Malformed] != 5 {
		t.Fatalf("outcomes = %v", o)
	}
	if got := eng.Snapshot().Records; got != 5000 {
		t.Fatalf("engine records = %d, want 5000", got)
	}
	if rep.Checkpoint != 5105 {
		t.Fatalf("checkpoint = %d, want every row (5105)", rep.Checkpoint)
	}
	if lines := strings.Count(rejects.String(), "\n"); lines != 1+70+30+5 {
		t.Fatalf("rejects lines = %d", lines)
	}
}

func TestTransientFaultsAreRetried(t *testing.T) {
	eng, s := setup(t, mockengine.Config{P429: 0.15, P500: 0.15, RetryAfter: time.Millisecond})
	cfg := baseCfg()
	cfg.MaxConsecutiveFailures = 100

	rep, err := Run(context.Background(), cfg, strings.NewReader(csvSpec{valid: 3000}.build()), s)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcomes[report.Inserted] != 3000 || eng.Snapshot().Records != 3000 {
		t.Fatalf("outcomes=%v records=%d", rep.Outcomes, eng.Snapshot().Records)
	}
	if rep.Retries == 0 {
		t.Fatal("expected retries")
	}
}

func TestAuthFailureIsFatal(t *testing.T) {
	_, s := setup(t, mockengine.Config{Token: "secret"})
	_, err := Run(context.Background(), baseCfg(), strings.NewReader(csvSpec{valid: 1000}.build()), s)
	if !retry.IsFatal(err) {
		t.Fatalf("want fatal, got %v", err)
	}
}

func TestDownstreamDownStopsImport(t *testing.T) {
	_, s := setup(t, mockengine.Config{P500: 1})
	cfg := baseCfg()
	cfg.Retry = retry.Policy{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}
	cfg.MaxConsecutiveFailures = 3
	rep, err := Run(context.Background(), cfg, strings.NewReader(csvSpec{valid: 5000}.build()), s)
	if !errors.Is(err, ErrDownstreamUnavailable) {
		t.Fatalf("want ErrDownstreamUnavailable, got %v", err)
	}
	if rep.Checkpoint != 0 {
		t.Fatalf("transient failures must not advance the checkpoint, got %d", rep.Checkpoint)
	}
}

func TestBadHeaderAndConfigAreFatal(t *testing.T) {
	_, s := setup(t, mockengine.Config{})
	if _, err := Run(context.Background(), baseCfg(), strings.NewReader("Name,Company\na,b\n"), s); !retry.IsFatal(err) {
		t.Fatalf("no identifier column: %v", err)
	}
	cfg := baseCfg()
	cfg.AccountID = 0
	if _, err := Run(context.Background(), cfg, strings.NewReader("Email\na@x.com\n"), s); !retry.IsFatal(err) {
		t.Fatalf("bad config: %v", err)
	}
}

func TestHardCancelStopsPromptlyWithoutLeaks(t *testing.T) {
	_, s := setup(t, mockengine.Config{BaseLatency: 50 * time.Millisecond})
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := Run(ctx, baseCfg(), strings.NewReader(csvSpec{valid: 50_000}.build()), s)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("took %v to stop", el)
	}
	s.Close()
	time.Sleep(100 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+5 {
		t.Fatalf("goroutine leak: before %d after %d", before, after)
	}
}

func TestSoftStopThenResumeHasNoDuplicates(t *testing.T) {
	eng, s := setup(t, mockengine.Config{BaseLatency: 5 * time.Millisecond})
	data := csvSpec{valid: 8000, dup: 50}.build()

	stop := make(chan struct{})
	cfg := baseCfg()
	cfg.Stop = stop
	time.AfterFunc(60*time.Millisecond, func() { close(stop) })

	rep1, err := Run(context.Background(), cfg, strings.NewReader(data), s)
	if err != nil {
		t.Fatalf("soft stop should not error: %v", err)
	}
	if !rep1.Cancelled || rep1.Checkpoint == 0 || rep1.Checkpoint >= 8050 {
		t.Fatalf("first run: cancelled=%v checkpoint=%d", rep1.Cancelled, rep1.Checkpoint)
	}

	cfg2 := baseCfg()
	cfg2.ResumeAfter = rep1.Checkpoint
	if _, err := Run(context.Background(), cfg2, strings.NewReader(data), s); err != nil {
		t.Fatal(err)
	}
	st := eng.Snapshot()
	if st.Records != 8000 {
		t.Fatalf("records = %d, want exactly 8000 (no duplicates, nothing lost)", st.Records)
	}
}

func TestBackpressureBoundsQueuedItems(t *testing.T) {
	_, s := setup(t, mockengine.Config{BaseLatency: 20 * time.Millisecond})
	cfg := baseCfg()
	cfg.QueueSize = 50
	cfg.Shards = 2
	cfg.BatchSize = 50
	rep, err := Run(context.Background(), cfg, strings.NewReader(csvSpec{valid: 3000}.build()), s)
	if err != nil {
		t.Fatal(err)
	}
	limit := int64(cfg.QueueSize*2 + cfg.Shards*cfg.BatchSize)
	if rep.PeakQueue > limit {
		t.Fatalf("peak queued %d exceeds bound %d", rep.PeakQueue, limit)
	}
	if rep.StageSeconds["read_blocked"] == 0 {
		t.Fatal("reader never blocked: backpressure did not engage")
	}
}

func TestSameKeyAlwaysSameShard(t *testing.T) {
	for _, k := range []string{"1:e:a@x.com", "1:p:+1415", ""} {
		if shardOf(k, 16) != shardOf(k, 16) {
			t.Fatal("shardOf not deterministic")
		}
	}
}
