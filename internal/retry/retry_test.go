package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

var fast = Policy{MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}

func TestSucceedsAfterTransientFailures(t *testing.T) {
	calls := 0
	n, err := Do(context.Background(), fast, nil, func(context.Context) error {
		calls++
		if calls < 3 {
			return Retryable(errors.New("http 500"), 0)
		}
		return nil
	})
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v, want 3 attempts and success", n, err)
	}
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	n, err := Do(context.Background(), fast, nil, func(context.Context) error {
		return Retryable(errors.New("http 503"), 0)
	})
	if n != fast.MaxAttempts || !IsRetryable(err) {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestDoesNotRetryFatalOrPermanent(t *testing.T) {
	for _, e := range []error{Fatal(errors.New("401")), errors.New("400 bad request")} {
		calls := 0
		_, err := Do(context.Background(), fast, nil, func(context.Context) error { calls++; return e })
		if calls != 1 || !errors.Is(err, e) {
			t.Fatalf("calls=%d err=%v for %v", calls, err, e)
		}
	}
}

func TestClassificationSurvivesWrapping(t *testing.T) {
	err := fmt.Errorf("batch 7: %w", Fatal(errors.New("401")))
	if !IsFatal(err) || IsRetryable(err) {
		t.Fatal("wrapping broke classification")
	}
}

func TestHonoursRetryAfter(t *testing.T) {
	var waits []time.Duration
	calls := 0
	_, _ = Do(context.Background(), fast, func(_ int, w time.Duration, _ error) { waits = append(waits, w) },
		func(context.Context) error {
			calls++
			if calls == 1 {
				return Retryable(errors.New("429"), 30*time.Millisecond)
			}
			return nil
		})
	if len(waits) != 1 || waits[0] < 30*time.Millisecond {
		t.Fatalf("waits = %v, want >= 30ms (Retry-After)", waits)
	}
}

func TestCancellationStopsWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	slow := Policy{MaxAttempts: 10, BaseDelay: time.Hour, MaxDelay: time.Hour}
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start := time.Now()
	_, err := Do(ctx, slow, nil, func(context.Context) error {
		return Retryable(errors.New("503"), time.Hour)
	})
	if !errors.Is(err, context.Canceled) || time.Since(start) > time.Second {
		t.Fatalf("err=%v elapsed=%v", err, time.Since(start))
	}
}

func TestBackoffIsBounded(t *testing.T) {
	p := Policy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second}
	for n := 1; n < 70; n++ { // 70 would overflow a naive shift
		if d := p.Backoff(n); d < 0 || d > p.MaxDelay {
			t.Fatalf("Backoff(%d) = %v", n, d)
		}
	}
}
