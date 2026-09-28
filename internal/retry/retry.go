// Package retry classifies errors and retries only the ones worth retrying.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// RetryableError marks a transient failure; After is a server-requested minimum wait (Retry-After).
type RetryableError struct {
	Err   error
	After time.Duration
}

func (e *RetryableError) Error() string { return "retryable: " + e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

// FatalError means continuing is pointless (e.g. 401 Unauthorized).
type FatalError struct{ Err error }

func (e *FatalError) Error() string { return "fatal: " + e.Err.Error() }
func (e *FatalError) Unwrap() error { return e.Err }

// Retryable / Fatal are small constructors so call sites read well.
func Retryable(err error, after time.Duration) error { return &RetryableError{Err: err, After: after} }
func Fatal(err error) error                          { return &FatalError{Err: err} }

// IsRetryable / IsFatal look through wrapping (errors.As), so fmt.Errorf("%w") is fine.
func IsRetryable(err error) bool { var r *RetryableError; return errors.As(err, &r) }
func IsFatal(err error) bool     { var f *FatalError; return errors.As(err, &f) }

// Policy is bounded exponential backoff with full jitter.
type Policy struct {
	MaxAttempts int           // total tries including the first (e.g. 5)
	BaseDelay   time.Duration // first backoff (e.g. 100ms)
	MaxDelay    time.Duration // cap (e.g. 5s)
}

// DefaultPolicy is used when the caller doesn't configure one.
var DefaultPolicy = Policy{MaxAttempts: 5, BaseDelay: 100 * time.Millisecond, MaxDelay: 5 * time.Second}

// Backoff is full-jitter exponential: random in [0, min(Max, Base*2^(n-1))].
func (p Policy) Backoff(n int) time.Duration {
	// Base<<shift <= Max  <=>  Base <= Max>>shift, so the shift never overflows.
	ceiling := p.MaxDelay
	if shift := n - 1; shift >= 0 && shift < 63 && p.BaseDelay <= p.MaxDelay>>shift {
		ceiling = p.BaseDelay << shift
	}
	return time.Duration(rand.Int64N(int64(ceiling) + 1))
}

// OnRetry is called before each wait. Used for metrics/logging. May be nil.
type OnRetry func(attempt int, wait time.Duration, err error)

// Do retries fn on *RetryableError until success, a non-retryable error, MaxAttempts, or cancellation.
func Do(ctx context.Context, p Policy, onRetry OnRetry, fn func(ctx context.Context) error) (int, error) {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	var err error
	for attempt := 1; ; attempt++ {
		err = fn(ctx)
		if err == nil {
			return attempt, nil
		}

		var re *RetryableError
		if !errors.As(err, &re) {
			return attempt, err // fatal or permanent: retrying won't help
		}
		if attempt >= p.MaxAttempts {
			return attempt, fmt.Errorf("gave up after %d attempts: %w", attempt, err)
		}

		wait := p.Backoff(attempt)
		if re.After > wait {
			wait = re.After // respect the server's Retry-After
		}
		if onRetry != nil {
			onRetry(attempt, wait, err)
		}

		// Context-aware sleep: time.Sleep would ignore cancellation for up to MaxDelay.
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return attempt, errors.Join(ctx.Err(), err)
		case <-t.C:
		}
	}
}
