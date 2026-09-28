package httpsink

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"leadingest/internal/lead"
	"leadingest/internal/mockengine"
	"leadingest/internal/retry"
	"leadingest/internal/sink"
)

func batch(emails ...string) sink.Batch {
	b := sink.Batch{AccountID: 1, Mode: sink.ModeUpdateMissing}
	for i, e := range emails {
		b.Leads = append(b.Leads, lead.Lead{Row: i + 1, AccountID: 1, Email: e})
	}
	return b
}

func TestWriteInsertsThenUpdates(t *testing.T) {
	srv := httptest.NewServer(mockengine.New(mockengine.Config{}))
	defer srv.Close()
	s := New(Config{BaseURL: srv.URL})

	res, err := s.Write(context.Background(), batch("a@x.com", "b@x.com"))
	if err != nil || res.Inserted != 2 {
		t.Fatalf("first write: %+v %v", res, err)
	}
	// Re-sending the same batch (e.g. a retry after a timeout) must not duplicate.
	res, err = s.Write(context.Background(), batch("a@x.com", "b@x.com"))
	if err != nil || res.Updated != 2 || res.Inserted != 0 {
		t.Fatalf("second write: %+v %v", res, err)
	}
}

func TestClassification(t *testing.T) {
	cases := []struct {
		name          string
		status        int
		wantRetryable bool
		wantFatal     bool
	}{
		{"429 retryable", http.StatusTooManyRequests, true, false},
		{"500 retryable", http.StatusInternalServerError, true, false},
		{"503 retryable", http.StatusServiceUnavailable, true, false},
		{"401 fatal", http.StatusUnauthorized, false, true},
		{"403 fatal", http.StatusForbidden, false, true},
		{"400 permanent", http.StatusBadRequest, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "2")
				http.Error(w, "nope", tc.status)
			}))
			defer srv.Close()
			_, err := New(Config{BaseURL: srv.URL}).Write(context.Background(), batch("a@x.com"))
			if retry.IsRetryable(err) != tc.wantRetryable || retry.IsFatal(err) != tc.wantFatal {
				t.Fatalf("err=%v retryable=%v fatal=%v", err, retry.IsRetryable(err), retry.IsFatal(err))
			}
			if !IsStatus(err, tc.status) {
				t.Fatalf("status not preserved in %v", err)
			}
		})
	}
}

func TestRetryAfterIsParsed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	_, err := New(Config{BaseURL: srv.URL}).Write(context.Background(), batch("a@x.com"))
	var re *retry.RetryableError
	if !errors.As(err, &re) || re.After != 3*time.Second {
		t.Fatalf("err=%v", err)
	}
}

func TestTimeoutIsRetryable(t *testing.T) {
	srv := httptest.NewServer(mockengine.New(mockengine.Config{PSlow: 1, SlowFor: time.Second}))
	defer srv.Close()
	_, err := New(Config{BaseURL: srv.URL, Timeout: 50 * time.Millisecond}).Write(context.Background(), batch("a@x.com"))
	if !retry.IsRetryable(err) {
		t.Fatalf("timeout should be retryable, got %v", err)
	}
}

func TestCancelledContextIsNotRetryable(t *testing.T) {
	srv := httptest.NewServer(mockengine.New(mockengine.Config{BaseLatency: time.Second}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := New(Config{BaseURL: srv.URL}).Write(ctx, batch("a@x.com"))
	if retry.IsRetryable(err) || err == nil {
		t.Fatalf("cancellation must not be retried, got %v", err)
	}
}

func TestAuth(t *testing.T) {
	srv := httptest.NewServer(mockengine.New(mockengine.Config{Token: "secret"}))
	defer srv.Close()
	if _, err := New(Config{BaseURL: srv.URL, Token: "wrong"}).Write(context.Background(), batch("a@x.com")); !retry.IsFatal(err) {
		t.Fatalf("wrong token should be fatal, got %v", err)
	}
	if _, err := New(Config{BaseURL: srv.URL, Token: "secret"}).Write(context.Background(), batch("a@x.com")); err != nil {
		t.Fatalf("right token failed: %v", err)
	}
}
