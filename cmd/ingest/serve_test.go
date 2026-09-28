package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"leadingest/internal/mockengine"
	"leadingest/internal/report"
	"leadingest/internal/sink/httpsink"
)

func testCommon() common {
	return common{sinkKind: "mock", workers: 2, shards: 2, batch: 100, queue: 100, flush: time.Second,
		retries: 3, maxFails: 5, onExists: "update", timeout: 5 * time.Second}
}

func bigCSV(n int) string {
	var b strings.Builder
	b.WriteString("Email\n")
	for i := range n {
		fmt.Fprintf(&b, "lead%d@acme.com\n", i)
	}
	return b.String()
}

// startServer runs runServer on a random port and returns its base URL, a cancel func (=SIGTERM), and its exit error channel.
func startServer(t *testing.T, eng *mockengine.Engine, o serveOpts) (string, context.CancelFunc, chan error) {
	t.Helper()
	engSrv := httptest.NewServer(eng)
	t.Cleanup(engSrv.Close)
	s := httpsink.New(httpsink.Config{BaseURL: engSrv.URL})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runServer(ctx, testCommon(), s, ln, o) }()
	return "http://" + ln.Addr().String(), cancel, done
}

func postImport(url, body string) (report.Report, int, error) {
	resp, err := http.Post(url, "text/csv", strings.NewReader(body))
	if err != nil {
		return report.Report{}, 0, err
	}
	defer resp.Body.Close()
	var rep report.Report
	err = json.NewDecoder(resp.Body).Decode(&rep)
	return rep, resp.StatusCode, err
}

func TestGracefulShutdownCheckpointsAndResumes(t *testing.T) {
	eng := mockengine.New(mockengine.Config{BaseLatency: 20 * time.Millisecond, Capacity: 1})
	url, sigterm, done := startServer(t, eng, serveOpts{maxImports: 2, drain: 200 * time.Millisecond, grace: 10 * time.Second})
	data := bigCSV(20_000)

	type res struct {
		rep report.Report
		err error
	}
	got := make(chan res, 1)
	go func() {
		rep, _, err := postImport(url+"/imports?account=1&import_id=shut", data)
		got <- res{rep, err}
	}()
	time.Sleep(300 * time.Millisecond)
	sigterm() // SIGTERM: drain 200ms, then soft stop

	r := <-got
	if r.err != nil {
		t.Fatal(r.err)
	}
	if !r.rep.Cancelled || r.rep.Checkpoint == 0 || r.rep.Checkpoint >= 20_000 {
		t.Fatalf("cancelled=%v checkpoint=%d", r.rep.Cancelled, r.rep.Checkpoint)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server exit: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server did not shut down")
	}

	// A new instance resumes after the checkpoint: no row lost, none duplicated.
	url2, stop2, _ := startServer(t, eng, serveOpts{maxImports: 2, drain: time.Second, grace: time.Second})
	defer stop2()
	rep, code, err := postImport(fmt.Sprintf("%s/imports?account=1&import_id=shut&resume=%d", url2, r.rep.Checkpoint), data)
	if err != nil || code != 200 || rep.Cancelled {
		t.Fatalf("resume: code=%d err=%v rep=%+v", code, err, rep)
	}
	if n := eng.Snapshot().Records; n != 20_000 {
		t.Fatalf("records = %d, want 20000", n)
	}
}

func TestCancelEndpointSoftStops(t *testing.T) {
	eng := mockengine.New(mockengine.Config{BaseLatency: 20 * time.Millisecond, Capacity: 1})
	url, stop, _ := startServer(t, eng, serveOpts{maxImports: 1, drain: time.Second, grace: time.Second})
	defer stop()

	got := make(chan report.Report, 1)
	go func() {
		rep, _, _ := postImport(url+"/imports?account=1&import_id=c1", bigCSV(20_000))
		got <- rep
	}()
	time.Sleep(200 * time.Millisecond)

	// A second import is refused while the only slot is busy.
	if _, code, _ := postImport(url+"/imports?account=1", "Email\na@x.com\n"); code != http.StatusTooManyRequests {
		t.Fatalf("second import: code %d, want 429", code)
	}
	req, _ := http.NewRequest(http.MethodDelete, url+"/imports/c1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusAccepted {
		t.Fatalf("cancel: %v %v", err, resp)
	}
	resp.Body.Close()
	select {
	case rep := <-got:
		if !rep.Cancelled || rep.Checkpoint == 0 {
			t.Fatalf("cancelled=%v checkpoint=%d", rep.Cancelled, rep.Checkpoint)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("import did not stop")
	}
}
