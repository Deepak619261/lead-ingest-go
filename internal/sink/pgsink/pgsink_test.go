package pgsink

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"leadingest/internal/pipeline"
	"leadingest/internal/report"
	"leadingest/internal/retry"
	"leadingest/internal/sink"
)

// Integration test: set LEADINGEST_PG_URL=postgres://user:pass@localhost:5432/leadingest_test
func connect(t *testing.T) *Sink {
	t.Helper()
	url := os.Getenv("LEADINGEST_PG_URL")
	if url == "" {
		t.Skip("LEADINGEST_PG_URL not set")
	}
	ctx := context.Background()
	s, err := New(ctx, url, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func csvRows(n, dups int) string {
	var b strings.Builder
	b.WriteString("First Name,Email,Phone,Company\n")
	for i := range n {
		fmt.Fprintf(&b, "Ann,ann%d@acme.com,+1415%07d,\n", i, i)
	}
	for i := range dups {
		fmt.Fprintf(&b, ",ANN%d@ACME.com,,Acme\n", i) // in-file duplicate carrying a new company
	}
	return b.String()
}

func TestPipelineIntoPostgresNoDuplicates(t *testing.T) {
	s := connect(t)
	ctx := context.Background()
	cfg := pipeline.Config{ImportID: "pg1", AccountID: 7, Workers: 4, Shards: 8, BatchSize: 250,
		Retry: retry.Policy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond}}

	rep, err := pipeline.Run(ctx, cfg, strings.NewReader(csvRows(5000, 100)), s)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcomes[report.Inserted] != 5000 || rep.Outcomes[report.Duplicate] != 100 {
		t.Fatalf("outcomes = %v", rep.Outcomes)
	}

	// Re-import the same file: everything becomes an update, nothing is duplicated.
	rep, err = pipeline.Run(ctx, cfg, strings.NewReader(csvRows(5000, 0)), s)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcomes[report.Updated] != 5000 || rep.Outcomes[report.Inserted] != 0 {
		t.Fatalf("re-import outcomes = %v", rep.Outcomes)
	}
	p, e, d, err := s.Stats(ctx)
	if err != nil || p != 5000 || e != 10000 || d != 0 {
		t.Fatalf("prospects=%d events=%d duplicateEmails=%d err=%v", p, e, d, err)
	}
}

func TestUpdateFillsOnlyMissingFields(t *testing.T) {
	s := connect(t)
	ctx := context.Background()
	b := sink.Batch{ImportID: "x", AccountID: 1, Mode: sink.ModeUpdateMissing}
	b.Leads = append(b.Leads, leadWith("a@x.com", "Ann", ""))
	if _, err := s.Write(ctx, b); err != nil {
		t.Fatal(err)
	}
	b.Leads[0] = leadWith("a@x.com", "Changed", "Acme")
	res, err := s.Write(ctx, b)
	if err != nil || res.Updated != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	var first, company string
	if err := s.pool.QueryRow(ctx, `SELECT first_name, company FROM prospects WHERE email='a@x.com'`).Scan(&first, &company); err != nil {
		t.Fatal(err)
	}
	if first != "Ann" || company != "Acme" {
		t.Fatalf("first=%q company=%q, want existing name kept and missing company filled", first, company)
	}
}
