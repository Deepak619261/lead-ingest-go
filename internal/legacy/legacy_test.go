package legacy

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"leadingest/internal/mockengine"
	"leadingest/internal/report"
	"leadingest/internal/sink/httpsink"
)

func TestLegacyDoesLookupsPerRow(t *testing.T) {
	eng := mockengine.New(mockengine.Config{})
	srv := httptest.NewServer(eng)
	defer srv.Close()
	s := httpsink.New(httpsink.Config{BaseURL: srv.URL})

	var b strings.Builder
	b.WriteString("Email\n")
	for i := range 1200 {
		fmt.Fprintf(&b, "a%d@x.com\n", i)
	}
	b.WriteString("bad-email\n")

	rep, err := Run(context.Background(), Config{AccountID: 1, LookupsPerRow: 3}, strings.NewReader(b.String()), s, s)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Outcomes[report.Inserted] != 1200 || rep.Outcomes[report.Invalid] != 1 {
		t.Fatalf("outcomes = %v", rep.Outcomes)
	}
	if st := eng.Snapshot(); st.Lookups != 3600 || st.BulkRequests != 3 {
		t.Fatalf("lookups=%d bulk=%d, want 3600 and 3", st.Lookups, st.BulkRequests)
	}
}
