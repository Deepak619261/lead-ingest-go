package csvstream

import (
	"errors"
	"io"
	"strings"
	"testing"
)

// readAll drains a Reader. Test helper only: production code never does this.
func readAll(t *testing.T, r *Reader) []Row {
	t.Helper()
	var rows []Row
	for {
		row, err := r.Next()
		if errors.Is(err, io.EOF) {
			return rows
		}
		if err != nil {
			t.Fatalf("unexpected stream error: %v", err)
		}
		rows = append(rows, row)
	}
}

func TestReader(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantComma  rune
		wantHeader []string
		wantRows   int
		wantBad    []int // row numbers expected to have Err != nil
	}{
		{
			name:       "comma separated",
			input:      "Email,First Name\na@x.com,Ann\nb@x.com,Bob\n",
			wantComma:  ',',
			wantHeader: []string{"Email", "First Name"},
			wantRows:   2,
		},
		{
			name:       "semicolon separated (European Excel)",
			input:      "Email;First Name;Company, Inc\na@x.com;Ann;Acme, Inc\n",
			wantComma:  ';',
			wantHeader: []string{"Email", "First Name", "Company, Inc"},
			wantRows:   1,
		},
		{
			name:       "UTF-8 BOM is stripped from first header",
			input:      string([]byte{0xEF, 0xBB, 0xBF}) + "Email,Name" + nl + "a@x.com,Ann" + nl,
			wantComma:  ',',
			wantHeader: []string{"Email", "Name"},
			wantRows:   1,
		},
		{
			name:       "wrong column count is a row error, not a fatal error",
			input:      "Email,Name\na@x.com,Ann\nbroken-row\nc@x.com,Cid\n",
			wantComma:  ',',
			wantHeader: []string{"Email", "Name"},
			wantRows:   3,
			wantBad:    []int{2},
		},
		{
			name:       "quoted field containing newline and comma",
			input:      "Email,Note\na@x.com,\"line1\nline2, still note\"\nb@x.com,ok\n",
			wantComma:  ',',
			wantHeader: []string{"Email", "Note"},
			wantRows:   2,
		},
		{
			name:       "CRLF line endings and blank lines",
			input:      "Email,Name\r\n\r\na@x.com,Ann\r\n",
			wantComma:  ',',
			wantHeader: []string{"Email", "Name"},
			wantRows:   1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewReader(strings.NewReader(tc.input))
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			if r.Comma() != tc.wantComma {
				t.Errorf("comma = %q, want %q", r.Comma(), tc.wantComma)
			}
			if strings.Join(r.Header(), "|") != strings.Join(tc.wantHeader, "|") {
				t.Errorf("header = %q, want %q", r.Header(), tc.wantHeader)
			}

			rows := readAll(t, r)
			if len(rows) != tc.wantRows {
				t.Fatalf("rows = %d, want %d", len(rows), tc.wantRows)
			}

			bad := map[int]bool{}
			for _, n := range tc.wantBad {
				bad[n] = true
			}
			for _, row := range rows {
				if gotBad := row.Err != nil; gotBad != bad[row.Num] {
					t.Errorf("row %d: err = %v, want error = %v", row.Num, row.Err, bad[row.Num])
				}
			}
		})
	}
}

func TestEmptyFileIsFatal(t *testing.T) {
	_, err := NewReader(strings.NewReader(""))
	if !errors.Is(err, ErrEmptyFile) {
		t.Fatalf("err = %v, want ErrEmptyFile", err)
	}
}

// Proves rows survive later reads even though ReuseRecord recycles the reader's slice.
func TestRowsAreNotOverwrittenByLaterReads(t *testing.T) {
	r, err := NewReader(strings.NewReader("Email\na@x.com\nb@x.com\n"))
	if err != nil {
		t.Fatal(err)
	}
	first, _ := r.Next()
	_, _ = r.Next()
	if first.Fields[0] != "a@x.com" {
		t.Fatalf("first row was overwritten: %q", first.Fields[0])
	}
}

func TestNormalizeHeader(t *testing.T) {
	cases := map[string]string{
		"First Name": "firstname",
		" EMAIL ":    "email",
		"Linked In":  "linkedin",
		"company":    "company",
	}
	for in, want := range cases {
		if got := NormalizeHeader(in); got != want {
			t.Errorf("NormalizeHeader(%q) = %q, want %q", in, got, want)
		}
	}
}

// go test -bench=. -benchmem ./internal/csvstream
func BenchmarkReader(b *testing.B) {
	var sb strings.Builder
	sb.WriteString("First Name,Last Name,Email,Phone,Company,Title\n")
	for i := 0; i < 10_000; i++ {
		sb.WriteString("Ann,Lee,ann.lee@example.com,+14155550100,Acme Inc,VP Sales\n")
	}
	data := sb.String()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for b.Loop() {
		r, _ := NewReader(strings.NewReader(data))
		for {
			if _, err := r.Next(); err != nil {
				break
			}
		}
	}
}

const nl = "\n"
