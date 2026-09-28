package lead

import (
	"errors"
	"strings"
	"testing"

	"leadingest/internal/csvstream"
)

func TestNewMapping(t *testing.T) {
	m, err := NewMapping([]string{"First Name", "Work Email", "Junk", "Job Title", "Email"})
	if err != nil {
		t.Fatal(err)
	}
	l := m.Build(7, csvstream.Row{Num: 3, Fields: []string{"Ann", "ann@x.com", "zzz", "CTO", "second@x.com"}})
	if l.FirstName != "Ann" || l.Email != "ann@x.com" || l.Title != "CTO" || l.AccountID != 7 || l.Row != 3 {
		t.Fatalf("unexpected lead: %+v", l)
	}
}

func TestNewMappingWithoutIdentifierIsFatal(t *testing.T) {
	_, err := NewMapping([]string{"First Name", "Company"})
	if !errors.Is(err, ErrNoIdentifierColumn) {
		t.Fatalf("err = %v, want ErrNoIdentifierColumn", err)
	}
}

func TestNormalize(t *testing.T) {
	in := Lead{
		FirstName: "  Ann ",
		LastName:  "\tLee\n",
		Email:     "  Ann.Lee@Acme.COM ",
		Phone:     " +1 (415) 555-0100 ",
		Company:   " Acme, Inc ",
		LinkedIn:  " https://www.linkedin.com/in/ann/ ",
		City:      " Pune ",
	}
	want := Lead{
		FirstName: "Ann",
		LastName:  "Lee",
		Email:     "ann.lee@acme.com",
		Phone:     "+14155550100",
		Company:   "Acme, Inc",
		LinkedIn:  "https://www.linkedin.com/in/ann",
		City:      "Pune",
	}
	in.Normalize()
	if in != want { // structs with only comparable fields can be compared with ==
		t.Fatalf("\n got %+v\nwant %+v", in, want)
	}
}

func TestNormalizePhoneWithoutPlus(t *testing.T) {
	l := Lead{Phone: "020-7946 0958 ext"}
	l.Normalize()
	if l.Phone != "02079460958" {
		t.Fatalf("phone = %q", l.Phone)
	}
}

func TestValidate(t *testing.T) {
	good := Lead{Row: 1, FirstName: "Ann", Email: "ann@acme.com"}

	tests := []struct {
		name      string
		edit      func(l *Lead) // change the good lead to make it bad
		wantField string        // "" means: expect no error
		wantCode  Code
	}{
		{"valid lead", func(l *Lead) {}, "", ""},
		{"phone only is fine", func(l *Lead) { l.Email = ""; l.Phone = "+14155550100" }, "", ""},
		{"linkedin only is fine", func(l *Lead) { l.Email = ""; l.LinkedIn = "https://linkedin.com/in/a" }, "", ""},
		{"no identifier", func(l *Lead) { l.Email = "" }, "email", CodeNoIdentifier},
		{"missing @", func(l *Lead) { l.Email = "ann.at.acme.com" }, "email", CodeInvalidEmail},
		{"email inside junk (an unanchored regex would accept this)", func(l *Lead) { l.Email = "hi ann@acme.com bye" }, "email", CodeInvalidEmail},
		{"no TLD", func(l *Lead) { l.Email = "ann@acme" }, "email", CodeInvalidEmail},
		{"noreply", func(l *Lead) { l.Email = "noreply@acme.com" }, "email", CodeArbitraryEmail},
		{"no-reply", func(l *Lead) { l.Email = "no-reply@acme.com" }, "email", CodeArbitraryEmail},
		{"phone with no digits", func(l *Lead) { l.Phone = "+" }, "phone", CodeInvalidPhone},
		{"first name too long", func(l *Lead) { l.FirstName = strings.Repeat("x", 251) }, "firstname", CodeTooLong},
		{"last name at limit is ok", func(l *Lead) { l.LastName = strings.Repeat("x", 150) }, "", ""},
		{"last name too long", func(l *Lead) { l.LastName = strings.Repeat("x", 151) }, "lastname", CodeTooLong},
		// 150 x "ü" = 150 runes but 300 bytes. Counting bytes would wrongly reject it.
		{"limit counts runes not bytes", func(l *Lead) { l.LastName = strings.Repeat("ü", 150) }, "", ""},
		{"company too long", func(l *Lead) { l.Company = strings.Repeat("c", 301) }, "company", CodeTooLong},
		{"phone too long", func(l *Lead) { l.Phone = "+" + strings.Repeat("1", 20) }, "phone", CodeTooLong},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := good // copy (value semantics!), so each case starts clean
			tc.edit(&l)
			err := l.Validate()

			if tc.wantField == "" {
				if err != nil {
					t.Fatalf("want no error, got %v", err)
				}
				return
			}
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("want *ValidationError, got %T (%v)", err, err)
			}
			if ve.Field != tc.wantField || ve.Code != tc.wantCode || ve.Row != 1 {
				t.Fatalf("got {field:%s code:%s row:%d}, want {field:%s code:%s row:1}",
					ve.Field, ve.Code, ve.Row, tc.wantField, tc.wantCode)
			}
		})
	}
}

func TestKey(t *testing.T) {
	tests := []struct {
		l    Lead
		want string
	}{
		{Lead{AccountID: 42, Email: "a@x.com", Phone: "+1", LinkedIn: "li"}, "42:e:a@x.com"},
		{Lead{AccountID: 42, Phone: "+1", LinkedIn: "https://linkedin.com/in/a"}, "42:l:https://linkedin.com/in/a"},
		{Lead{AccountID: 42, Phone: "+14155550100"}, "42:p:+14155550100"},
		{Lead{AccountID: 42}, ""},
	}
	for _, tc := range tests {
		if got := tc.l.Key(); got != tc.want {
			t.Errorf("Key() = %q, want %q", got, tc.want)
		}
	}
}

// The same person in different accounts must NOT collide.
func TestKeyIsScopedByAccount(t *testing.T) {
	a := Lead{AccountID: 1, Email: "a@x.com"}
	b := Lead{AccountID: 2, Email: "a@x.com"}
	if a.Key() == b.Key() {
		t.Fatal("keys collide across accounts")
	}
}

// End-to-end through the real CSV reader: case-different duplicates get the same key.
func TestDuplicateEmailsDifferentCaseShareKey(t *testing.T) {
	r, _ := csvstream.NewReader(strings.NewReader("Email,Name\nAnn@X.com,Ann\n ann@x.COM ,Ann2\n"))
	m, _ := NewMapping(r.Header())
	var keys []string
	for {
		row, err := r.Next()
		if err != nil {
			break
		}
		l := m.Build(9, row)
		l.Normalize()
		keys = append(keys, l.Key())
	}
	if len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] {
		t.Fatalf("keys = %q, want two equal non-empty keys", keys)
	}
}

// go test -bench=Key -benchmem ./internal/lead
func BenchmarkKey(b *testing.B) {
	l := Lead{AccountID: 123456, Email: "ann.lee@acme.com"}
	b.ReportAllocs()
	for b.Loop() {
		_ = l.Key()
	}
}

func BenchmarkValidate(b *testing.B) {
	l := Lead{Row: 1, FirstName: "Ann", LastName: "Lee", Email: "ann.lee@acme.com", Phone: "+14155550100", Company: "Acme"}
	b.ReportAllocs()
	for b.Loop() {
		_ = l.Validate()
	}
}
