// Package lead maps CSV rows to leads and normalises, validates and keys them (rules match the legacy importer).
package lead

import (
	"errors"
	"fmt"
	"regexp"

	"leadingest/internal/csvstream"
)

// Field length limits (runes, not bytes), matching the legacy importer's checks.
const (
	MaxFirstName = 250
	MaxLastName  = 150
	MaxEmail     = 250
	MaxTitle     = 250
	MaxPhone     = 20
	MaxDefault   = 300 // company, city, country, industry, linkedin
)

// emailRE is anchored (^...$); an unanchored pattern would accept "junk a@b.com junk".
var emailRE = regexp.MustCompile(`^[a-zA-Z0-9+._-]+@[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,}$`)

// Lead is one normalised prospect candidate.
type Lead struct {
	Row       int   `json:"row"`       // CSV data row number (for error reports)
	AccountID int64 `json:"accountId"` // tenant: every key is scoped to it

	FirstName string `json:"firstName,omitempty"`
	LastName  string `json:"lastName,omitempty"`
	Email     string `json:"email,omitempty"`
	Phone     string `json:"phone,omitempty"`
	Company   string `json:"company,omitempty"`
	Title     string `json:"title,omitempty"`
	LinkedIn  string `json:"linkedIn,omitempty"`
	City      string `json:"city,omitempty"`
	Country   string `json:"country,omitempty"`
	Industry  string `json:"industry,omitempty"`
}

// Code is a machine-readable reason. Used in metrics labels and the failed-rows CSV.
type Code string

const (
	CodeNoIdentifier   Code = "no_identifier"   // none of email / phone / linkedin
	CodeInvalidEmail   Code = "invalid_email"   // fails emailRE
	CodeArbitraryEmail Code = "arbitrary_email" // noreply@ / no-reply@
	CodeTooLong        Code = "too_long"        // a field exceeds its limit
	CodeInvalidPhone   Code = "invalid_phone"   // phone has no digits after flattening
)

// ValidationError is a row-level error: record it and move on.
type ValidationError struct {
	Row   int
	Field string
	Code  Code
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("row %d: %s: %s", e.Row, e.Field, e.Code)
}

// ErrNoIdentifierColumn is fatal: with no email/phone/linkedin column no row can be valid.
var ErrNoIdentifierColumn = errors.New("lead: CSV has no email, phone or linkedin column")

// aliases: normalised header name -> field. Several headers can map to one field.
var aliases = map[string]string{
	"firstname": "firstname", "first": "firstname",
	"lastname": "lastname", "last": "lastname", "surname": "lastname",
	"email": "email", "emailid": "email", "emailaddress": "email", "workemail": "email",
	"phone": "phone", "phonenumber": "phone", "mobile": "phone",
	"company": "company", "companyname": "company", "organization": "company",
	"title": "title", "jobtitle": "title", "designation": "title",
	"linkedin": "linkedin", "linkedinurl": "linkedin", "linkedinprofile": "linkedin",
	"city": "city", "country": "country", "industry": "industry",
}

// Mapping remembers which column index feeds which field. Build it once per file.
type Mapping struct {
	idx map[string]int // field -> column index
}

// NewMapping builds the mapping from the header row.
func NewMapping(header []string) (Mapping, error) {
	m := Mapping{idx: make(map[string]int)}
	for i, h := range header {
		field, ok := aliases[csvstream.NormalizeHeader(h)]
		if !ok {
			continue // unmapped column: ignored (custom fields are out of scope)
		}
		if _, dup := m.idx[field]; !dup { // first matching column wins
			m.idx[field] = i
		}
	}
	_, e := m.idx["email"]
	_, p := m.idx["phone"]
	_, l := m.idx["linkedin"]
	if !e && !p && !l {
		return Mapping{}, ErrNoIdentifierColumn
	}
	return m, nil
}

// Build turns a (well-formed) CSV row into a raw, not-yet-normalised Lead.
func (m Mapping) Build(accountID int64, row csvstream.Row) Lead {
	get := func(field string) string {
		i, ok := m.idx[field]
		if !ok || i >= len(row.Fields) {
			return ""
		}
		return row.Fields[i]
	}
	return Lead{
		Row:       row.Num,
		AccountID: accountID,
		FirstName: get("firstname"),
		LastName:  get("lastname"),
		Email:     get("email"),
		Phone:     get("phone"),
		Company:   get("company"),
		Title:     get("title"),
		LinkedIn:  get("linkedin"),
		City:      get("city"),
		Country:   get("country"),
		Industry:  get("industry"),
	}
}
