package lead

import (
	"strconv"
	"strings"
	"unicode/utf8"
)

// Normalize cleans the lead in place (pointer receiver: it mutates the caller's value).
func (l *Lead) Normalize() {
	// A slice of pointers lets us trim all fields in one loop.
	for _, p := range []*string{
		&l.FirstName, &l.LastName, &l.Email, &l.Phone, &l.Company,
		&l.Title, &l.LinkedIn, &l.City, &l.Country, &l.Industry,
	} {
		*p = strings.TrimSpace(*p)
	}
	l.Email = strings.ToLower(l.Email)
	l.Phone = flattenPhone(l.Phone)
	l.LinkedIn = strings.TrimSuffix(l.LinkedIn, "/")
}

// flattenPhone keeps a leading '+' and digits: " +1 (415) 555-0100" -> "+14155550100".
func flattenPhone(s string) string {
	var b strings.Builder
	b.Grow(len(s)) // one allocation, sized up front
	for i, r := range s {
		if (r == '+' && i == 0) || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// Validate returns nil or a *ValidationError for the first problem; call it after Normalize.
func (l Lead) Validate() error {
	fail := func(field string, code Code) error {
		return &ValidationError{Row: l.Row, Field: field, Code: code}
	}

	if l.Email == "" && l.Phone == "" && l.LinkedIn == "" {
		return fail("email", CodeNoIdentifier)
	}
	if l.Email != "" && !emailRE.MatchString(l.Email) {
		return fail("email", CodeInvalidEmail)
	}
	if strings.HasPrefix(l.Email, "noreply@") || strings.HasPrefix(l.Email, "no-reply@") {
		return fail("email", CodeArbitraryEmail)
	}
	if l.Phone != "" && strings.IndexAny(l.Phone, "0123456789") < 0 {
		return fail("phone", CodeInvalidPhone)
	}

	// Limits count runes, not bytes ("Müller" is 6 runes, 7 bytes).
	limits := [...]struct {
		field string
		value string
		max   int
	}{
		{"firstname", l.FirstName, MaxFirstName},
		{"lastname", l.LastName, MaxLastName},
		{"email", l.Email, MaxEmail},
		{"title", l.Title, MaxTitle},
		{"phone", l.Phone, MaxPhone},
		{"company", l.Company, MaxDefault},
		{"city", l.City, MaxDefault},
		{"country", l.Country, MaxDefault},
		{"industry", l.Industry, MaxDefault},
		{"linkedin", l.LinkedIn, MaxDefault},
	}
	for _, lim := range limits {
		// Cheap check first: a string can't have more runes than bytes.
		if len(lim.value) > lim.max && utf8.RuneCountInString(lim.value) > lim.max {
			return fail(lim.field, CodeTooLong)
		}
	}
	return nil
}

// Key is the dedupe/idempotency key: account + email, else LinkedIn, else phone (legacy match order).
func (l Lead) Key() string {
	acct := strconv.FormatInt(l.AccountID, 10)
	switch {
	case l.Email != "":
		return acct + ":e:" + l.Email
	case l.LinkedIn != "":
		return acct + ":l:" + l.LinkedIn
	case l.Phone != "":
		return acct + ":p:" + l.Phone
	}
	return ""
}
