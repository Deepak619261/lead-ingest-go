package pgsink

import "leadingest/internal/lead"

func leadWith(email, first, company string) lead.Lead {
	return lead.Lead{AccountID: 1, Email: email, FirstName: first, Company: company}
}
