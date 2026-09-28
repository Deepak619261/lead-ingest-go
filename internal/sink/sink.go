// Package sink defines where validated leads go (the downstream lead store).
package sink

import (
	"context"

	"leadingest/internal/lead"
)

// Mode is what to do when a lead already exists.
type Mode string

const (
	ModeUpdateMissing Mode = "update" // default: fill empty fields of the existing prospect
	ModeSkipExisting  Mode = "skip"   // leave existing prospects untouched
)

// Batch is one write unit; its keys are unique and no other in-flight batch shares them.
type Batch struct {
	ImportID  string      `json:"importId"`
	AccountID int64       `json:"accountId"`
	Mode      Mode        `json:"mode"`
	Leads     []lead.Lead `json:"leads"`
}

// Result counts what happened to a batch.
type Result struct {
	Inserted int `json:"inserted"`
	Updated  int `json:"updated"`
	Skipped  int `json:"skipped"` // existed and Mode == skip
}

// Sink must be concurrency-safe and idempotent per key, so retries and resumes never duplicate.
type Sink interface {
	Write(ctx context.Context, b Batch) (Result, error)
	Close() error
}

// Lookuper does one round-trip per call; only the legacy (per-row model) runner uses it.
type Lookuper interface {
	LookupOne(ctx context.Context, accountID int64, l lead.Lead) error
}
