// Package checkpoint makes imports resumable.
package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Tracker computes the watermark: highest row W such that rows 1..W are all finished.
type Tracker struct {
	mu      sync.Mutex
	low     int
	pending map[int]struct{}
}

// NewTracker starts with every row <= start already done (resume).
func NewTracker(start int) *Tracker {
	return &Tracker{low: start, pending: make(map[int]struct{})}
}

// Done marks rows finished.
func (t *Tracker) Done(rows ...int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, r := range rows {
		if r <= t.low {
			continue
		}
		t.pending[r] = struct{}{}
	}
	for { // advance while the next row is done
		if _, ok := t.pending[t.low+1]; !ok {
			return
		}
		delete(t.pending, t.low+1)
		t.low++
	}
}

// Watermark returns W: all rows 1..W are done.
func (t *Tracker) Watermark() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.low
}

// State is what's persisted.
type State struct {
	ImportID  string `json:"importId"`
	AccountID int64  `json:"accountId"`
	Watermark int    `json:"watermark"`
}

// Save writes State atomically (temp file + rename), so a crash never leaves a half-written file.
func Save(path string, s State) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".ckpt-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// Load reads State. A missing file returns (zero State, nil): nothing to resume.
func Load(path string) (State, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return State{}, nil
	}
	if err != nil {
		return State{}, err
	}
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return State{}, fmt.Errorf("checkpoint %s is corrupt: %w", path, err)
	}
	return s, nil
}
