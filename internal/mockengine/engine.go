// Package mockengine is a fake lead API: upserts by key, simulated latency, injected 429/500/timeouts/401.
package mockengine

import (
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"leadingest/internal/lead"
	"leadingest/internal/sink"
)

// Config controls latency and fault injection.
type Config struct {
	BaseLatency   time.Duration // per bulk request (network + fixed server cost)
	PerLead       time.Duration // additional cost per lead in a bulk request
	LookupLatency time.Duration // per single-row lookup (models one DB round-trip)
	P429          float64       // probability a request gets 429
	P500          float64       // probability a request gets 500
	PSlow         float64       // probability a request hangs for SlowFor (to trigger client timeouts)
	SlowFor       time.Duration
	RetryAfter    time.Duration // value sent in Retry-After on 429
	MaxInFlight   int           // >0: reject with 429 above this many concurrent bulk requests
	Capacity      int           // >0: at most this many requests are served at once, the rest queue (like DB cores)
	Token         string        // non-empty: require "Authorization: Bearer <Token>"
}

// Stats is what GET /stats returns.
type Stats struct {
	Records         int   `json:"records"`
	Inserts         int64 `json:"inserts"`
	Updates         int64 `json:"updates"`
	Skips           int64 `json:"skips"`
	BulkRequests    int64 `json:"bulkRequests"`
	Lookups         int64 `json:"lookups"`
	Rejected429     int64 `json:"rejected429"`
	Injected500     int64 `json:"injected500"`
	InjectedSlow    int64 `json:"injectedSlow"`
	MaxInFlightSeen int64 `json:"maxInFlightSeen"`
}

// Engine is an http.Handler.
type Engine struct {
	cfg Config
	mux *http.ServeMux

	mu    sync.Mutex
	store map[string]lead.Lead

	inflight atomic.Int64
	slots    chan struct{}
	stats    struct {
		inserts, updates, skips, bulk, lookups, r429, r500, slow, maxInflight atomic.Int64
	}
}

// New builds an engine.
func New(cfg Config) *Engine {
	if cfg.SlowFor == 0 {
		cfg.SlowFor = 30 * time.Second
	}
	if cfg.RetryAfter == 0 {
		cfg.RetryAfter = time.Second
	}
	e := &Engine{cfg: cfg, mux: http.NewServeMux(), store: make(map[string]lead.Lead)}
	if cfg.Capacity > 0 {
		e.slots = make(chan struct{}, cfg.Capacity)
	}
	e.mux.HandleFunc("POST /v1/leads/bulk", e.bulk)
	e.mux.HandleFunc("GET /v1/leads/lookup", e.lookup)
	e.mux.HandleFunc("GET /stats", e.getStats)
	e.mux.HandleFunc("POST /reset", e.reset)
	return e
}

func (e *Engine) ServeHTTP(w http.ResponseWriter, r *http.Request) { e.mux.ServeHTTP(w, r) }

// sleep waits d or until the client goes away.
func sleep(r *http.Request, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-r.Context().Done():
		return false
	}
}

func (e *Engine) authorised(w http.ResponseWriter, r *http.Request) bool {
	if e.cfg.Token != "" && r.Header.Get("Authorization") != "Bearer "+e.cfg.Token {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func (e *Engine) bulk(w http.ResponseWriter, r *http.Request) {
	if !e.authorised(w, r) {
		return
	}
	e.stats.bulk.Add(1)

	n := e.inflight.Add(1)
	defer e.inflight.Add(-1)
	for { // record the high-water mark of concurrency
		m := e.stats.maxInflight.Load()
		if n <= m || e.stats.maxInflight.CompareAndSwap(m, n) {
			break
		}
	}

	// Too many concurrent requests => 429: the downstream limit that makes more writers stop helping.
	if e.cfg.MaxInFlight > 0 && n > int64(e.cfg.MaxInFlight) {
		e.tooMany(w)
		return
	}
	switch p := rand.Float64(); {
	case p < e.cfg.P429:
		e.tooMany(w)
		return
	case p < e.cfg.P429+e.cfg.P500:
		e.stats.r500.Add(1)
		sleep(r, e.cfg.BaseLatency)
		http.Error(w, "injected failure", http.StatusInternalServerError)
		return
	case p < e.cfg.P429+e.cfg.P500+e.cfg.PSlow:
		e.stats.slow.Add(1)
		sleep(r, e.cfg.SlowFor) // client should time out and retry
		http.Error(w, "too slow", http.StatusGatewayTimeout)
		return
	}

	var b sink.Batch
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if e.slots != nil {
		select {
		case e.slots <- struct{}{}:
			defer func() { <-e.slots }()
		case <-r.Context().Done():
			return
		}
	}
	if !sleep(r, e.cfg.BaseLatency+time.Duration(len(b.Leads))*e.cfg.PerLead) {
		return // client gave up; don't apply (a real server might have, which is why we upsert)
	}

	var res sink.Result
	e.mu.Lock()
	for _, l := range b.Leads {
		k := l.Key()
		old, exists := e.store[k]
		switch {
		case !exists:
			e.store[k] = l
			res.Inserted++
		case b.Mode == sink.ModeSkipExisting:
			res.Skipped++
		default:
			e.store[k] = fillMissing(old, l)
			res.Updated++
		}
	}
	e.mu.Unlock()

	e.stats.inserts.Add(int64(res.Inserted))
	e.stats.updates.Add(int64(res.Updated))
	e.stats.skips.Add(int64(res.Skipped))
	writeJSON(w, res)
}

func (e *Engine) tooMany(w http.ResponseWriter) {
	e.stats.r429.Add(1)
	w.Header().Set("Retry-After", strconv.Itoa(int(e.cfg.RetryAfter.Seconds())))
	http.Error(w, "rate limited", http.StatusTooManyRequests)
}

// fillMissing keeps existing values and fills blanks (update-missing mode).
func fillMissing(old, in lead.Lead) lead.Lead {
	for _, f := range []struct {
		dst *string
		src string
	}{
		{&old.FirstName, in.FirstName}, {&old.LastName, in.LastName}, {&old.Phone, in.Phone},
		{&old.Company, in.Company}, {&old.Title, in.Title}, {&old.LinkedIn, in.LinkedIn},
		{&old.City, in.City}, {&old.Country, in.Country}, {&old.Industry, in.Industry},
	} {
		if *f.dst == "" {
			*f.dst = f.src
		}
	}
	return old
}

func (e *Engine) lookup(w http.ResponseWriter, r *http.Request) {
	if !e.authorised(w, r) {
		return
	}
	e.stats.lookups.Add(1)
	if !sleep(r, e.cfg.LookupLatency) {
		return
	}
	e.mu.Lock()
	_, found := e.store[r.URL.Query().Get("key")]
	e.mu.Unlock()
	writeJSON(w, map[string]bool{"found": found})
}

// Snapshot returns current stats (also used directly by tests).
func (e *Engine) Snapshot() Stats {
	e.mu.Lock()
	records := len(e.store)
	e.mu.Unlock()
	return Stats{
		Records:         records,
		Inserts:         e.stats.inserts.Load(),
		Updates:         e.stats.updates.Load(),
		Skips:           e.stats.skips.Load(),
		BulkRequests:    e.stats.bulk.Load(),
		Lookups:         e.stats.lookups.Load(),
		Rejected429:     e.stats.r429.Load(),
		Injected500:     e.stats.r500.Load(),
		InjectedSlow:    e.stats.slow.Load(),
		MaxInFlightSeen: e.stats.maxInflight.Load(),
	}
}

func (e *Engine) getStats(w http.ResponseWriter, _ *http.Request) { writeJSON(w, e.Snapshot()) }

func (e *Engine) reset(w http.ResponseWriter, _ *http.Request) {
	e.mu.Lock()
	e.store = make(map[string]lead.Lead)
	e.mu.Unlock()
	s := &e.stats
	for _, c := range []*atomic.Int64{&s.inserts, &s.updates, &s.skips, &s.bulk, &s.lookups, &s.r429, &s.r500, &s.slow, &s.maxInflight} {
		c.Store(0)
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
