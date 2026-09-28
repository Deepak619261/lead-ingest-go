// Package pgsink writes batches to Postgres set-based: ~5 round-trips per BATCH instead of ~8 per ROW.
package pgsink

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"leadingest/internal/lead"
	"leadingest/internal/retry"
	"leadingest/internal/sink"
)

//go:embed schema.sql
var schemaSQL string

const (
	sourceCSVImport  = "csv_import"
	eventTypeCreated = "prospect.created"
	eventTypeUpdated = "prospect.updated"
)

// Sink implements sink.Sink and sink.Lookuper.
type Sink struct{ pool *pgxpool.Pool }

var (
	_ sink.Sink     = (*Sink)(nil)
	_ sink.Lookuper = (*Sink)(nil)
)

// New connects a pool sized for the writer concurrency.
func New(ctx context.Context, url string, maxConns int32) (*Sink, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, retry.Fatal(fmt.Errorf("pg url: %w", err))
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, retry.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, classify(ctx, fmt.Errorf("pg ping: %w", err))
	}
	return &Sink{pool: pool}, nil
}

// EnsureSchema creates the tables if missing.
func (s *Sink) EnsureSchema(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

// Reset empties the tables (benchmarks only).
func (s *Sink) Reset(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, "TRUNCATE prospects, prospect_events RESTART IDENTITY")
	return err
}

// Stats returns prospect rows, event rows, and (account, lower(email)) groups with more than one row.
func (s *Sink) Stats(ctx context.Context) (prospects, events, duplicateEmails int64, err error) {
	err = s.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM prospects),
		(SELECT count(*) FROM prospect_events),
		(SELECT count(*) FROM (SELECT 1 FROM prospects WHERE email IS NOT NULL
		   GROUP BY account_id, lower(email) HAVING count(*) > 1) d)`).Scan(&prospects, &events, &duplicateEmails)
	return
}

// Write runs one transaction per batch; a retry after any failure is safe because nothing partial commits.
func (s *Sink) Write(ctx context.Context, b sink.Batch) (sink.Result, error) {
	var res sink.Result
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Point lookups of <=500 keys must use the indexes even when stats are stale (e.g. a freshly truncated table).
		if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
			return err
		}
		existing, err := prefetch(ctx, tx, b)
		if err != nil {
			return err
		}

		var inserts []lead.Lead
		var updateIDs []int64
		var updates []lead.Lead
		for i, l := range b.Leads {
			if id := existing[i]; id != 0 {
				if b.Mode == sink.ModeSkipExisting {
					res.Skipped++
					continue
				}
				updateIDs = append(updateIDs, id)
				updates = append(updates, l)
				continue
			}
			inserts = append(inserts, l)
		}

		newIDs, err := insert(ctx, tx, b, inserts)
		if err != nil {
			return err
		}
		if err := update(ctx, tx, updateIDs, updates); err != nil {
			return err
		}
		if err := events(ctx, tx, b.AccountID, newIDs, updateIDs); err != nil {
			return err
		}
		res.Inserted, res.Updated = len(newIDs), len(updateIDs)
		return nil
	})
	if err != nil {
		return sink.Result{}, classify(ctx, err)
	}
	return res, nil
}

// prefetch finds existing prospects for the whole batch in ONE query; returns the existing id per lead index (0 = new).
func prefetch(ctx context.Context, tx pgx.Tx, b sink.Batch) ([]int64, error) {
	emails, linkedins, phones := make([]string, 0, len(b.Leads)), []string{}, []string{}
	for _, l := range b.Leads {
		if l.Email != "" {
			emails = append(emails, l.Email)
		}
		if l.LinkedIn != "" {
			linkedins = append(linkedins, l.LinkedIn)
		}
		if l.Phone != "" {
			phones = append(phones, l.Phone)
		}
	}
	rows, err := tx.Query(ctx, `
		SELECT id, coalesce(lower(email),''), coalesce(linkedin,''), coalesce(phone,'')
		FROM prospects
		WHERE account_id = $1 AND deleted_at IS NULL
		  AND (lower(email) = ANY($2) OR linkedin = ANY($3) OR phone = ANY($4))`,
		b.AccountID, emails, linkedins, phones)
	if err != nil {
		return nil, err
	}
	byEmail, byLinkedIn, byPhone := map[string]int64{}, map[string]int64{}, map[string]int64{}
	for rows.Next() {
		var id int64
		var e, li, ph string
		if err := rows.Scan(&id, &e, &li, &ph); err != nil {
			return nil, err
		}
		put := func(m map[string]int64, k string) {
			if _, ok := m[k]; k != "" && !ok {
				m[k] = id
			}
		}
		put(byEmail, e)
		put(byLinkedIn, li)
		put(byPhone, ph)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Match order: email, then LinkedIn, then phone.
	out := make([]int64, len(b.Leads))
	for i, l := range b.Leads {
		switch {
		case l.Email != "" && byEmail[l.Email] != 0:
			out[i] = byEmail[l.Email]
		case l.LinkedIn != "" && byLinkedIn[l.LinkedIn] != 0:
			out[i] = byLinkedIn[l.LinkedIn]
		case l.Phone != "" && byPhone[l.Phone] != 0:
			out[i] = byPhone[l.Phone]
		}
	}
	return out, nil
}

// columns turns leads into parallel arrays for unnest(): one round-trip for N rows.
type columns struct{ email, first, last, title, company, phone, linkedin, city, country, industry []string }

func toColumns(ls []lead.Lead) columns {
	c := columns{}
	for _, l := range ls {
		c.email = append(c.email, l.Email)
		c.first = append(c.first, l.FirstName)
		c.last = append(c.last, l.LastName)
		c.title = append(c.title, l.Title)
		c.company = append(c.company, l.Company)
		c.phone = append(c.phone, l.Phone)
		c.linkedin = append(c.linkedin, l.LinkedIn)
		c.city = append(c.city, l.City)
		c.country = append(c.country, l.Country)
		c.industry = append(c.industry, l.Industry)
	}
	return c
}

func insert(ctx context.Context, tx pgx.Tx, b sink.Batch, ls []lead.Lead) ([]int64, error) {
	if len(ls) == 0 {
		return nil, nil
	}
	c := toColumns(ls)
	rows, err := tx.Query(ctx, `
		INSERT INTO prospects (account_id, email, first_name, last_name, title, company,
		                       phone, linkedin, city, country, industry, source, import_id)
		SELECT $1, NULLIF(e,''), NULLIF(f,''), NULLIF(l,''), NULLIF(t,''), NULLIF(c,''),
		       NULLIF(p,''), NULLIF(li,''), NULLIF(ci,''), NULLIF(co,''), NULLIF(ind,''), $2, $3
		FROM unnest($4::text[], $5::text[], $6::text[], $7::text[], $8::text[],
		            $9::text[], $10::text[], $11::text[], $12::text[], $13::text[])
		     AS u(e, f, l, t, c, p, li, ci, co, ind)
		RETURNING id`,
		b.AccountID, sourceCSVImport, b.ImportID,
		c.email, c.first, c.last, c.title, c.company, c.phone, c.linkedin, c.city, c.country, c.industry)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// update fills only the columns that are empty today (existing values win).
func update(ctx context.Context, tx pgx.Tx, ids []int64, ls []lead.Lead) error {
	if len(ids) == 0 {
		return nil
	}
	c := toColumns(ls)
	_, err := tx.Exec(ctx, `
		UPDATE prospects p SET
			first_name  = COALESCE(NULLIF(p.first_name,''),  NULLIF(u.f,'')),
			last_name   = COALESCE(NULLIF(p.last_name,''),   NULLIF(u.l,'')),
			title       = COALESCE(NULLIF(p.title,''),       NULLIF(u.t,'')),
			company     = COALESCE(NULLIF(p.company,''),     NULLIF(u.c,'')),
			phone       = COALESCE(NULLIF(p.phone,''),       NULLIF(u.p,'')),
			linkedin    = COALESCE(NULLIF(p.linkedin,''),    NULLIF(u.li,'')),
			city        = COALESCE(NULLIF(p.city,''),        NULLIF(u.ci,'')),
			country     = COALESCE(NULLIF(p.country,''),     NULLIF(u.co,'')),
			industry    = COALESCE(NULLIF(p.industry,''),    NULLIF(u.ind,'')),
			updated_at  = now()
		FROM unnest($1::bigint[], $2::text[], $3::text[], $4::text[], $5::text[],
		            $6::text[], $7::text[], $8::text[], $9::text[], $10::text[])
		     AS u(id, f, l, t, c, p, li, ci, co, ind)
		WHERE p.id = u.id`,
		ids, c.first, c.last, c.title, c.company, c.phone, c.linkedin, c.city, c.country, c.industry)
	return err
}

// events writes created/updated outbox rows in one statement instead of one INSERT per prospect.
func events(ctx context.Context, tx pgx.Tx, accountID int64, created, updated []int64) error {
	if len(created)+len(updated) == 0 {
		return nil
	}
	ids := append(append(make([]int64, 0, len(created)+len(updated)), created...), updated...)
	types := make([]string, 0, len(ids))
	for range created {
		types = append(types, eventTypeCreated)
	}
	for range updated {
		types = append(types, eventTypeUpdated)
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO prospect_events (account_id, prospect_id, event_type, payload)
		SELECT $1, id, t, '{"source":"csv_import"}' FROM unnest($2::bigint[], $3::text[]) AS u(id, t)`,
		accountID, ids, types)
	return err
}

// LookupOne is a single-row existence query (the legacy runner uses it to model per-row lookups).
func (s *Sink) LookupOne(ctx context.Context, accountID int64, l lead.Lead) error {
	var id int64
	err := s.pool.QueryRow(ctx,
		`SELECT id FROM prospects WHERE account_id = $1 AND lower(email) = $2 LIMIT 1`,
		accountID, l.Email).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return classify(ctx, err)
	}
	return nil
}

// Close closes the pool.
func (s *Sink) Close() error { s.pool.Close(); return nil }

// classify maps Postgres/network errors onto retry classes.
func classify(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case pgErr.Code == "40001", pgErr.Code == "40P01", pgErr.Code == "53300", pgErr.Code == "57P01",
			strings.HasPrefix(pgErr.Code, "08"):
			return retry.Retryable(err, 0) // serialization, deadlock, too many conns, shutdown, connection
		case strings.HasPrefix(pgErr.Code, "28"), pgErr.Code == "3D000", pgErr.Code == "42P01":
			return retry.Fatal(err) // auth, missing database, missing table
		default:
			return err // data problem: permanent for this batch
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) || pgconn.SafeToRetry(err) || errors.Is(err, context.DeadlineExceeded) {
		return retry.Retryable(err, 0)
	}
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return retry.Retryable(err, 0)
	}
	return err
}
