# Architecture

## Pipeline

```
                 ┌──────────────────────────┐
  CSV (file /    │ producer (1 goroutine)   │  csvstream: bufio + encoding/csv, ReuseRecord,
  HTTP upload /  │ stream-parse, skip rows  │  delimiter sniff, BOM strip. Malformed row = row error.
  S3 body)  ───► │ <= checkpoint            │
                 └────────────┬─────────────┘
                              │ rowsCh (bounded, QueueSize)      ◄── BACKPRESSURE: full => reader blocks
                 ┌────────────┼─────────────┐
                 ▼            ▼             ▼
            ┌─────────┐  ┌─────────┐   ┌─────────┐
            │worker 1 │  │worker 2 │ … │worker W │   map header → Lead, Normalize, Validate
            └────┬────┘  └────┬────┘   └────┬────┘   (CPU only, ~1µs/row)
                 └────────────┼─────────────┘
                              │ resultsCh (bounded)
                 ┌────────────▼─────────────┐
                 │ router (1 goroutine)     │  invalid/malformed → rejects.csv
                 │ in-file dedupe map[key]  │  duplicate key     → rejects.csv
                 │ shard = fnv(key) % S     │  same key ⇒ same writer, always
                 └──┬─────────┬─────────┬───┘
         shardCh[0] │         │         │ shardCh[S-1] (bounded, BatchSize)
                    ▼         ▼         ▼
              ┌──────────┐┌──────────┐┌──────────┐
              │writer 0  ││writer 1  ││writer S-1│  batch by size OR FlushInterval
              │batch+retry││          ││          │  retry.Do: backoff+jitter, Retry-After
              └────┬─────┘└────┬─────┘└────┬─────┘
                   └───────────┼───────────┘         S = max in-flight requests downstream
                               ▼
                 ┌──────────────────────────┐
                 │ Sink (interface)         │  httpsink: HTTP lead API (mock)
                 │ idempotent per key       │  pgsink:   Postgres, 1 tx/batch:
                 └──────────────────────────┘            prefetch(ANY) → INSERT unnest
                                                          → UPDATE fill-missing → events
   side channels: report.Collector (atomics) → end-of-run report + Prometheus /metrics
                  checkpoint.Tracker (watermark) → checkpoint file every 1s
```

All stages run under one `errgroup`: the first fatal error cancels the shared context and every
goroutine exits (each blocking send/receive is a `select` that also watches `ctx.Done()`).

## Two knobs, not one
| Knob | Controls | Bounded by |
|---|---|---|
| `-workers` (W) | CPU work: parse/validate | CPU cores. **Benchmarks show 1 worker is already enough** (~1 µs/row). |
| `-shards` (S) | concurrent requests to the downstream | **downstream capacity** (DB cores/connections, API rate limit). This is the real throughput knob. |

## Error model
| Class | Examples | Handling |
|---|---|---|
| Row | malformed CSV row, invalid email, too-long field, in-file duplicate | `rejects.csv` with reason, counters, import continues |
| Batch, transient | HTTP 429/5xx, timeout, connection reset, PG deadlock/serialization | retry with full-jitter backoff (honours `Retry-After`), up to `-retries` |
| Batch, permanent | HTTP 400, PG constraint/data error | batch rows → rejects, import continues |
| Fatal | 401/403, bad config, no identifier column, broken input stream, `-max-consecutive-failures` batches failing in a row | cancel everything, report + checkpoint |

## Idempotency
- **Business key**: `accountId + lower(email)`, else LinkedIn, else flattened phone. That's the legacy importer's match order, kept for behavioural parity.
- **In-file**: the router drops later rows with an already-seen key.
- **Across writers**: hash sharding sends a key to exactly one writer, so two concurrent transactions can never both insert the same person. This matters because the prospects table has **no unique index** on email (common in CRM schemas).
- **Across retries and restarts**: sinks upsert by key, so re-sending a batch turns inserts into updates, never duplicates. Postgres writes a batch in one transaction, so nothing partial commits.
- **Resume**: `checkpoint.Tracker` keeps a low watermark (all rows ≤ W finished). Rows are finished out of order, so "last row written" would be wrong. Transient failures that exhausted their retries do **not** advance the watermark, so a resume re-sends them.

- **Known limit**: sharding serializes the *primary* key (email). Two different emails that share a phone/LinkedIn can land on different shards, so the fallback phone match is not serialized: one run may insert both, another may merge them. The legacy per-row importer behaves the same way (it has no in-file phone dedupe). The fix, if needed, is a unique partial index or an advisory lock on the flattened phone.

## Postgres finding: index column order + missing statistics
- **Symptom.** The first Postgres run took **64.7 s**, with p95 sink latency at 8.1 s. Batch prefetch cost grew with table size.
- **Cause.** The table was new, or truncated, and had no statistics. The planner assumed `account_id = $1` was very selective and used any index that *starts with* `account_id`, only on that column. Every row of the account came back and was filtered one by one against the 500-key array.
- **Fixes.**
  1. Indexes put the selective column first: `(lower(email), account_id)`, `(linkedin, account_id)`, `(phone, account_id)`. Now no index can serve `account_id` alone, and the plan is a BitmapOr over the three lookups: 350 ms → 17 ms per prefetch on a 141k-row table.
  2. `SET LOCAL enable_seqscan = off` in the batch transaction, because a ≤500-key lookup should never seq-scan.
- **Result.** 150k rows into brand-new tables in **2.4–3.0 s** (3 runs).
- **Takeaway.** The common `(tenant_id, email)` index order is fragile for batched `= ANY(...)` lookups.

## Cancellation and shutdown
- **Hard stop** = cancel `ctx`: in-flight requests are aborted, and unconfirmed rows stay below the watermark.
- **Soft stop** = close `cfg.Stop`: stop reading, then drain workers, router and writers, flush tails, and write the final checkpoint.
- **CLI**: the first Ctrl-C is a soft stop, the second a hard stop.
- **Service (`ingest serve`)** on SIGTERM:
  1. `/healthz` returns 503 and new connections are refused.
  2. Running imports get `-drain` to finish.
  3. They are then soft-stopped and get `-grace` to flush and checkpoint. Each response carries `checkpoint`, and the client re-POSTs with `?resume=N`.
  4. Anything still running after that is hard-cancelled.

## Memory
O(QueueSize×2 + Shards×BatchSize) rows in flight, plus the in-file dedupe map (≈100 B per unique key: 15 MB at 150k, 50 MB at 500k).
For multi-million-row files, replace the map with a per-shard Bloom filter plus the sink's prefetch.

## Legacy importer vs this service
| Legacy per-row importer | This service |
|---|---|
| whole file read into one string, then a list of all rows | stream from `io.Reader`, bounded channels |
| `foreach` row: ~7–11 sequential DB queries | one prefetch query per 500-row batch (`= ANY($1)`) |
| 500-row chunks written sequentially | S concurrent writers, each batch one transaction |
| one event-queue INSERT per prospect | one INSERT … SELECT unnest per batch |
| no in-file dedupe, no retries | in-file dedupe, classified retries, circuit-breaker style abort |
| offset = rows processed (only valid when sequential) | watermark that stays correct under concurrency |
