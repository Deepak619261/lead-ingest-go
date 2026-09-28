# lead-ingest-go

**A high-throughput, fault-tolerant CSV lead ingestion service in Go.** It streams a large CSV of sales leads, validates and de-duplicates them, and writes them to a lead API (Postgres or HTTP) through a bounded concurrent pipeline with batching, retries, backpressure, checkpoint/resume and graceful shutdown.

It was built to answer a real engineering question: *a legacy importer that processes leads row by row takes too long on 150k-lead files. Does rewriting it in Go fix that, or is it the architecture?*

| 150,000 leads → Postgres | Result |
|---|---|
| Legacy structure (per-row queries, whole file in memory) | ~3.9 min locally, 645 rows/s |
| **This pipeline, 8 writers** | **2.6 s, 57,190 rows/s, 55 MB peak memory, 0 duplicates** |
| Speed-up | **~89×**: ~12× from set-based batching, ~8× from bounded concurrency |

> Verdict from the benchmarks: the win is **architectural**, not linguistic. The whole import uses ~7 CPU-seconds, and the time went to N+1 database round-trips. Go's contribution is how simply the bounded pipeline can be expressed, plus a small static binary. See [docs/BENCHMARK.md](docs/BENCHMARK.md#8-is-it-go-or-is-it-the-architecture).

---

## Architecture

```
CSV stream ─► producer ─► [rowsCh] ─► W workers ─► [resultsCh] ─► router ─► [shard 0..S-1] ─► S batch writers ─► lead API
 (file/HTTP     parse       bounded     normalize     bounded      dedupe +     bounded          size/time flush     Postgres or HTTP
  upload/S3)                            + validate                 hash shard                    + retry/backoff     (idempotent upsert)
                  ▲                                                                                   │
                  └──────────────── backpressure: full channels pause reading ◄───────────────────────┘
```

- **Streaming, O(queue) memory.** `io.Reader` → `encoding/csv` with record reuse. A 500k-row file peaks at 75 MB.
- **Bounded worker pool + channels.** Every stage is a goroutine connected by bounded channels, so a slow database slows the reader instead of filling memory.
- **Two separate knobs.** `-workers` for CPU work, `-shards` for in-flight downstream requests. The benchmarks show only the second one matters.
- **Idempotency.**
  - Business key = account + email, then LinkedIn, then phone.
  - In-file duplicates are dropped.
  - Hash sharding guarantees one writer per key, so no insert race even without a unique index.
  - Sinks upsert, so retries and resumes never duplicate.
- **Set-based Postgres writes.** One transaction per batch: `= ANY($1)` prefetch → `INSERT … SELECT unnest(…)` → fill-missing `UPDATE … FROM unnest` → batched event rows. About 5 round-trips per 500 leads instead of ~8 per lead.
- **Classified errors.**
  - Row errors (malformed, invalid, duplicate) go to `rejects.csv` and the import continues.
  - 429/5xx/timeouts are retried with full-jitter exponential backoff, honouring `Retry-After`.
  - 401, bad config, or N consecutive failed batches stop the import.
- **Checkpoint/resume.** A low-watermark tracker stays correct while rows finish out of order across concurrent writers.
- **Cancellation and graceful shutdown.**
  - Soft stop: stop reading, flush, checkpoint.
  - Hard stop: cancel the context.
  - On SIGTERM, `ingest serve` drains, then soft-stops, then hard-stops.
- **Observability.** Prometheus metrics (rows by outcome, validation codes, sink latency histograms, retries, queue depth, active workers), pprof, and an end-of-run report showing **where the time went** per stage, with p50/p95/p99, peak memory, CPU and GC.

Details: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## Benchmarks (highlights)

Postgres, 150k rows, batch 500, median of 3 runs:

| Writers | 1 | 4 | **8** | 16 | 32 |
|---|---|---|---|---|---|
| Duration | 20.0 s | 3.6 s | **2.6 s** | 2.5 s | 3.4 s |
| Sink p99 | 105 ms | 210 ms | **265 ms** | 480 ms | 1,101 ms |

- **The knee is at 8 writers.** Beyond it, throughput flattens and then drops while tail latency grows 4×.
- **CPU workers don't matter.** With 8 writers, 1 to 16 validation workers all took 2.5–2.7 s; validation costs ~1 µs/row.
- **Over-concurrency against a rate-limited API is worse than too little.** 16 writers ran 5.5× slower, and 32 tripped the circuit breaker.
- **The benchmark found a real bug.** On a fresh table, `(account_id, email)` index order let the planner scan a whole tenant for every batch lookup, so the run took 64.7 s. Selective-column-first indexes cut each prefetch from 350 ms to 17 ms, and the import to 2.4–3.0 s.

Full report with methodology and caveats: [docs/BENCHMARK.md](docs/BENCHMARK.md). Production estimates: [docs/PREDICTIONS.md](docs/PREDICTIONS.md).

## Quick start

Requires Go 1.26+. Postgres is optional.

```bash
go build -o bin/ ./cmd/...
./bin/gencsv -n 150000 -out testdata/leads_150k.csv          # deterministic test data with dirty rows

# Option A: mock HTTP lead API (latency, capacity and fault injection)
./bin/mockengine -latency 30ms -per-lead 50us -capacity 8 &
./bin/ingest run -file testdata/leads_150k.csv -workers 4 -shards 8 -batch 500 \
    -rejects testdata/rejects.csv -checkpoint testdata/import.ckpt -metrics-addr :9091

# Option B: Postgres (creates its tables)
./bin/ingest run -file testdata/leads_150k.csv -sink pg -pg-url "postgres://user:pass@localhost:5432/leads"

# As a service: stream an upload straight into the pipeline (no temp file)
./bin/ingest serve -addr :8080 -sink pg -pg-url "$PG_URL"
curl -X POST --data-binary @testdata/leads_150k.csv "localhost:8080/imports?account=1"
curl -X DELETE localhost:8080/imports/<id>        # soft-cancel; the response carries a checkpoint to resume from
```

Full stack (Postgres + mock engine + service + Prometheus): `docker compose -f deploy/docker-compose.yml up --build`

## Failure handling

`bench/failures.ps1` plus the test suite cover these scenarios:

| Scenario | Behaviour |
|---|---|
| Malformed CSV row / invalid lead | rejected with a reason, import continues |
| HTTP 500 / timeout | retried with backoff, 0 rows lost |
| HTTP 429 | retried, `Retry-After` honoured |
| HTTP 401 | fatal, stops immediately |
| Downstream fully down | stops after N consecutive failed batches; checkpoint not advanced |
| Ctrl-C / cancel endpoint / SIGTERM | in-flight batches flushed, checkpoint written; resume yields exactly N records, 0 duplicates |

## Project layout
```
cmd/ingest          CLI (run) and HTTP service (serve) with graceful shutdown
cmd/mockengine      fake lead API: latency, capacity, 429/500/timeout/401 injection
cmd/gencsv          deterministic CSV generator with duplicate/invalid/malformed rows
internal/csvstream  streaming CSV reader (delimiter sniffing, BOM, row-level errors)
internal/lead       lead model: normalize, validate, idempotency key
internal/pipeline   producer → worker pool → router → sharded batch writers
internal/sink       Sink interface; httpsink (HTTP), pgsink (Postgres, set-based)
internal/retry      error classification + full-jitter exponential backoff
internal/checkpoint watermark tracker + atomic checkpoint file
internal/report     stage timings, latency percentiles, memory/CPU/GC
internal/metrics    Prometheus collectors
internal/legacy     model of the legacy per-row importer (benchmark baseline)
bench/              benchmark matrix, Postgres sweep, failure demos
deploy/             docker-compose + Prometheus config
```

## Tests
```bash
go test ./internal/... ./cmd/...                       # unit + integration (mock engine via httptest)
LEADINGEST_PG_URL=postgres://... go test ./internal/sink/pgsink   # Postgres integration
```
Covers:
- CSV edge cases, validation rules and retry classification;
- backpressure bounds, goroutine-leak checks on cancel, and soft stop + resume with zero duplicates;
- graceful shutdown of the HTTP service, and Postgres re-import idempotency.

## Tech
Go 1.26 · goroutines/channels/`errgroup` · `encoding/csv` · `net/http` · `jackc/pgx/v5` · Prometheus `client_golang` · `log/slog` · pprof · Docker (distroless)
