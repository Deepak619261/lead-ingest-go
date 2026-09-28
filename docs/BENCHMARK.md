# Benchmark report

**Machine:** laptop, Intel i5-12450H (12 threads), Windows 11, Go 1.26.2, PostgreSQL 17.6 (local).
**Data:** `gencsv` with a fixed seed, so every run reads identical bytes. It has ~2% in-file duplicates (different case), ~3% invalid rows, ~0.1% malformed rows. The 150k-row file is 21.7 MB.
**How it's measured:** each run is one `ingest run` process, and the numbers come from its own report (wall clock, per-stage timers, `runtime/metrics` sampled every 100 ms for memory and CPU, and a latency sample of every sink call). Details in [How it's measured](#how-its-measured).

Labels used below:
- **[PG]** measured against real local Postgres.
- **[SIM]** measured against the mock lead API. Its latency (30 ms + 50 µs/lead per request, 8 served at once) is an *assumption*, so read these results for their **shape**, not their absolute values.
- **[MODEL]** the "legacy" runner, which reproduces the *structure* of the per-row importer being replaced. It is a model, not a separate binary.

## 1. Headline: 150,000 leads

| | Duration | Rows/s | Peak memory (runtime) | CPU used |
|---|---|---|---|---|
| **Go pipeline → Postgres, 8 writers** [PG], median of 3 | **2.6 s** | **57,190** | 55 MB | ~7 s CPU |
| Go pipeline → Postgres, 1 writer [PG], median of 3 | 20.0 s | 7,499 | 43 MB | |
| Legacy model → Postgres, 8 real queries/row [PG, MODEL] | 10k rows in 15.5 s ⇒ **~3.9 min** for 150k | 645 | holds every row | |
| Go pipeline → mock engine, 8 writers [SIM] | 2.6 s | 58,816 | 47 MB | 5.1 s CPU |
| Legacy model → mock engine [SIM, MODEL] | 50k rows in 824 s ⇒ **~41 min** for 150k | 61 | 37 MB at 50k and growing | |

Against local Postgres the pipeline is **~89× faster** than the legacy structure (645 → 57,190 rows/s).
The simulated engine charges ~2 ms per lookup, and the gap grows to ~960×. Production latency sits between the two (see PREDICTIONS.md).

## 2. Where the speed-up comes from (Postgres, 150k)

| Step | rows/s | Gain | What changed |
|---|---|---|---|
| Legacy structure | 645 | — | 8 sequential single-row queries per row, then a 500-row bulk write |
| + set-based prefetch (1 query per 500 rows), batched events, streaming. **Still 1 writer** | 7,499 | **11.6×** | no concurrency at all |
| + 8 concurrent writers | 57,190 | **7.6×** | goroutines + channels + sharding |
| + 16 writers | 60,320 | 1.05× | knee reached |

**About 12× comes from doing less work, and about 8× from doing it concurrently. Neither depends on the language.**

## 3. Concurrency sweep: where more stops helping

Postgres [PG], 150k rows, batch 500, median of 3 runs:

| Writers | Duration | Rows/s | Sink p99 | Peak mem |
|---|---|---|---|---|
| 1 | 20.0 s | 7,499 | 105 ms | 43 MB |
| 4 | 3.6 s | 41,444 | 210 ms | 51 MB |
| **8** | **2.6 s** | **57,190** | **265 ms** | 55 MB |
| 16 | 2.5 s | 60,320 | 480 ms | 80 MB |
| 32 | 3.4 s | 44,130 | **1,101 ms** | 108 MB |

Mock engine with 8 slots [SIM], workers = writers = N, 150k rows:

| N | 1 | 2 | 4 | 8 | 16 | 32 |
|---|---|---|---|---|---|---|
| Duration (s) | 18.9 | 9.6 | 4.9 | **2.6** | 2.2 | 2.3 |
| Rows/s | 7,943 | 15,688 | 30,387 | 58,816 | 66,916 | 64,537 |

**Knee: 8 writers.** Scaling is linear up to the downstream's capacity. Past it you gain little, tail latency climbs (Postgres p99 goes 265 ms → 1.1 s), and past 16 throughput *drops* because of lock, WAL and CPU contention on the database.

### Validation workers alone don't matter [SIM]
With writers fixed at 8, **1, 2, 4, 8 or 16 validation workers all took 2.5–2.7 s.** Parsing plus validation costs ~1 µs/row (0.3 s parse + 0.6 s validate for 150k, summed over goroutines). The CPU side is never the bottleneck, so the concurrency knob that matters is **in-flight requests to the downstream**, not CPU workers.

### Exceeding a rate limit is worse than being too slow [SIM]
Here the mock engine returns 429 above 8 in-flight requests (Retry-After: 1 s):

| Writers | 4 | 8 | 16 | 32 |
|---|---|---|---|---|
| Result | 4.9 s | **2.7 s** | 14.6 s (5.5× slower) | **import aborted**: consecutive 429s tripped the "engine unavailable" guard |

## 4. Batch size

| Batch | 100 | 250 | 500 | 1000 | 2000 |
|---|---|---|---|---|---|
| [SIM] duration | 7.5 s | 3.8 s | 2.7 s | 2.0 s | 1.7 s |
| [SIM] peak heap | 25 MB | 28 MB | 32 MB | 41 MB | 55 MB |
| [PG] duration (single runs, noisy) | 8.0 s | 6.4 s | 6.1 s | 6.1 s | 7.0 s |
| [PG] sink p99 | 93 ms | 278 ms | 501 ms | 523 ms | 992 ms |

- On the simulated engine, bigger is always better, because it amortizes a fixed 30 ms per request.
- On real Postgres, **250–1000 is a plateau**. 2000 is slower, with twice the tail latency: each transaction holds locks longer and writes more WAL at once.
- **500 is the right default**, and it matches the legacy importer's chunk size.

## 5. Dataset size [SIM, 8 writers, batch 500]

| Rows | 10k | 50k | 100k | 150k | 500k |
|---|---|---|---|---|---|
| Duration | 0.27 s | 0.87 s | 1.76 s | 2.59 s | 8.49 s |
| Rows/s | 37k | 57k | 57k | 58k | 59k |
| Peak heap | 10 MB | 18 MB | 26 MB | 34 MB | 75 MB |

Throughput is flat, so the pipeline scales linearly. Memory grows slowly, and **only** because of the in-file dedupe map (~100 B per unique key); the row queues are fixed-size.
The legacy structure holds every row: 9 MB at 10k and 37 MB at 50k, heading past 100 MB at 150k.

## 6. Failure handling (`bench/failures.ps1`, 20k rows, mock engine)

| Scenario | Result |
|---|---|
| Malformed rows + invalid leads | rejected one by one into `rejects.csv` with a reason; import finished, exit 0 |
| 20% HTTP 500 | 8 retries, **0 rows lost** (18,962 records, same as a clean run) |
| 20% HTTP 429 (Retry-After 1 s) | 13 retries, each waiting ≥ 1 s, 0 lost |
| 5% of requests hang 30 s (client timeout 1 s) | 2 retries, 0 lost |
| 401 wrong token | **fatal**: stopped at once, exit 1 |
| Engine 100% down | stopped after 5 consecutive failed batches; checkpoint stays at 0, so a resume re-sends them |
| Hard cancel mid-import | stops within ms, no goroutine leak (`TestHardCancelStopsPromptlyWithoutLeaks`) |
| Soft stop, then resume from checkpoint | **exactly 8,000 of 8,000 records, 0 duplicates** (`TestSoftStopThenResumeHasNoDuplicates`) |
| SIGTERM on `ingest serve` mid-upload | drains, soft-stops, responds with a checkpoint; a new instance resumes to exactly 20,000 records (`TestGracefulShutdownCheckpointsAndResumes`) |
| Postgres re-import of the same file | every row becomes an update; **0 duplicate emails** (`TestPipelineIntoPostgresNoDuplicates`) |

## 7. A bug the benchmark found
- **Symptom.** The first Postgres run took **64.7 s**, with p95 sink latency at 8.1 s.
- **Cause.** On a table with no statistics, the planner used an `(account_id, …)` index on `account_id` alone. With one account that means scanning every row, then filtering against the 500-key array, so the cost grew with table size.
- **Fixes.**
  1. Selective-column-first indexes, `(lower(email), account_id)` etc. The prefetch went from 350 ms to 17 ms (BitmapOr plan).
  2. `SET LOCAL enable_seqscan = off` inside the batch transaction.
- **Result.** The same import on brand-new tables takes **2.4–3.0 s**. Without the benchmark this would have shipped: it bites any new or bulk-loaded table.

## 8. Is it Go, or is it the architecture?

| Evidence | Points to |
|---|---|
| CPU used for all of 150k rows: 5–7 CPU-seconds; parse 0.3 s, validate 0.6 s | The language's raw speed is **irrelevant**: legacy spends minutes waiting on I/O |
| 11.6× from set-based prefetch with **one** writer | architecture (less work) |
| 7.6× from 8 concurrent writers, capped by the database | architecture (overlapping I/O), bounded by downstream capacity |
| GC: ~20–60 cycles, **< 30 ms total pause** per 150k import | Go runtime: a nice property, not a throughput driver here |
| Peak memory 47–55 MB, flat with file size | architecture (streaming + bounded channels); any runtime with async streams + bounded queues gets the same |

**Conclusion.** The improvement comes from **architecture**: set-based prefetch instead of N+1 queries, batched writes and events, streaming, and bounded concurrency sized to the database. It does not come from Go being a faster language.
A rewrite in C#, Java or Rust with the same bounded-queue design and the same SQL would very likely reach similar throughput, because the database is the bottleneck in all of them.

What Go adds is **simplicity and footprint**:
- The whole bounded pipeline (producer, worker pool, router, sharded batchers, cancellation) is ~500 lines using only goroutines, channels, `select` and `errgroup`.
- It builds to a single ~15 MB static binary that runs in ~50 MB of memory.
- pprof and benchmarking are built in.

So the honest recommendation is: **fix the architecture first, in whichever language the team will maintain.** Choose Go if the team wants a small, standalone ingestion service.

## How it's measured
- **Wall time:** `time.Now()` at the start of the run, `time.Since()` at the end.
- **Stage times:** each stage adds `time.Since(t0)` to an atomic counter. They are summed across goroutines.
- **Latency:** every sink call is timed (reservoir of up to 50k samples), then sorted for percentiles.
- **Memory:** `runtime/metrics` `/memory/classes/total:bytes` (≈ RSS) and `/memory/classes/heap/objects:bytes`, sampled every 100 ms; the peak is reported.
- **CPU:** `/cpu/classes/total − idle` from `runtime/metrics`. **GC:** `runtime.ReadMemStats` diff.
- **Repeats:** Postgres writer sweep = median of 3. Everything else is single runs, so expect ±15% noise.
- **Caveats:**
  - The client, the "engine" and the database share one laptop.
  - Local Postgres has ~0.2 ms RTT, an empty-to-150k table, and no replicas or other traffic.
  - The legacy baseline is a structural model. For a production decision, also run the real legacy importer on the same generated files.
- **Reproduce:** `.\bench\bench.ps1`, `.\bench\bench-pg.ps1`, `.\bench\failures.ps1`. Raw JSON reports are written to `bench/results/`.
