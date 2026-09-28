# Production metric predictions

These are **predictions**, extrapolated from the measured local results in BENCHMARK.md with the assumptions stated below. Treat them as ranges to validate, not promises.

## Assumptions (production vs laptop)
| Factor | Laptop (measured) | Production (assumed) | Effect |
|---|---|---|---|
| App → Postgres RTT | ~0.2 ms | 0.5–2 ms (in-cluster) | hurts per-row designs a lot (×RTT per query), batch designs barely (5 statements per batch) |
| Table size | 0 → 150k rows | millions of rows, more indexes, colder cache | 1.5–3× slower per batch |
| Work per prospect | 1 table + events | also emails, phones, custom fields, tags, workflow triggers | 2–4× more statements per batch (all can be set-based) |
| DB shared with live traffic | no | yes | cap writers at ~8; expect 50–70% scaling efficiency |
| Measured batch cost (1 writer) | 20.0 s / 300 batches ≈ **67 ms per 500 leads** | 67 ms × 3–10 ≈ **0.2–0.7 s per batch** | |

## Predicted: 150,000 leads per import

| Metric | Legacy per-row importer (predicted) | Go pipeline, 8 writers (predicted) |
|---|---|---|
| DB round-trips | ~8–11 per row ⇒ **1.2–1.7 M** (+150k event inserts) | ~5–15 per 500-row batch ⇒ **~1.5–4.5 k** |
| Duration | **~40–90 min** (15–35 ms/row) | **~15–60 s** (central estimate ~30 s) |
| Throughput | ~30–65 rows/s | **~2,500–10,000 rows/s** |
| Peak memory | 0.5–1.5 GB (whole file as a string + per-field objects for every row) | **< 100 MB** (measured 55 MB locally; queues are bounded) |
| CPU | mostly idle, waiting on I/O | ~7 CPU-s per import ⇒ ~0.25 core on average |
| GC | many large-heap collections | < 50 ms total pause per import |
| Duplicate risk under concurrency | n/a (sequential) | 0 for the primary key (hash sharding + transaction per batch) |

**Speed-up predicted: ~50–150×.**
Nearly all of it comes from replacing per-row queries with set-based batches, and from bounded concurrent writers. The same change in any language would get most of it.

## Recommended production settings
| Setting | Value | Why |
|---|---|---|
| `-workers` | 2 | CPU work is ~1 µs/row; more workers measured no gain |
| `-shards` (writers) | **8**, never above DB vCPU/2 | the measured knee; beyond it p99 rises 2–4× and throughput falls |
| `-batch` | 500 | measured plateau at 250–1000; 2000 doubles p99 |
| `-queue` | 1000 | bounds memory; backpressure engages when the DB slows |
| `-retries` / `-max-consecutive-failures` | 5 / 5 | rides out blips, stops fast when the DB is really down |
| Pod resources | request 250m CPU / 128 Mi; limit 1 CPU / 256 Mi | measured 55 MB peak, ~7 CPU-s per 150k |

## What to watch (Prometheus → Grafana)
- `ingest_sink_write_seconds` p99 **rising while throughput stays flat** means the DB is saturated: reduce writers.
- `ingest_retries_total{reason="http_429"}` or `transient` climbing means you're pushing past the downstream limit.
- `ingest_queue_depth` pinned at its maximum is normal (backpressure). If it's pinned at 0, the upload is the bottleneck.
- `ingest_rows_total{outcome=...}` gives the import result mix; `ingest_import_duration_seconds` tracks the SLO.

## Risks the numbers don't capture
1. **Downstream burst.** 150k prospect-created events land in ~30 s instead of over an hour. Downstream consumers must apply their own backpressure, and if events are partitioned by account, one partition gets the whole burst.
2. **Upload limits.** A 150k-row CSV is ~22 MB, so upload size/row limits must accommodate it, or the file should go straight to object storage and be streamed from there.
3. **Secondary-identifier matches** (phone/LinkedIn) across shards are not serialized (see ARCHITECTURE.md). The rate is small, and it's the same behaviour as today.
4. **Legacy timing is modelled.** Validate the legacy column by measuring real import durations (start to finish) on 50k–150k imports, and by running the legacy importer on the same generated files.
