# Operations, data quality and scale guide

## File completeness and delayed arrivals

- Producer writes data objects to a unique immutable path: `source/yyyy/mm/dd/batch_id/part-xxxxx.ext`.
- Producer uploads a manifest only after all objects are committed. Manifest includes expected file count, rows per file, bytes, checksum, schema version, source watermark and control totals.
- Register manifest in `OPS.BATCH_CONTROL` and `OPS.FILE_MANIFEST`; batch remains `WAITING` until expected files are visible and Snowpipe load history confirms successful ingestion.
- Use a grace period / source-specific SLA. If the deadline expires, set `LATE`, alert and keep the batch out of curated publication. Late files can be replayed idempotently.
- Do not infer completeness from a quiet period or one successful file. Object-store listing is useful for repair, but manifest is authoritative.
- Check SHA-256 at producer/adapter or trusted cloud service. Snowflake does not automatically validate an arbitrary manifest checksum against object contents.

## DQ performance pattern

- Persist raw once; standardize once; evaluate multiple rules in one pass using expressions/arrays or a metadata-driven SQL generator.
- Use `TRY_TO_DATE`, `TRY_TO_DECIMAL`, `TRY_TO_NUMBER`, `REGEXP_LIKE`, `COUNT_IF`, `QUALIFY ROW_NUMBER`.
- Store rule outcomes in `AUDIT.DQ_RESULTS` with batch/event/rule/severity; route bad records to quarantine with original payload and reason.
- Run cheap validations first: null/shape/domain; expensive regex/reference checks only on candidates that pass cheap checks.
- Use incremental batch predicates and micro-partition-friendly filters on `BATCH_ID`, ingestion date or source partition. Avoid applying functions to filter columns.
- For dedup, partition by stable event ID or source event key. For trade key/version use a deterministic ordering: source sequence, event timestamp, producer priority, event ID.
- Snowpark is appropriate for complex XML/fixed-width parsing, reference enrichment or custom algorithms; use vectorized/batched processing, not per-row Python UDFs for simple predicates.

## Reconciliation controls

At minimum reconcile: (1) expected vs arrived files, (2) expected vs loaded rows, (3) loaded = accepted + rejected + explicitly duplicate/suppressed, (4) source and target counts by business date/currency/instrument, (5) notional totals by currency, (6) source sequence/watermark gaps, (7) rejected reason distribution, (8) late/partial batch SLA. Keep currencies separate unless an approved FX conversion is applied.

## Failure handling

- Retry transient network/warehouse errors with bounded exponential backoff; do not retry deterministic schema/DQ failures indefinitely.
- Use idempotent event IDs, immutable file names, batch state machine and procedure transactions.
- Quarantine poison files and records; preserve raw bytes/payload and error diagnostics.
- On corrected file, publish a new immutable object key / new file version and link it to original manifest. Avoid overwriting an object under the same path because Snowpipe file-load metadata can suppress reprocessing.
- Re-drive only failed batch partitions after checking the file ledger and event deduplication state.
- Airflow `max_active_runs=1` is a safe demo guard. At scale use keyed partition ownership or one apply worker per hash partition to avoid overlapping trade-key MERGEs.

## Snowflake monitoring

- `INFORMATION_SCHEMA.COPY_HISTORY` for recent file loads; `SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY` / `LOAD_HISTORY` for historical analysis (with latency/retention caveats).
- `ACCOUNT_USAGE.QUERY_HISTORY`, `WAREHOUSE_METERING_HISTORY`, `TASK_HISTORY`, `PIPE_USAGE_HISTORY`, `WAREHOUSE_LOAD_HISTORY`, `AUTOMATIC_CLUSTERING_HISTORY` for query, compute, pipe, task and clustering performance.
- `SYSTEM$PIPE_STATUS('<db>.<schema>.<pipe>')` for pipe health; alert on stale/paused pipes and queue backlog.
- Snowflake Event Tables + OpenTelemetry for procedure/task/application logs and traces. Correlate with `BATCH_ID`, `EVENT_ID`, `QUERY_ID`.
- Snowflake Data Metric Functions (DMFs), Expectations and anomaly detection can monitor nulls, duplicates, freshness and volume; availability/notifications depend on edition and configuration.
- Use Snowflake ALERT objects / notification integrations for native threshold alerts; use external integrations for PagerDuty/Slack/Teams/ServiceNow. Alerts should include batch, source, SLA, error count and runbook link.

## 10,000x scaling plan

1. Partition producer output into bounded, compressed files; avoid thousands of tiny files. Benchmark file sizes and ingestion latency; common starting range is tens to hundreds of MB compressed, adjusted to payload and concurrency.
2. Separate streams/paths/pipes by source and priority to avoid noisy-neighbour effects; filter cloud event notifications at source.
3. Use Snowpipe for file arrivals; Snowpipe Streaming HP for continuous high-throughput Kafka events. Select by latency and volume, not fashion.
4. Keep raw immutable and append-only. Transform in micro-batches; use Streams/Tasks or dbt incremental models. Do not full-refresh raw/large history each run.
5. Separate ingestion, transformation and BI warehouses. Autosuspend, auto-resume, resource monitors and workload isolation.
6. Scale throughput horizontally with file parallelism / Kafka partitions / Snowpipe; scale concurrency with multi-cluster warehouses. Bigger warehouse is not automatically faster for a single query.
7. Cluster only when query profile shows poor pruning and recurring selective predicates justify maintenance credits. Consider Search Optimization only for highly selective point lookups.
8. Use retention tiers, archival to object storage, Time Travel appropriate to recovery objectives, and retention/legal hold controls.
9. Run backfills in isolated warehouses and controlled windows. Track bytes scanned, spill, queue time, credits per million trades and end-to-end latency.
10. Load test with realistic skew, hot trade IDs, late events, duplicates, schema drift and simultaneous consumers.

## Suggested operational SLOs

| Metric | Example target (business to approve) |
|---|---:|
| File arrival to raw availability | p95 < 5 minutes |
| Streaming event availability | p95 < 10 seconds where HP streaming is used |
| Manifest-to-curated completion | p95 < 15 minutes |
| Reconciliation | 100% accounted rows; zero unexplained financial delta |
| Duplicate event application | 0 |
| Critical DQ / pipeline failure alert | < 5 minutes |
| Batch completeness | 100% expected files or explicit exception approval |

These are example objectives, not Snowflake guarantees.
