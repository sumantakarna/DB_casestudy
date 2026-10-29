# Operations and scale

## Late files
Use a file manifest, source batch ID, checksum and row count. In Airflow, use a sensor or event trigger with a timeout. Alert on missed SLA and do not publish incomplete fund/date partitions. Keep a durable batch ledger and process each source batch idempotently.

## Data quality
Land raw payloads before business transformations. Parse with TRY_TO_* where appropriate, then reject missing or malformed required fields with explicit reason codes. Use dbt generic/custom tests for not-null, unique, accepted values, referential integrity, freshness and financial reconciliations. Critical control failures should block downstream publication.

## Task failures
Airflow retries transient failures with bounded attempts/backoff and sends an email through its configured email backend. Make load and transformation steps idempotent. Persist batch/run state, row counts and error details. Serialize current-state version processing or use a transactional procedure to avoid concurrent version races.

## Snowflake monitoring
Useful views include:
- `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`: runtime, status, bytes scanned and errors.
- `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`: warehouse credits.
- `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY`: queueing and warehouse load.
- `SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY`: load history/errors, subject to view coverage and latency.
- `INFORMATION_SCHEMA.TASK_HISTORY()` and pipe history when using native Tasks/Snowpipe.
- dbt `manifest.json`, `run_results.json`, and logs.

Query history example:

```sql
SELECT QUERY_ID, WAREHOUSE_NAME, EXECUTION_STATUS,
       TOTAL_ELAPSED_TIME/1000 AS ELAPSED_SECONDS, BYTES_SCANNED,
       ERROR_MESSAGE, START_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
  AND (QUERY_TEXT ILIKE '%TRADE_EVENTS%' OR QUERY_TEXT ILIKE '%CURRENT_TRADES%')
ORDER BY START_TIME DESC LIMIT 200;
```

Warehouse spend example:

```sql
SELECT WAREHOUSE_NAME, SUM(CREDITS_USED) AS CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1 ORDER BY 2 DESC;
```

## Alerts
Airflow's failure callback sends an email through the configured Airflow email backend. Snowflake Alerts can evaluate failed-run rows on a schedule and call a notification integration; the exact setup depends on account edition, privileges and notification integration. See `ALERTS.sql` for a starter example. Test delivery and do not email sensitive trade payloads.

## 10,000x scaling strategy
1. Avoid tiny files; batch into compressed files and use parallel `COPY INTO`.
2. Consider cloud-stage Snowpipe auto-ingest for file events or Snowpipe Streaming for suitable low-latency use cases.
3. Partition by independent source/date domains and preserve per-trade ordering.
4. Use incremental dbt models and bounded change windows; avoid rescanning all history.
5. Separate ingestion, transformation and BI workloads into different warehouses.
6. Size compute based on load tests and monitor credits per million trades, throughput, and end-to-end latency.
7. Add queue-depth/freshness SLAs, backpressure, file manifests, and controlled backfills.
8. Keep key-level correctness and reconciliation checks efficient.
9. Use clustering only when query profiles justify it.
10. Revisit retention, access controls, masking, and audit policy as data volume grows.

A 10,000x volume increase needs measured capacity planning, not a 10,000x warehouse-size increase.
