# 5-minute interview walkthrough

1. A generator simulates one batch across JSONL, CSV and XML files plus a manifest (counts, bytes, SHA-256, per-currency control totals) and a `_SUCCESS` marker written last.
2. The loader verifies every file against the manifest (incomplete, truncated or non-UTF-8 files stop here), PUTs to a stage and COPYs with a per-format column mapping into one RAW table. The manifest is registered only if loaded rows match: it is the commit signal.
3. Kafka/CDC sources land in the same RAW table through the Kafka connector, so downstream steps are identical for every source type.
4. `CLASSIFY_BATCH` runs all rules in a single set-based pass (missing values, wrong types, domain values, control/unreadable characters, maturity in the past, duplicates) and writes results to `AUDIT.DQ_RESULTS`; failures go to `AUDIT.REJECTED_TRADES` with reason and original payload.
5. `APPLY_BATCH` (transactional, refuses unclassified batches) keeps history, rejects stale versions, replaces same-version events deterministically and merges current state; expired trades are retained with status EXPIRED.
6. `RECONCILE_BATCH` checks files, rows, loaded = accepted + rejected + suppressed, and notional per currency.
7. Airflow waits for the manifest (sensor with timeout -> LATE), then classify -> apply -> dbt -> reconcile and fails loudly on a reconciliation FAIL; Streamlit shows status, rejections and reconciliation.
8. Scale with parallel files/Snowpipe, workload-separated warehouses, incremental models and per-trade-key serialization. Harden with least privilege (`60_grants.sql`), key-pair auth, secrets management and audit retention.
