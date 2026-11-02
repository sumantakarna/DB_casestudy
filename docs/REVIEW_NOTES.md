# Review notes: what was wrong and what changed

The repo mixed an old design (`TRADE_DB`, `QUANTITY`, `TRADE_SIDE`) with the new multi-source one (`DB_TRADE_DB`, `NOTIONAL`, `SIDE`). The new design is now the only one.

| Area | Problem | Fix |
|---|---|---|
| Makefile / old DAG | Called the generator with `--output/--count`, which it rejects | Aligned to `--out/--rows/--format`; guarded by a test |
| `load_to_snowflake.py` | JSONL-only, legacy tables/columns, password auth, `ON_ERROR=CONTINUE` | Multi-format loader with manifest verification, key-pair auth, commit-on-match |
| Generator | Same rows written to all formats (3x duplicates), random event IDs, `created_utc` was a date | Rows split across formats, deterministic IDs, real UTC time, control totals, optional error injection |
| dbt | Two `source('raw')` definitions (one pointing at `TRADE_DB`); models referencing missing columns; unfiltered merge with duplicate keys; `STANDARD_STANDARD` schema; `dbt-utils` pip package that does not exist | Legacy models/sources removed, one source, deduped incremental DQ model, no custom schema, tests warn on source data |
| `CLASSIFY_BATCH` | NULL side/currency never flagged; all copies of a duplicate rejected; NULL `EVENT_ID` rows skipped every check and were applied; `[[:cntrl:]]` pattern anchored with `.*` missed rows with newlines; `GROUP BY` on VARIANT | Null-safe rules, one copy of a duplicate kept, `EVENT_KEY` for every row, `REGEXP_COUNT`, U+FFFD check, reasons aggregated in a CTE |
| `APPLY_BATCH` | Could apply unclassified batches; NULL source sequence sorted first | Classification guard, `NULLS LAST`, batch status update |
| `RECONCILE_BATCH` | `OBJECT_CONSTRUCT` inside `VALUES` is invalid; accepted = ingested - rejected | `INSERT ... SELECT`, real accepted/rejected/suppressed counts, per-currency control totals |
| `REGISTER_MANIFEST` | `:var:path` access on a VARIANT parameter; no control totals | `GET(...)`, control totals table |
| Bootstrap | `CREATE OR REPLACE STREAM` reset the stream on every run; `FF_XML` loaded a whole file as one row; roles in `.env`/Kafka config never created | `IF NOT EXISTS`, `STRIP_OUTER_ELEMENT=TRUE`, `60_grants.sql` |
| Airflow | Retries used as a wait loop, `xcom_pull` without task id, dbt in the Airflow env | Sensor with timeout and LATE marking, conf-based batch id, `DBT_BIN`, recon failure fails the DAG |
| Streamlit | Wrong database and column names, password auth | Current names, key-pair, reconciliation panel |
| Terraform | Invalid HCL (`;` in blocks), duplicate provider block, undefined variables | Rewritten and consistent |
| CI | `dbt parse ... \|\| true` hid failures; no profile | Dummy CI profile, no `\|\| true`, terraform validate |
| Docs | README layout/quick start/diagram out of date, `ALERTS.sql` on dropped schema | Updated; `ALERTS.sql` removed (see `sql/50`) |

## Not verified here
Snowflake SQL, Airflow, dbt and Terraform could not be executed in this environment (no network, no Snowflake account). Python code is covered by tests; run the SQL in a dev account and let CI check dbt/Terraform before relying on it.
