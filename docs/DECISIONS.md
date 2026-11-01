# Architecture decision record

## Why not use Snowpark for every DQ check?
SQL predicates execute close to storage, are optimizer-visible, and avoid Python serialization/runtime overhead. Snowpark is used when parsing or algorithms are materially easier in Python. Benchmark both on representative data.

## Why dbt plus Snowflake Tasks/Airflow?
dbt provides model modularity, tests, docs and lineage. Tasks/Streams coordinate native Snowflake work. Airflow handles external dependencies, source readiness and cross-platform recovery. Avoid orchestrating every SQL statement externally.

## Why a manifest?
A file landing event proves only that one object arrived. It does not prove all expected parts arrived or that source control totals reconcile. Manifest is the batch commit contract.

## Trade version semantics
Lower version than current is rejected. Equal version replaces according to deterministic latest-event-wins. Higher version supersedes. Maturity earlier than processing date is rejected at ingestion; maturity date reached/passed is a lifecycle status. Cancellation is a business event, not deletion.
