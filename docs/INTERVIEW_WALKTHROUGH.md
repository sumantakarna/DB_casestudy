# 5-minute interview walkthrough

1. Python simulates trade events as JSONL.
2. A Snowflake connector uploads the file to an internal stage; `COPY INTO` loads raw events.
3. dbt staging normalizes and deduplicates events.
4. The decision model validates required fields, maturity, quantity/price and version ordering.
5. Accepted events merge into current state; rejected events enter a separate audit table with reason and payload.
6. Expired current trades are retained with an `EXPIRED` status.
7. Airflow schedules and retries the flow; failure callback sends email.
8. Snowflake account usage views, dbt artifacts and Airflow logs support monitoring; Streamlit shows status and rejection categories.
9. Scale with parallel staged loads/Snowpipe, incremental transformations, workload separation and serialized per-trade version handling.
10. Production hardening includes least privilege, secrets management, business-approved contracts, audit retention and transactional publication.
