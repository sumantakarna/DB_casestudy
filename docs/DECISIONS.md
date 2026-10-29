# Design decisions and limitations

- Internal stage + `COPY INTO` gives a simple native Snowflake bulk-ingestion demonstration. Cloud storage + Snowpipe can be added for event-driven arrival.
- dbt expresses validation and transformation in version-controlled SQL.
- Current state and rejected events are separate tables. Expired trades remain visible with status `EXPIRED`.
- A batch ID limits the events handled by each dbt invocation. The Airflow DAG serializes runs.
- Same-version replacement is implemented because the case study explicitly requests it. A real trading platform may instead quarantine conflicting same-version payloads.
- `CURRENT_DATE()` follows Snowflake session date/time settings; define a business timezone and market calendar for production.
- dbt's multi-model build is not one atomic transaction. For strict audit/current-state atomicity, use a transactionally controlled stored procedure or stage decisions then publish only after all controls pass.
- Password auth is used for demo simplicity; production should use a managed secrets store and approved key-pair/OAuth authentication.
- Validate Terraform provider resources against the pinned provider release and use least-privilege roles before applying.
