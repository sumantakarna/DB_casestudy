{{ config(materialized='incremental', unique_key='EVENT_ID', incremental_strategy='merge',
          on_schema_change='sync_all_columns') }}

SELECT EVENT_ID, TRADE_ID, VERSION, FINAL_REJECTION_REASON AS REJECTION_REASON,
  CASE FINAL_REJECTION_REASON
    WHEN 'STALE_VERSION' THEN 'Incoming version is lower than current stored version.'
    WHEN 'SUPERSEDED_IN_BATCH' THEN 'A higher version for this trade exists in the same batch.'
    WHEN 'MATURITY_BEFORE_TODAY' THEN 'Maturity date is earlier than processing date.'
    WHEN 'INVALID_QUANTITY' THEN 'Quantity must be greater than zero.'
    WHEN 'INVALID_PRICE' THEN 'Price must be greater than zero.'
    ELSE 'Trade failed a required-field or business validation.'
  END AS REJECTION_DETAILS,
  SOURCE_SYSTEM, BATCH_ID, EVENT_TIMESTAMP, RAW_PAYLOAD,
  CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS REJECTED_AT
FROM {{ ref('int_trade_decisions') }}
WHERE DECISION_STATUS = 'REJECTED'
