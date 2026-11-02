{{ config(materialized='view') }}
-- Typed/standardised view of RAW. Bad values become NULL (TRY_*); rules live in int_trade_dq.
SELECT EVENT_ID, SOURCE_SYSTEM, SOURCE_TYPE, BATCH_ID, TRADE_ID,
       VERSION, UPPER(EVENT_TYPE) AS EVENT_TYPE,
       TRY_TO_DATE(TRADE_DATE) AS TRADE_DATE, TRY_TO_DATE(MATURITY_DATE) AS MATURITY_DATE,
       COUNTERPARTY, UPPER(INSTRUMENT) AS INSTRUMENT, UPPER(CURRENCY) AS CURRENCY,
       TRY_TO_DECIMAL(NOTIONAL,38,2) AS NOTIONAL, TRY_TO_DECIMAL(PRICE,38,8) AS PRICE,
       UPPER(SIDE) AS SIDE, SOURCE_SEQUENCE, INGESTED_AT, SOURCE_FILE, FILE_ROW_NUMBER,
       PAYLOAD, RECORD_HASH, SCHEMA_VERSION
FROM {{ source('raw','trade_events') }}
{% if var('batch_id', none) %}WHERE BATCH_ID = '{{ var("batch_id") | replace("'", "''") }}'{% endif %}
