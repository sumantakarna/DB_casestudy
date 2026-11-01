-- Reconcile batch controls against raw, DQ, rejects and curated outcomes.
CREATE OR REPLACE PROCEDURE DB_TRADE_DB.OPS.RECONCILE_BATCH(P_BATCH_ID STRING)
RETURNS STRING LANGUAGE SQL EXECUTE AS OWNER AS
$$
DECLARE
  v_expected_files NUMBER; v_arrived_files NUMBER; v_expected_records NUMBER;
  v_ingested NUMBER; v_rejected NUMBER; v_accepted NUMBER; v_status STRING;
BEGIN
  SELECT EXPECTED_FILES,EXPECTED_RECORDS INTO :v_expected_files,:v_expected_records
  FROM DB_TRADE_DB.OPS.BATCH_CONTROL WHERE BATCH_ID=:P_BATCH_ID;
  SELECT COUNT(DISTINCT SOURCE_FILE),COUNT(*) INTO :v_arrived_files,:v_ingested
  FROM DB_TRADE_DB.RAW.TRADE_EVENTS WHERE BATCH_ID=:P_BATCH_ID;
  SELECT COUNT(DISTINCT EVENT_ID) INTO :v_rejected FROM DB_TRADE_DB.AUDIT.REJECTED_TRADES WHERE BATCH_ID=:P_BATCH_ID;
  v_accepted := GREATEST(v_ingested-v_rejected,0);
  v_status := IFF(v_expected_files=v_arrived_files AND v_expected_records=v_ingested,'PASS','FAIL');
  INSERT INTO DB_TRADE_DB.OPS.RECON_RESULTS(BATCH_ID,EXPECTED_FILES,ARRIVED_FILES,EXPECTED_RECORDS,INGESTED_RECORDS,ACCEPTED_RECORDS,REJECTED_RECORDS,RECON_STATUS,DETAILS)
  VALUES(:P_BATCH_ID,:v_expected_files,:v_arrived_files,:v_expected_records,:v_ingested,:v_accepted,:v_rejected,:v_status,
    OBJECT_CONSTRUCT('file_count_delta',v_arrived_files-v_expected_files,'record_count_delta',v_ingested-v_expected_records));
  UPDATE DB_TRADE_DB.OPS.BATCH_CONTROL SET STATUS=IFF(:v_status='PASS','RECONCILED','RECON_FAILED'),COMPLETED_AT=CURRENT_TIMESTAMP(),UPDATED_AT=CURRENT_TIMESTAMP() WHERE BATCH_ID=:P_BATCH_ID;
  RETURN v_status;
END;
$$;

-- Financial control total example. Compare using an agreed currency/FX basis; never sum mixed currencies.
SELECT BATCH_ID,CURRENCY,COUNT(*) TRADE_COUNT,SUM(NOTIONAL) TOTAL_NOTIONAL
FROM DB_TRADE_DB.CURATED.TRADE_HISTORY
GROUP BY BATCH_ID,CURRENCY ORDER BY BATCH_ID,CURRENCY;
