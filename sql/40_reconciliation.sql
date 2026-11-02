-- loaded = accepted + rejected + suppressed (duplicates / same-version supersession); also files and per-currency totals.
CREATE OR REPLACE PROCEDURE DB_TRADE_DB.OPS.RECONCILE_BATCH(P_BATCH_ID STRING)
RETURNS STRING LANGUAGE SQL EXECUTE AS OWNER AS
$$
DECLARE
  v_expected_files NUMBER; v_expected_records NUMBER; v_arrived_files NUMBER; v_ingested NUMBER;
  v_accepted NUMBER; v_rejected NUMBER; v_suppressed NUMBER; v_ccy_deltas NUMBER; v_status STRING;
BEGIN
  SELECT EXPECTED_FILES, EXPECTED_RECORDS INTO :v_expected_files, :v_expected_records
  FROM DB_TRADE_DB.OPS.BATCH_CONTROL WHERE BATCH_ID = :P_BATCH_ID;
  SELECT COUNT(DISTINCT SOURCE_FILE), COUNT(*) INTO :v_arrived_files, :v_ingested
  FROM DB_TRADE_DB.RAW.TRADE_EVENTS WHERE BATCH_ID = :P_BATCH_ID;
  SELECT COUNT(*) INTO :v_accepted FROM DB_TRADE_DB.CURATED.TRADE_HISTORY WHERE BATCH_ID = :P_BATCH_ID;
  SELECT COUNT(*) INTO :v_rejected FROM DB_TRADE_DB.STANDARD.V_RAW_EVENTS R
  WHERE R.BATCH_ID = :P_BATCH_ID
    AND R.EVENT_KEY IN (SELECT EVENT_ID FROM DB_TRADE_DB.AUDIT.REJECTED_TRADES WHERE BATCH_ID = :P_BATCH_ID);
  v_suppressed := v_ingested - v_accepted - v_rejected;

  SELECT COUNT(*) INTO :v_ccy_deltas FROM (
    SELECT 1
    FROM (SELECT CURRENCY, EXPECTED_NOTIONAL FROM DB_TRADE_DB.OPS.BATCH_CONTROL_TOTALS WHERE BATCH_ID = :P_BATCH_ID) E
    FULL OUTER JOIN (SELECT UPPER(CURRENCY) AS CURRENCY, SUM(TRY_TO_DECIMAL(NOTIONAL,38,2)) AS ACTUAL
                     FROM DB_TRADE_DB.RAW.TRADE_EVENTS WHERE BATCH_ID = :P_BATCH_ID GROUP BY 1) A
      ON E.CURRENCY = A.CURRENCY
    WHERE EXISTS (SELECT 1 FROM DB_TRADE_DB.OPS.BATCH_CONTROL_TOTALS WHERE BATCH_ID = :P_BATCH_ID)
      AND COALESCE(E.EXPECTED_NOTIONAL,0) <> COALESCE(A.ACTUAL,0)
  );

  v_status := IFF(COALESCE(v_expected_files = v_arrived_files AND v_expected_records = v_ingested
                           AND v_suppressed >= 0 AND v_ccy_deltas = 0, FALSE), 'PASS', 'FAIL');
  INSERT INTO DB_TRADE_DB.OPS.RECON_RESULTS (BATCH_ID,EXPECTED_FILES,ARRIVED_FILES,EXPECTED_RECORDS,INGESTED_RECORDS,ACCEPTED_RECORDS,REJECTED_RECORDS,RECON_STATUS,DETAILS)
  SELECT :P_BATCH_ID, :v_expected_files, :v_arrived_files, :v_expected_records, :v_ingested, :v_accepted, :v_rejected, :v_status,
         OBJECT_CONSTRUCT('file_count_delta', :v_arrived_files - :v_expected_files,
                          'record_count_delta', :v_ingested - :v_expected_records,
                          'suppressed_records', :v_suppressed,
                          'currencies_with_notional_delta', :v_ccy_deltas);
  UPDATE DB_TRADE_DB.OPS.BATCH_CONTROL
     SET STATUS = IFF(:v_status = 'PASS','RECONCILED','RECON_FAILED'), COMPLETED_AT = CURRENT_TIMESTAMP(), UPDATED_AT = CURRENT_TIMESTAMP()
   WHERE BATCH_ID = :P_BATCH_ID;
  RETURN v_status;
END;
$$;

-- Financial control totals by currency (never sum mixed currencies without an approved FX basis).
SELECT BATCH_ID, CURRENCY, COUNT(*) TRADE_COUNT, SUM(NOTIONAL) TOTAL_NOTIONAL
FROM DB_TRADE_DB.CURATED.TRADE_HISTORY GROUP BY BATCH_ID, CURRENCY ORDER BY BATCH_ID, CURRENCY;
