-- Illustrative Snowflake Alert. Requires a notification integration and appropriate grants.
CREATE OR REPLACE ALERT TRADE_DB.MONITORING.FAILED_PIPELINE_ALERT
  WAREHOUSE = TRADE_PIPELINE_WH
  SCHEDULE = '5 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM TRADE_DB.MONITORING.PIPELINE_RUNS
    WHERE STATUS = 'FAILED'
      AND STARTED_AT >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
  ))
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'TRADE_PIPELINE_EMAIL_INTEGRATION',
      'data-platform@example.com',
      'Trade pipeline failure detected',
      'A trade pipeline run failed. Review Airflow logs and Snowflake query history.'
    );

-- Resume only after setting up and testing the notification integration:
-- ALTER ALERT TRADE_DB.MONITORING.FAILED_PIPELINE_ALERT RESUME;
