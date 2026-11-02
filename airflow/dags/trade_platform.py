"""Batch processing DAG (trigger with conf {"batch_id": "..."}).

Works the same for every source type: files (Snowpipe/loader), Kafka connector or CDC all land in
RAW.TRADE_EVENTS and are committed by a registered manifest. The DAG waits for completeness, then
classify -> apply -> dbt -> reconcile. Curated state is produced by the SQL procedures; dbt adds
standardised models and tests on the same rules.
"""
import json
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sensors.python import PythonSensor

CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_trade")
PROJECT_DIR = Path(os.getenv("TRADE_PROJECT_DIR", Path(__file__).resolve().parents[2]))
DBT_DIR = os.getenv("DBT_PROJECT_DIR", str(PROJECT_DIR / "dbt_project"))
DBT_BIN = os.getenv("DBT_BIN", "dbt")  # run dbt from its own venv/image to avoid dependency clashes
WAIT_TIMEOUT_MIN = int(os.getenv("BATCH_READY_TIMEOUT_MIN", "120"))


def _hook():
    return SnowflakeHook(snowflake_conn_id=CONN_ID)


def _batch_id(context):
    batch_id = (context["dag_run"].conf or {}).get("batch_id")
    if not batch_id:
        raise AirflowFailException("Trigger this DAG with conf {'batch_id': '<id>'}")
    return batch_id


def batch_ready(**context):
    """True once the manifest is registered and arrived files/rows match it."""
    row = _hook().get_first(
        """SELECT B.MANIFEST_RECEIVED, B.EXPECTED_FILES, B.EXPECTED_RECORDS,
                  COUNT(DISTINCT R.SOURCE_FILE), COUNT(R.EVENT_ID)
           FROM DB_TRADE_DB.OPS.BATCH_CONTROL B
           LEFT JOIN DB_TRADE_DB.RAW.TRADE_EVENTS R ON R.BATCH_ID = B.BATCH_ID
           WHERE B.BATCH_ID = %s
           GROUP BY B.MANIFEST_RECEIVED, B.EXPECTED_FILES, B.EXPECTED_RECORDS""",
        parameters=(_batch_id(context),))
    if not row or not row[0]:
        return False
    _, exp_files, exp_rows, files, rows = row
    return (exp_files or 0) <= files and (exp_rows or 0) <= rows


def mark_late(context):
    batch_id = (context["dag_run"].conf or {}).get("batch_id")
    if batch_id:
        _hook().run("UPDATE DB_TRADE_DB.OPS.BATCH_CONTROL SET STATUS='LATE', UPDATED_AT=CURRENT_TIMESTAMP() WHERE BATCH_ID=%s",
                    parameters=(batch_id,))


def call_proc(sql, **context):
    _hook().run(sql, parameters=(_batch_id(context),))


def run_dbt(**context):
    subprocess.run([DBT_BIN, "build", "--project-dir", DBT_DIR,
                    "--profiles-dir", os.getenv("DBT_PROFILES_DIR", DBT_DIR),
                    "--vars", json.dumps({"batch_id": _batch_id(context)})], check=True)


def reconcile(**context):
    row = _hook().get_first("CALL DB_TRADE_DB.OPS.RECONCILE_BATCH(%s)", parameters=(_batch_id(context),))
    if not row or row[0] != "PASS":
        raise AirflowFailException(f"Reconciliation failed for batch: {row}")


with DAG(dag_id="deutsche_bank_trade_batch", start_date=datetime(2026, 1, 1), schedule=None, catchup=False,
         max_active_runs=1, tags=["snowflake", "trades"],
         default_args={"retries": 2, "retry_delay": timedelta(minutes=5)}) as dag:
    wait = PythonSensor(task_id="wait_for_manifest_and_files", python_callable=batch_ready, mode="reschedule",
                        poke_interval=60, timeout=WAIT_TIMEOUT_MIN * 60, retries=0, on_failure_callback=mark_late)
    classify = PythonOperator(task_id="classify_batch", python_callable=call_proc,
                              op_kwargs={"sql": "CALL DB_TRADE_DB.STANDARD.CLASSIFY_BATCH(%s)"})
    apply_ = PythonOperator(task_id="apply_trade_versions", python_callable=call_proc,
                            op_kwargs={"sql": "CALL DB_TRADE_DB.CURATED.APPLY_BATCH(%s)"})
    dbt = PythonOperator(task_id="dbt_build", python_callable=run_dbt)
    recon = PythonOperator(task_id="reconcile_batch", python_callable=reconcile)
    wait >> classify >> apply_ >> dbt >> recon
