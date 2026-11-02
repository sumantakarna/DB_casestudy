"""Demo DAG: generate a multi-format sample batch -> verify + load to Snowflake -> trigger the batch DAG."""
import os
import subprocess
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PROJECT_DIR = Path(os.getenv("TRADE_PROJECT_DIR", Path(__file__).resolve().parents[2]))
OUT_DIR = PROJECT_DIR / "sample_data"


def generate(**context):
    batch_id = f"DEMO-{context['ds_nodash']}-{uuid.uuid4().hex[:6].upper()}"
    rows = os.getenv("TRADE_BATCH_ROWS", "1000")
    subprocess.run([sys.executable, str(PROJECT_DIR / "scripts/generate_trades.py"), "--out", str(OUT_DIR),
                    "--rows", rows, "--format", "all", "--batch-id", batch_id, "--inject-errors"], check=True)
    return batch_id


def load(**context):
    batch_id = context["ti"].xcom_pull(task_ids="generate_sample_batch")
    subprocess.run([sys.executable, str(PROJECT_DIR / "scripts/load_to_snowflake.py"), "--dir", str(OUT_DIR),
                    "--batch-id", batch_id], cwd=str(PROJECT_DIR), check=True)


with DAG(dag_id="trade_demo_end_to_end", start_date=datetime(2026, 1, 1), schedule=None, catchup=False,
         max_active_runs=1, tags=["demo"], default_args={"retries": 1, "retry_delay": timedelta(minutes=2)}) as dag:
    gen = PythonOperator(task_id="generate_sample_batch", python_callable=generate)
    ld = PythonOperator(task_id="load_batch", python_callable=load)
    trigger = TriggerDagRunOperator(task_id="trigger_batch_processing", trigger_dag_id="deutsche_bank_trade_batch",
                                    conf={"batch_id": "{{ ti.xcom_pull(task_ids='generate_sample_batch') }}"},
                                    wait_for_completion=True, poke_interval=30)
    gen >> ld >> trigger
