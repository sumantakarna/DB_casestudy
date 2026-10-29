"""Airflow DAG: generate -> Snowflake load -> dbt build, with retries and email alert."""
import os
import subprocess
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

PROJECT_DIR = Path(os.getenv("TRADE_PROJECT_DIR", "/opt/airflow/dws_trade_pipeline"))
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "data-platform@example.com")


def generate(**context):
    batch = f"airflow-{context['ds_nodash']}-{uuid.uuid4().hex[:8]}"
    output = PROJECT_DIR / "data" / f"{batch}.jsonl"
    output.parent.mkdir(parents=True, exist_ok=True)
    count = int(Variable.get("TRADE_BATCH_COUNT", default_var="100"))
    subprocess.run([sys.executable, str(PROJECT_DIR/"scripts/generate_trades.py"),
                    "--output", str(output), "--count", str(count), "--batch-id", batch], check=True)
    context["ti"].xcom_push(key="batch_id", value=batch)
    context["ti"].xcom_push(key="file_path", value=str(output))


def load(**context):
    path = context["ti"].xcom_pull(task_ids="generate_trades", key="file_path")
    subprocess.run([sys.executable, str(PROJECT_DIR/"scripts/load_to_snowflake.py"),
                    "--file", path], cwd=str(PROJECT_DIR), check=True)


def run_dbt(**context):
    batch = context["ti"].xcom_pull(task_ids="generate_trades", key="batch_id")
    profiles = os.getenv("DBT_PROFILES_DIR", str(PROJECT_DIR/"dbt_project"))
    subprocess.run(["dbt", "build", "--project-dir", str(PROJECT_DIR/"dbt_project"),
                    "--profiles-dir", profiles, "--target", os.getenv("DBT_TARGET", "dev"),
                    "--vars", f'{{"batch_id":"{batch}"}}'], cwd=str(PROJECT_DIR), check=True)


def failure_email(context):
    send_email(to=ALERT_EMAIL,
               subject=f"[AIRFLOW FAILURE] {context['dag'].dag_id}.{context['task_instance'].task_id}",
               html_content=f"Run: {context.get('run_id')}<br>Error: {context.get('exception')}")


default_args = {"owner": "data-platform", "depends_on_past": False, "retries": 2,
                "retry_delay": timedelta(minutes=3), "email_on_failure": False,
                "on_failure_callback": failure_email}

with DAG(dag_id="dws_trade_processing", start_date=datetime(2025, 1, 1),
         schedule="0 * * * *", catchup=False, max_active_runs=1,
         default_args=default_args, tags=["snowflake", "dbt", "trades"]) as dag:
    generate_task = PythonOperator(task_id="generate_trades", python_callable=generate)
    load_task = PythonOperator(task_id="load_to_snowflake", python_callable=load)
    dbt_task = PythonOperator(task_id="dbt_build", python_callable=run_dbt)
    generate_task >> load_task >> dbt_task
