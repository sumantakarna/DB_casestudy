"""Cross-system coordinator. Snowpipe ingests continuously; DAG gates dbt/apply/recon on manifest readiness."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os, subprocess, json

DBT_DIR='/opt/airflow/dbt_project'

def check_ready(**context):
    batch_id=context['dag_run'].conf.get('batch_id')
    if not batch_id: raise ValueError('Supply dag_run.conf.batch_id')
    hook=SnowflakeHook(snowflake_conn_id='snowflake_trade')
    row=hook.get_first("""
      SELECT B.STATUS, B.MANIFEST_RECEIVED, B.EXPECTED_FILES, B.EXPECTED_RECORDS,
             COUNT(DISTINCT R.SOURCE_FILE) AS ARRIVED_FILES, COUNT(R.EVENT_ID) AS LOADED_ROWS
      FROM DB_TRADE_DB.OPS.BATCH_CONTROL B
      LEFT JOIN DB_TRADE_DB.RAW.TRADE_EVENTS R ON R.BATCH_ID=B.BATCH_ID
      WHERE B.BATCH_ID=%s
      GROUP BY B.STATUS,B.MANIFEST_RECEIVED,B.EXPECTED_FILES,B.EXPECTED_RECORDS
    """,(batch_id,))
    if not row or not row[1]: raise ValueError(f'Manifest not registered for {batch_id}')
    if row[4] < row[2] or row[5] < row[3]:
        raise ValueError(f'Files/rows incomplete for {batch_id}: expected files={row[2]}, arrived={row[4]}, expected rows={row[3]}, loaded rows={row[5]}')
    context['ti'].xcom_push(key='batch_id',value=batch_id)

def run_sql(sql, **context):
    batch_id=context['ti'].xcom_pull(key='batch_id')
    SnowflakeHook(snowflake_conn_id='snowflake_trade').run(sql,parameters=(batch_id,))

with DAG(dag_id='deutsche_bank_trade_batch',start_date=datetime(2026,1,1),schedule=None,catchup=False,
         max_active_runs=1,default_args={'retries':3,'retry_delay':timedelta(minutes=5)}) as dag:
    ready=PythonOperator(task_id='check_manifest_and_batch_ready',python_callable=check_ready)
    def invoke_dbt(**context):
        batch=context['ti'].xcom_pull(key='batch_id')
        subprocess.run(['dbt','build','--project-dir',DBT_DIR,'--profiles-dir',DBT_DIR,'--vars',json.dumps({'batch_id':batch})],check=True,env=os.environ.copy())
    dbt=PythonOperator(task_id='dbt_build',python_callable=invoke_dbt)
    classify=PythonOperator(task_id='classify_batch',python_callable=run_sql,op_kwargs={'sql':'CALL DB_TRADE_DB.STANDARD.CLASSIFY_BATCH(%s)'})
    apply=PythonOperator(task_id='apply_trade_versions',python_callable=run_sql,op_kwargs={'sql':'CALL DB_TRADE_DB.CURATED.APPLY_BATCH(%s)'})
    recon=PythonOperator(task_id='reconcile_batch',python_callable=run_sql,op_kwargs={'sql':'CALL DB_TRADE_DB.OPS.RECONCILE_BATCH(%s)'})
    ready >> dbt >> classify >> apply >> recon
