import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).parent.absolute()
if PROJECT_ROOT.name == 'dags':
    PROJECT_ROOT = PROJECT_ROOT.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src')) 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}


def run_daily_analytics(**context):
    from job3_analytics import run_analytics
    execution_date = context['execution_date'].date()
    run_analytics(target_date=execution_date)


with DAG(
    dag_id='job3_daily_summary',
    default_args=default_args,
    description='Daily analytics job: SQLite events -> aggregations -> summary table',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['analytics', 'sqlite', 'daily', 'batch'],
) as dag:

    analytics_task = PythonOperator(
        task_id='compute_daily_summary',
        python_callable=run_daily_analytics,
    )