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
    'retry_delay': timedelta(minutes=5),
}


def run_clean_store_task(**context):
    from job2_cleaner import run_cleaner
    run_cleaner()


with DAG(
    dag_id='job2_clean_store',
    default_args=default_args,
    description='Hourly batch job: Kafka -> cleaning -> SQLite',
    schedule='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['cleaning', 'kafka', 'sqlite', 'batch'],
) as dag:

    clean_and_store_task = PythonOperator(
        task_id='clean_and_store',
        python_callable=run_clean_store_task,
    )