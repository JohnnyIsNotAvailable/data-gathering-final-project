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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def run_ingestion_task(**context):
    from job1_producer import run_producer
    run_producer(duration_seconds=240)


with DAG(
    dag_id='job1_ingestion',
    default_args=default_args,
    description='Continuous data ingestion from API to Kafka',
    schedule=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ingestion', 'kafka', 'api'],
) as dag:

    ingestion_task = PythonOperator(
        task_id='fetch_and_produce',
        python_callable=run_ingestion_task,
    )
