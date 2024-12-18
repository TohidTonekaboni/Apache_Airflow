from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from process import etl


default_args = {
    'owner': 'Tohid',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG (
    dag_id='etl_mongodb',
    default_args=default_args, 
    description='This is etl pipline from mongodb and load on clickhouse',
    start_date=datetime(2024, 12 ,18),
    schedule_interval='@hourly'
) as dag:
    task = PythonOperator(task_id='etl_mongo_to_clickhouse',
                          python_callable=etl)
    task

