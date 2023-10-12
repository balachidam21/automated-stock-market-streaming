from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from streaming import start_streaming

start_date = datetime(2023, 9, 30, 12,0)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

with DAG('stock_market_streaming', default_args=default_args, schedule_interval='0 * * * *', catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id = 'stream_to_kafka',
        python_callable=start_streaming,
        dag=dag
    )
    streaming_task