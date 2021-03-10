from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from clickhouse_driver.client import Client
import os
from dotenv import load_dotenv
load_dotenv()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 9),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

def get_activated_sources():
    DB_HOST = os.getenv('DB_HOST')
    PATH_LOG_FILE = os.getenv('PATH_LOG_FILE')
    client = Client(f'{DB_HOST}')
    
    print(client.execute("DROP TABLE IF EXISTS admin_backend_logs.logs"))
    os.system(f'time clickhouse-client --query="INSERT INTO admin_backend_logs.logs_tmp FORMAT CSV" < {PATH_LOG_FILE}')
    print(client.execute("CREATE TABLE admin_backend_logs.logs　ENGINE = ReplacingMergeTree(day, (date), 8192)　AS SELECT DISTINCT　toDate(date) AS day,　date,　method,　originalUrl,　statusCode,　contentType,　userAgent,　ip, userId　FROM admin_backend_logs.logs_tmp;"))
   
    return 1

with DAG('admin_backend_logs_dag',
    default_args=default_args,
    schedule_interval=timedelta (days = 1),
    catchup=False) as dag:

    start_task = DummyOperator(task_id='start_task')
    discharge_task = PythonOperator(task_id='discharge_task', python_callable=get_activated_sources)
    start_task >> discharge_task