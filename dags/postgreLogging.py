from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from clickhouse_driver.client import Client
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 9),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

def get_activated_sources():
    client = Client('127.0.0.1')
    print(client.execute("DROP TABLE IF EXISTS admin_backend_logs.logs"))
    os.system('time clickhouse-client --query="INSERT INTO admin_backend_logs.logs_tmp FORMAT CSV" < /home/oleg/web/go/gd_admin_backend/src/logger/app.csv')# поменять путь
    print(client.execute("CREATE TABLE admin_backend_logs.logs　ENGINE = ReplacingMergeTree(day, (date), 8192)　AS SELECT DISTINCT　toDate(date) AS day,　date,　method,　originalUrl,　statusCode,　contentType,　userAgent,　ip　FROM admin_backend_logs.logs_tmp;"))
   
    return 1

with DAG('admin_backend_logs_dag',
    default_args=default_args,
    schedule_interval=timedelta (days = 1),
    catchup=False) as dag:

    start_task = DummyOperator(task_id='start_task')
    discharge_task = PythonOperator(task_id='discharge_task', python_callable=get_activated_sources)
    start_task >> discharge_task