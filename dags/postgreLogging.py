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
   
    #print(client.execute('time clickhouse-client --query="INSERT INTO admin_backend_logs.logs FORMAT CSV" < /home/oleg/web/go/gd_admin_backend/src/logger/app1.csv'))
    #time clickhouse-client --query="INSERT INTO admin_backend_logs.logs_tmp FORMAT CSV" < /home/oleg/web/go/gd_admin_backend/src/logger/app1.csv
    
    
    # data_file = []
    # in_file = '/home/oleg/web/go/gd_admin_backend/src/logger/app.log' # поменять на нормальный путь

    # with open(in_file, 'r') as read_file:
    #     for line in read_file:
    #         tmp = line.strip('\n')
    #         tmp = tmp.split(' -- ')
    #         tmp[1] = tmp[1].split(' ')
    #         if(tmp[1][0] == "GET"):
    #             tmp[1] = { "method": tmp[1][0], "originalUrl": tmp[1][1], "statusCode": tmp[1][2], "contentType": tmp[1][3] + tmp[1][4], "userAgent": tmp[1][5], "ip": tmp[1][6] }
    #             data_file.append(tmp[1])

    # read_file.close()
    
    # print(data_file)
    # request = f"INSERT INTO admin_backend_log (method, originalUrl, statusCode, contentType, userAgent, ip) VALUES('{data_file[0]['method']}', '{data_file[0]['originalUrl']}', '{data_file[0]['statusCode']}', '{data_file[0]['contentType']}', '{data_file[0]['userAgent']}', '{data_file[0]['ip']}');"
    
    # print(request)
    # pg_hook = PostgresHook(postgre_conn_id="postgres_default", schema="airflow")

    # conn = pg_hook.get_conn()
    # cur = conn.cursor()
    # cur.execute(request)
    # conn.commit()

    # return conn
    return 1

with DAG('admin_backend_logs_dag',
    default_args=default_args,
    schedule_interval=timedelta (days = 1),
    catchup=False) as dag:

    start_task = DummyOperator(task_id='start_task')
    discharge_task = PythonOperator(task_id='discharge_task', python_callable=get_activated_sources)
    start_task >> discharge_task