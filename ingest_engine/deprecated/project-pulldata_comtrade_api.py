from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import datetime

from comtrade_api import CommonTradeAPI

a = CommonTradeAPI(
                    save_path='/home/ingest/input',
                    gbq_creds_path='/home/ingest/credentials'
                )

default_args = {
    'owner': 'project-pulldata_comtrade_api',
    'start_date': datetime.datetime(2021, 1, 1, 00, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_comtrade_api',
     catchup=False,
     default_args=default_args,
     schedule_interval='@monthly',
     ) as dag:
    
    
    check_data_utils = PythonOperator(task_id='run',
                               python_callable=a.run)

check_data_utils
