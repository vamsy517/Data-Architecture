"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import airflow
import pandas
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'testdag_deploy',
    default_args=default_args,
    description='new dag',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=40))

# priority_weight has type int in Airflow DB, uses the maximum.
t1 = BashOperator(
    task_id='echo',
    bash_command='echo new dag deployed',
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31-1)