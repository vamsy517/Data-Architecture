from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pardot_utils.dag_functions import *
from permutive_pardot_match.gbq_queries import QUERY_EMAILS

# download emails data
print('Downloading data for current day...')
emails = get_gbq_table(PULLDATA_PROJECT_ID, QUERY_EMAILS)

default_args = {
    'owner': 'project-pulldata_pardot',
    'start_date': datetime.datetime(2021, 2, 9, 21, 1, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_pardot_data_update',
         catchup=False,
         default_args=default_args,
         schedule_interval='0 15 * * *',
         ) as dag:
    # update_prospects = PythonOperator(task_id='upload_data_prospects', python_callable=upload_data_prospects)
    # update_emails = PythonOperator(task_id='upload_data_emails', python_callable=upload_data_emails,
    #                                op_kwargs={
    #                                    "prospects_counter": "{{ti.xcom_pull(task_ids='upload_data_prospects')}}"})
    # update_opportunity = PythonOperator(task_id='upload_data_opportunity', python_callable=upload_data_opportunity,
    #                                     op_kwargs={"emails_counter": "{{ti.xcom_pull(task_ids='upload_data_emails')}}"})
    # update_visitors = PythonOperator(task_id='upload_data_visitors', python_callable=upload_data_visitors,
    #                                     op_kwargs={"opportunity_counter": "{{ti.xcom_pull(task_ids='upload_data_opportunity')}}"})
    # update_visitors_avtivity = PythonOperator(task_id='upload_data_visitor_activity', python_callable=upload_data_visitor_activity,
    #                                     op_kwargs={"visitors_counter": "{{ti.xcom_pull(task_ids='upload_data_visitors')}}"})
    update_visitors_avtivity = PythonOperator(task_id='upload_data_visitor_activity', python_callable=upload_data_visitor_activity)

# update_prospects >> update_emails >> update_opportunity >> update_visitors >> update_visitors_avtivity
update_visitors_avtivity