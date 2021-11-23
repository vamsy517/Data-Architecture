from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import pandas as pd
import datetime
from trends_utils.utils import download_trends
import os


def download_latest_trends():
    # Read google sheet with keywords list
    kw_df = pd.read_excel(
        'https://docs.google.com/spreadsheets/d/e/'
        '2PACX-1vQQ3lOkjhGyAMr1yHzQPWkOObC1HY51Z9BilE2M6dXq08LaVHWdMVL_91ouWwdliC5uHKioUfuuc_mK/pub?output=xlsx')

    # Download trends for each domain for the last 5 years and upload them to GBQ dataset
    download_trends(kw_df, 'daily', "_latest_breaking_rising_trends")
    return True


default_args = {
    'owner': 'airflow_gcp',
    'start_date': datetime.datetime(2021, 7, 7, 5, 4, 00),
    'concurrency': 1, 'retries': 0
}
dag = DAG('google_trends_latest',
          description='Download latest trends data for given keywords list and upload results to BigQuery',
          schedule_interval='0 6 * * *',
          default_args=default_args, catchup=False)
latest_trends = PythonOperator(task_id='download_latest_trends', python_callable=download_latest_trends, dag=dag)
email = EmailOperator(
        task_id='send_email',
        to=['petyo.karatov@ns-mediagroup.com', 
            'nina.nikolaeva@ns-mediagroup.com',
            'Dzhumle.Mehmedova@ns-mediagroup.com',
            'Laura.Griffiths@globaldata.com',
            'jade.larkin@globaldata.com',
            'emma.haslett@ns-mediagroup.com'],
        subject='Daily Breaking and Rising Trends',
        html_content="""<h3>Daily reports for breaking and rising trends
        for each requested domain.</h3>""",
        files=['/home/ingest/trends/daily/' + i for i in os.listdir('/home/ingest/trends/daily/')],
        dag=dag
    )
email_tech_monitor = EmailOperator(
        task_id='send_email_tech_monitor',
        to=['pete.swabey@techmonitor.ai', 
            'matthew.gooding@techmonitor.ai',
            'victor.vladev@ns-mediagroup.com'],
        subject='Tech Monitor Daily Breaking and Rising Trends',
        html_content="""<h3>Daily reports for breaking and rising trends
        for Tech Monitor.</h3>""",
        files=['/home/ingest/trends/daily/tech_monitor_latest_breaking_rising_trends.xlsx'],
        dag=dag
    )

latest_trends >> [email, email_tech_monitor]
