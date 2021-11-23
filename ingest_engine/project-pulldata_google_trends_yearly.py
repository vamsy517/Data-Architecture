from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import pandas as pd
import datetime
from trends_utils.utils import download_trends
import os


def download_yearly_trends():
    # Read google sheet with keywords list
    kw_df = pd.read_excel('https://docs.google.com/spreadsheets/d/e/'
                          '2PACX-1vQQ3lOkjhGyAMr1yHzQPWkOObC1HY51Z9BilE2M6dXq08LaVHWdMVL_91ouWwdliC5uHKioUfuuc_mK/'
                          'pub?output=xlsx')
    
    # Download trends for each domain for the last 5 years and upload them to GBQ dataset
    download_trends(kw_df, 'yearly', "_breaking_rising_trends_past_5_years")
    return True


default_args = {
        'owner': 'airflow_gcp',
        'start_date': datetime.datetime(2021, 7, 11, 8, 00, 00),
        'concurrency': 1,
        'retries': 0
}
dag = DAG('google_trends_5_years', description='Download trends data for given keywords'
                                               'list for the past 5 years and upload results to BigQuery',
          schedule_interval='@monthly',
          default_args=default_args, catchup=False)

trends = PythonOperator(task_id='download_yearly_trends', python_callable=download_yearly_trends, dag=dag)
email = EmailOperator(
        task_id='send_email',
        to=['petyo.karatov@ns-mediagroup.com', 
            'nina.nikolaeva@ns-mediagroup.com',
            'Dzhumle.Mehmedova@ns-mediagroup.com',
            'Laura.Griffiths@globaldata.com',
            'jade.larkin@globaldata.com',
            'emma.haslett@ns-mediagroup.com'],
        subject='Breaking and Rising Trends for Last 5 Years',
        html_content="""<h3>Reports for breaking and rising trends
        for the last 5 years for each requested domain.</h3>""",
        files=['/home/ingest/trends/yearly/' + i for i in os.listdir('/home/ingest/trends/yearly/')],
        dag=dag
    )
email_tech_monitor = EmailOperator(
        task_id='send_email_tech_monitor',
        to=['pete.swabey@techmonitor.ai', 
            'matthew.gooding@techmonitor.ai',
            'victor.vladev@ns-mediagroup.com'],
        subject='Tech Monitor Breaking and Rising Trends for Last 5 Years',
        html_content="""<h3>Reports for breaking and rising trends
         for the last 5 years for Tech Monitor.</h3>""",
        files=['/home/ingest/trends/yearly/tech_monitor_breaking_rising_trends_past_5_years.xlsx'],
        dag=dag
    )



trends >> [email, email_tech_monitor]
