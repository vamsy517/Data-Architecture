from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import pandas as pd
import datetime
from datetime import date, timedelta
import os
from collections import defaultdict


def read_log():
    # read email log to find the last sent email
    df_time = pd.read_csv('/home/ingest/logs/email_log.csv', header = None)
    df_time[0]= pd.to_datetime(df_time[0]).dt.date
    date = df_time.iloc[0,0]
    # get last_day_check date
    last_day_check = date.today() - timedelta(days=1)
    # list all log file in ingest logs folder
    logs_dir = os.listdir('/home/ingest/logs/')
    # keep those logs, which should be processed by the email dag
    email_file_list = [list_item for list_item in logs_dir if list_item.endswith('processing_results.csv')]
    adhoc_list = ['api_test_log.csv', 'cleanup_log.csv']
    email_file_list.extend(adhoc_list)
    # create new defaultdict to store all dataframes
    dict_results = defaultdict()
    # for each file
    for file in email_file_list:
        # read the file with | separator
        df = pd.read_csv('/home/ingest/logs/' + file, header = None, sep='|')
        # for the log file from commercial updates:
        if ('update_processing_results.csv' in file):
            # count number of processed entities
            df['len'] = df[1].apply(lambda x: len(str(x).split(',')))
            # get the entity
            df['city_or_country'] = df[2].apply(lambda x: str(x).split('.')[0])
            # concatenate findings
            df['concat'] = df['city_or_country'] + ': ' + df['len'].astype('str')
            # remove unnecessary columns
            df = df.drop(columns = [1,3,'len','city_or_country'], axis=1)
        # rename all columns
        df.columns = ['Timestamp', 'Filename', 'Message']
        # convert Timestamp string to Timestamp object
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], infer_datetime_format=True)
        # get date from Timestamp object
        df['Date'] = df['Timestamp'].dt.date
        # keep only valid entries
        df_new = df[df['Date']>date]
        # add the results to a dictionary
        dict_results[file]=df_new
    # create response
    response = "<h4>Report on " + str(last_day_check) + ":</h4>"
    # add findings for API test
    df = dict_results['api_test_log.csv']
    if df.shape[0] == 0:
        response = response + '<p>No API test was performed.</p>'
    else:
        response = response + '<p>' + df['Message'].values[0] + '</p>'
    # add findings for Cleanup API
    df = dict_results['cleanup_log.csv']
    if df.shape[0] == 0:
        response = response + '<p>No cleanup was performed.</p>'
    else:
        response = response + '<p>' + df['Message'].values[0] + '</p>'
    # add findings for commercial updates
    df = dict_results['update_processing_results.csv']
    if df.shape[0] == 0:
        response = response + '<p>No updates from Economics API.</p>'
    else:
        response = response + '<p>' + 'Economics API update:' + str(set(df['Message'])) + '</p>'
    # add findings for Covid19 data update
    df = dict_results['update_covid19_processing_results.csv']
    if df.shape[0] == 0:
        response = response + '<p>No updates from Covid19 data.</p>'
    else:
        response = response + '<p>' + str(len(set(df['Filename']))) + ' Covid19 table(s) were updated successfully.' + '</p>'
    # add findings for Companies API updates
    df_raw = dict_results['update_companies_processing_results.csv']
    if df_raw.shape[0] == 0:
        response = response + '<p>No updates from Companies API.</p>'
    else:
        # get number of successfully updated tables
        df_succ=df_raw[df_raw['Message'].str.endswith('successfully.')]
        succ = len(set(df_succ['Filename']))
        # get number of skipped tables
        df_fail=df_raw[~df_raw['Message'].str.endswith('successfully.')]
        fail = len(set(df_fail['Filename']))
        response = response + '<p>' + str(succ) + ' Companies table(s) were updated successfully. ' + str(fail) + ' Companies table(s) were not updated.</p>'
    # add findings for MNC API updates
    df_raw = dict_results['update_mnc_processing_results.csv']
    if df_raw.shape[0] == 0:
        response = response + '<p>No updates from MNC Segments and Subsidiaries API.</p>'
    else:
        # get number of successfully updated tables
        df_succ=df_raw[df_raw['Message'].str.endswith('successfully.')]
        succ = len(set(df_succ['Filename']))
        # get number of skipped tables
        df_fail=df_raw[~df_raw['Message'].str.endswith('successfully.')]
        fail = len(set(df_fail['Filename']))
        response = response + '<p>' + str(succ) + ' MNC table(s) were updated successfully. ' + str(fail) + ' MNC table(s) were not updated.</p>'
    # add findings for Alchemer API updates
    df_raw = dict_results['update_surveys_processing_results.csv']
    if df_raw.shape[0] == 0:
    	response = response + '<p>No updates from Alchemer API.</p>'
    else:
    	# get number of successfully updated tables
    	df_succ=df_raw[df_raw['Message'].str.endswith('successfully.')]
    	succ = len(set(df_succ['Filename']))
    	# get number of skipped tables
    	df_fail=df_raw[~df_raw['Message'].str.endswith('successfully.')]
    	fail = len(set(df_fail['Filename']))
    	response = response + '<p>' + str(succ) + ' Alchemer table(s) were updated successfully. ' + str(fail) + ' Alchemer table(s) were not updated.</p>'
    # add findings for Ingest
    df = dict_results['processing_results.csv']
    if df.shape[0] == 0:
        response = response + '<p>No new file was detected after check on date: ' + str(date) + '</p>'
    else:
        response = response + '<p>The files were recorded from date ' + str(date +  timedelta(days=1)) + " to "+ str(last_day_check) +":</p>" + df.to_html() + '</p>'
    
    return response

def record_email_log():
    with open ('/home/ingest/logs/email_log.csv', 'w') as fl:
        curr_time = date.today() - timedelta(days=1)
        fl.write(str(curr_time))
    return True    

default_args = {
        'owner': 'email_processing_logs',
        'start_date':datetime.datetime(2020, 7, 3, 5, 1, 00),
        'concurrency': 1,
        'retries': 0
}

dag = DAG('email_processing_logs', description='Email processed logs',
          schedule_interval='0 5 * * *',
          default_args = default_args, catchup=False)

read_log = PythonOperator(task_id='read_log', python_callable=read_log, dag=dag)

email = EmailOperator(
        task_id='send_email',
        to=['petyo.karatov@ns-mediagroup.com','veselin.valchev@ns-mediagroup.com','nina.nikolaeva@ns-mediagroup.com','yavor.vrachev@ns-mediagroup.com','Dzhumle.Mehmedova@ns-mediagroup.com'],
        subject='Dag Performed Operations Log',р4
        html_content="""<h3>Dag Performed Operations Log:</h3><div> {{ti.xcom_pull(task_ids='read_log')}} </div>""" ,
        dag=dag
)
record_email_log = PythonOperator(task_id='record_email_log', python_callable=record_email_log, dag=dag)

read_log >> email >> record_email_log