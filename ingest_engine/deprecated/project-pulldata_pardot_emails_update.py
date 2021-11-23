from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

import pandas as np
from pandas.io.json import json_normalize
import requests
import pandas as pd
import json
import xmltodict
import re
import time
import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq

import pytz

import numpy as np
from sqlalchemy import create_engine
import datahub_updater

def get_credentials(project_id):
    ''' 
        Create a BigQuery client and authorize the connection to BigQuery
        Input:  
                project_id: project-pulldata
        Output:
                return big query client
    '''
    keyfile='/home/ingest/credentials/gbq_credentials.json'
    credentials = service_account.Credentials.from_service_account_file(
        keyfile,
    )
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = project_id
    
    client = bigquery.Client.from_service_account_json(keyfile)
    return client

def get_api_key():
    '''
    Get API key
    Return : API key for Authorization
    '''
    # API url for login
    url = 'https://pi.pardot.com/api/login/version/4'
    # json with data that need to be pass via post request
    d = {
        'email':'leads@verdict.co.uk',
        'password':'Leads@123',
        'user_key':'bfe54a8f04e0a7d4239d5bcd90e79cbd'
    }
    #get response via post request
    response = requests.post(url, data=d)
    # get api from response by searching what is inside <api_key> tag
    api_key = re.findall("<api_key>(.*?)</api_key>", response.text)[0]
    return api_key

def get_batch(api_key, after_mark, max_id):
    '''
    Get bach of first 200 recodrs
    Args:
        api_key: api key for Authorization
        after_mark: last date that we need to start download
        max_id: id from where we continue download
    Output: first 200 records
    '''
    # API url
    url = 'https://pi.pardot.com/api/emailClick/version/4/do/query'
    # params for return new created data
    p = {
        'id_greater_than': max_id,
        'created_after': after_mark,            
        'sort_by': 'id',
        'sort_order': 'ascending'
    }
    # header params for Authorization
    h = {
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Pardot user_key=bfe54a8f04e0a7d4239d5bcd90e79cbd ,api_key={api_key}"
    }
    # get request to retur the data
    response = requests.get(url, headers=h, params=p)
    return response

def get_batch_emails(api_key, list_email_id):
    '''
    Get bach of first 200 recodrs
    Args:
        api_key: api key for Authorization
        list_email_id: id for email

    Output: first 200 records
    '''
    # API url with list email id
    url = f'https://pi.pardot.com/api/email/version/4/do/stats/id/{list_email_id}?'
    # if created use params for return new created data
    p = {}
    # header params for Authorization
    h = {
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Pardot user_key=bfe54a8f04e0a7d4239d5bcd90e79cbd ,api_key={api_key}"
    }
    # get request to retur the data
    response = requests.get(url, headers=h, params=p)
    return response

def get_new_created_data(last_date, get_id):
    '''
    Get all created/updated data
    Args:
        last_date: date to start from
        get_id: ID to interate the data
    Output:
        return all collected data (creaded new or updated new)
    '''
    # get api_key
    api_key = get_api_key()
    # get first ID from where will start to collect the data
    max_id = str(int(get_id) - 1)
    # date from were we will start to collect the data
    after_mark = last_date - datetime.timedelta(seconds=1)
    print(after_mark)
    
    # set count_total to true for first iteration
    count_total = True
    # set error counter to 0
    error_count = 0
    # set total records to 0
    total_record = 0
    # create empty dataframe for apending the data
    final_df = pd.DataFrame()
    # start loop to iterate to all data 200 by 200
    while True:
        # try except block to handle errors because API not respond or hit limits
        try:
            
            # get bach of 200 records
            response = get_batch(api_key, after_mark, max_id)
            # find error if API key expired get new API key and run again get_bach to return the data
            find_err_1 = re.findall('<err code="1">(.*?)</err>', response.text)
            if len(find_err_1) != 0:
                if find_err_1[0] == 'Invalid API key or user key':
                    api_key = get_api_key()
                    response = get_batch(api_key, after_mark, max_id)
            # dataframe with response
            df = pd.DataFrame.from_dict(xmltodict.parse(response.text))
            print(df)
            # if first run get the total of records
            if count_total:
                total_results = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])    

            # total for the iteration
            total_for_period = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])
            # record the dataframe with the needed data
            df = json_normalize(pd.DataFrame.from_dict(df.loc['result']['rsp'])['emailClick'])
        # if error appeared wait 1 min and run try block again. if there is 5 errors raise error that there is to many errors
        except:
            print("API NOT WORK! TRIED AGAIN")
            error_count += 1
            if error_count == 5:
                raise Exception('Too many ERRORS!')
            time.sleep(60)
            continue
        # set max id to the max id in the dataframe    
        max_id = df.id.max()

            
        print('Total_results: ' + str(total_results))
        print('total_for_period: ' + str(total_for_period))
        print(after_mark)
        print('shape:'+ str(df.shape))
        # drop these IDs from final dataframe
        ids = list(df.id)
        # if not first run remove data with IDs from new dataframe and set count total to false
        if count_total == False:
            final_df = final_df[~final_df.id.isin(ids)]
        count_total = False    
            
        # concatenata dataframe from current iteration to final dataframe and drop duplicates rows if any
        final_df = pd.concat([final_df, df], axis=0, ignore_index=True, sort=False)
        
        print('total_shape: ' + str(final_df.shape))
        # if records from final data are equal or more than total results return from API we are collected all the data and break the loop
        after_mark = datetime.datetime.strptime(df.created_at.max(), '%Y-%m-%d %H:%M:%S')
        if final_df.shape[0] >= total_results:
            break
    # return all collected data
    return final_df

def get_email_metrics(final_df):
    # get data for Emails metric detals interating and call the function to get metrics from API
    # for every ID from New Email Clicks
    api_key = get_api_key()
    df_full_email = pd.DataFrame()
    for list_email_id in final_df.list_email_id.unique():
        response = get_batch_emails(api_key, list_email_id)
        df = pd.DataFrame.from_dict(xmltodict.parse(response.text))
        df = json_normalize(df.loc['stats']['rsp'])
        df['id'] = list_email_id
        df_full_email = df_full_email.append(df, ignore_index=True)
    print('Emails metrics data is collected')
    return df_full_email

def get_max(project_id, dataset, tablename):
    '''
    Get data from GBQ
    Input:
        project_id: id of project
        dataset: name of dataset
        tablename: name of table
    Output:
        dataframe with max created at, max updated at and min id
    '''
    
    sql = 'select max(created_at) as max_created_at, min(id) as id from ' + project_id + '.' + dataset + '.' + tablename
    df_max = pandas_gbq.read_gbq(query = sql)
    return df_max

def log_update(ids_clicks, ids_details):
    '''
        log the tables and IDs which are updated
    '''
    print(f'Log table Emails')
    with open ('/home/ingest/logs/update_pardot_emails_results.csv', 'a') as fl:
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ids_clicks_count = str(len(ids_clicks))
        fl.write(str(curr_time) + '|' + ids_clicks_count + '|Emails clicks updated successfully.\n')
        ids_details_count = str(len(ids_details))
        fl.write(str(curr_time) + '|' + ids_details_count + '|Emails metric details updated successfully.\n')

def clean_cols(df):
    # clean column names to fit GBQ convention
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('/', '_')
    df.columns = df.columns.str.replace('-', '_')
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('[^A-Za-z0-9_]+', '')
    df.columns = ['_' + x  if x[0].isdigit()  else x for x in df.columns]
    return df

def upload_data():
    # set project ID and connect to GBQ and Postgree
    project_id = "project-pulldata"
    gbq_client = get_credentials(project_id)
    postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
#   get max created and updated values to start download form that values
    max_created_updated = get_max(project_id, 'Pardot', 'EmailClicks')
    # get new created data
    new_created_df = get_new_created_data(max_created_updated.max_created_at.max(), str(int(max_created_updated.id.min()) - 1))
    # fix data types and column names
    new_created_df = new_created_df.apply(lambda x : pd.to_numeric(x, errors='ignore'))
    new_created_df['email_template_id'] = new_created_df['email_template_id'].astype(pd.Int64Dtype())
    date_cols = ['created_at']  
    new_created_df[date_cols] = new_created_df[date_cols].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S', errors='ignore', utc=True)
    ids_clicks = list(new_created_df.id)
    # update the data with datahub updater class
    update_emailclicks = datahub_updater.Updater(project_id, 
                                            'Pardot', 'EmailClicks', new_created_df,
                                            'id', ['id'],
                                             ids_clicks)
    print('Update Pardot EmailsClicks data')
    update_emailclicks.update_data()
    # get the data for email metric details and fix the data types
    email_df = get_email_metrics(new_created_df)
    filter_col = [col for col in email_df if col.endswith('rate') or col.endswith('ratio')]
    for col in filter_col:
        email_df[col] = email_df[col].str.rstrip('%').astype('float') / 100.0
    email_df = email_df.apply(lambda x : pd.to_numeric(x, errors='ignore'))
    email_df.id = email_df.id.astype('int')
    ids_details = list(email_df.id)
    # update the data with datahub updater class
    update_emaildetails = datahub_updater.Updater(project_id, 
                                            'Pardot', 'Emails', email_df,
                                            'id', ['id'],
                                             ids_details)
    print('Update Pardot Emails data')
    update_emaildetails.update_data()
    # add log to log file
    log_update(ids_clicks, ids_details)
    print('Log is recorded') 
    return True


default_args = {
    'owner': 'project-pulldata_Pardot_Emails_data_update',
    'start_date': datetime.datetime(2020, 11, 24, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_Pardot_Emails_data_update',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 20 * * *',
     ) as dag:
        
    update_email = PythonOperator(task_id='upload_data',
                               python_callable=upload_data)

update_email