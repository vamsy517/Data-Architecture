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
import datahub_updater
import numpy as np
from sqlalchemy import create_engine
import numpy as np

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
        'email':'adminpardot@ns-mediagroup.com',
        'password':'Test1234!',
        'user_key':'a3a3f52c58d97ad56d8baa4a483bab09'
    }
    #get response via post request
    response = requests.post(url, data=d)
    # get api from response by searching what is inside <api_key> tag
    api_key = re.findall("<api_key>(.*?)</api_key>", response.text)[0]
    return api_key
def get_batch(api_key, after_mark, before_mark, crated_updated, max_id):
    '''
    Get bach of first 200 recodrs
    Args:
        api_key: api key for Authorization
        after_mark: last date that we need to start download
        create_updated: if 'created' download new created, if 'updated' new updated
        max_id: id from where we continue download
    Output: first 200 records
    '''
    # API url
    url = 'https://pi.pardot.com/api/listMembership/version/4/do/query'
    # if created use params for return new created data
    if crated_updated == 'created':
        print('created')
        p = {
            
            'id_greater_than': max_id,
            'created_after': after_mark,
            'created_before': before_mark,
            
            'sort_by': 'id',
            'sort_order': 'ascending'
        }
    else:
        # if 'updated' use params for return new updated data
        print('updated')
        p = {
            
            'id_greater_than': max_id,
            'updated_after': after_mark,
            'updated_before': before_mark,
            'created_after': '2020-09-01 00:00:00',
            'sort_by': 'id',
            'sort_order': 'ascending'
        } 
    # header params for Authorization
    h = {
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Pardot user_key=a3a3f52c58d97ad56d8baa4a483bab09 ,api_key={api_key}"
    }
    # get request to retur the data
    response = requests.get(url, headers=h, params=p)
    return response

def get_new_created_updated_data(last_date, get_id, crated_updated):
    '''
    Get all created/updated data
    Args:
        last_date: date to start from
        get_id: ID to interate the data
        create_updated: if 'created' download new created, if 'updated' new updated
    Output:
        return all collected data (creaded new or updated new)
    '''
    # get api_key
    api_key = get_api_key()
    # get first ID from where will start to collect the data
    max_id = get_id
    # date from were we will start to collect the data
    after_mark = last_date - datetime.timedelta(seconds=1)
    print(after_mark)
    time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # create empty dataframe for apending the data
    final_df = pd.DataFrame()
    # set empty dataframe that will append data for the period
    
    while True:
        period_df = pd.DataFrame()
            # set count_total to true for first iteration
        count_total = True
        # set error counter to 0
        error_count = 0
        # set total records to 0
        total_record = 0
        # start loop to iterate to all data 200 by 200
        while True:

            # set period for 8 hours
            before_mark = after_mark + datetime.timedelta(hours=3)
            # try except block to handle errors because API not respond or hit limits
            try:
                # get bach of 200 records
                response = get_batch(api_key, after_mark, before_mark, crated_updated, max_id)
                # find error if API key expired get new API key and run again get_bach to return the data
                find_err_1 = re.findall('<err code="1">(.*?)</err>', response.text)
                if len(find_err_1) != 0:
                    if find_err_1[0] == 'Invalid API key or user key':
                        api_key = get_api_key()
                        response = get_batch(api_key, after_mark, before_mark, crated_updated, max_id)
                # dataframe with response
                df = pd.DataFrame.from_dict(xmltodict.parse(response.text))
                print(df)
                # if first run get the total of records
                if count_total:
                    try:
                        total_results = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])    
                    except:
                        total_results = int(df.loc['result']['rsp']['total_results'])
                    # count total to false
                    count_total = False
                # try/except block because api return different results if there is no new reocords
                try:
                    total_for_period = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])
                except:
                    total_for_period = int(df.loc['result']['rsp']['total_results'])
                # if we have 0 new record return the final dataframe with all reuslts
             
                if total_for_period == 1:
                    df = df.loc['result']['rsp']
                    df = pd.DataFrame([df['list_membership']], columns=df['list_membership'].keys())
                # if there is no data for update return False
                elif total_for_period == 0:
                    break
                else:
                    df = json_normalize(pd.DataFrame.from_dict(df.loc['result']['rsp'])['list_membership'])
 
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
            
            # concatenata dataframe from current iteration to final dataframe and drop duplicates rows if any
            period_df = pd.concat([period_df, df], axis=0, ignore_index=True, sort=False)
            period_df = period_df.drop_duplicates(subset='id', keep="last")
            print('total_shape: ' + str(period_df.shape))
            # if records from final data are equal or more than total results return from API we are collected all the data and break the loop

            if period_df.shape[0] >= total_results:
                break
                
                
        if not final_df.empty:
            print('Max min between periods:')
            print(final_df.id.max(), period_df.id.min())        
        # concatenata dataframe from current iteration to final dataframe and drop duplicates rows if any
        final_df = pd.concat([final_df, period_df], axis=0, ignore_index=True, sort=False)
        final_df = final_df.drop_duplicates(subset='id', keep="last")
        if crated_updated == 'created':
            after_mark = datetime.datetime.strptime(period_df.created_at.max(), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(seconds=1)
        else:
            after_mark = datetime.datetime.strptime(period_df.updated_at.max(), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(seconds=1)
        max_id = str(int(period_df.id.max()) - 1)
        if datetime.datetime.strptime(before_mark.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S') >= datetime.datetime.strptime(time_now, '%Y-%m-%d %H:%M:%S'):
            return final_df

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
    
    sql = 'select max(created_at) as max_created_at, max(updated_at) as max_updated_at, min(id) as minid, max(id) as maxid from ' + project_id + '.' + dataset + '.' + tablename
    df_max = pandas_gbq.read_gbq(query = sql)
    return df_max

def log_update(ids, table):
    '''
        log the tables and IDs which are updated
    '''
    print(f'Log table {table}')
    with open ('/home/ingest/logs/update_pardot_list_memberships_results_b2c.csv', 'a') as fl:
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_ids_count = str(len(ids))
        fl.write(str(curr_time) + '|' + update_ids_count + '|' + table + '|Updated successfully.\n')

        
def upload_data():
    # set project ID and connect to GBQ and Postgree
    project_id = "project-pulldata"
    gbq_client = get_credentials(project_id)
    postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
    # get max created and updated values to start download form that values
    max_created_updated = get_max(project_id, 'PardotB2C', 'List_Memberships')
    # get new created data
    new_created_df = get_new_created_updated_data(max_created_updated.max_created_at.max(), str(int(max_created_updated.maxid) - 1), 'created')
    # get new updated data
    new_updated_df = get_new_created_updated_data(max_created_updated.max_updated_at.max(), str(int(max_created_updated.minid) - 1), 'updated')
    # concat update and new data
    final_df = pd.concat([new_created_df, new_updated_df], axis=0, ignore_index=True, sort=False)
    # fix data types
    final_df = final_df.apply(lambda x : pd.to_numeric(x, errors='ignore'))
    d = {'false': False, 'true': True}
    final_df['opted_out'] = final_df['opted_out'].map(d)
    date_cols = ['created_at','updated_at']
    final_df[date_cols] = final_df[date_cols].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S', errors='ignore', utc=True)
    # drop duplicates rows
    final_df = final_df.drop_duplicates(keep="last")
    # record IDs
    ids = list(final_df.id)
    # split final data in chunks and run updater class on every chunk
    # we split the data because is to big and can't be uploaded at once
    def index_marks(nrows, chunk_size):
        return range(1 * chunk_size, (nrows // chunk_size + 1) * chunk_size, chunk_size)
    def split(dfm, chunk_size):
        indices = index_marks(dfm.shape[0], chunk_size)
        return np.split(dfm, indices)
    
    chunks = split(final_df, 30000)
    for c in chunks:
        ids_details = list(c.id)
        update_list = datahub_updater.Updater(project_id, 
                                            'PardotB2C', 'List_Memberships', c,
                                            'id', ['id'],
                                            ids_details)
        update_list.update_data()
    # record log file
    
    log_update(ids, 'List_Memberships')
    print('Log is recorded')
    return True

default_args = {
    'owner': 'project-pulldata_Pardot_List_Memberships_data_update_B2C',
    'start_date': datetime.datetime(2020, 11, 24, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_Pardot_List_Memberships_data_update_B2C',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 23 * * *',
     ) as dag:
    
    update_list_memberships = PythonOperator(task_id='upload_data',
                               python_callable=upload_data)
update_list_memberships