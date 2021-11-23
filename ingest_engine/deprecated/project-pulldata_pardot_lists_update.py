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

def get_batch(api_key, after_mark, crated_updated, max_id):
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
    url = 'https://pi.pardot.com/api/list/version/4/do/query'
    # if created use params for return new created data
    if crated_updated == 'created':
        print('created')
        p = {
            'format':'bulk',
            'id_greater_than': max_id,
            'created_after': after_mark,
            
            'sort_by': 'id',
            'sort_order': 'ascending'
        }
    else:
        # if 'updated' use params for return new updated data
        print('updated')
        p = {
            'format':'bulk',
            'id_greater_than': max_id,
            'updated_after': after_mark,
            
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

def get_new_created_data(last_date, get_id, crated_updated):
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
    max_id = str(int(get_id) - 1)
    # date from were we will start to collect the data
    after_mark = last_date
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
            response = get_batch(api_key, after_mark, crated_updated, max_id)

            # find error if API key expired get new API key and run again get_bach to return the data
            find_err_1 = re.findall('<err code="1">(.*?)</err>', response.text)
            if len(find_err_1) != 0:
                if find_err_1[0] == 'Invalid API key or user key':
                    api_key = get_api_key()
                    response = get_batch(api_key, after_mark, crated_updated, max_id)
            # dataframe with response
            df = pd.DataFrame.from_dict(xmltodict.parse(response.text))
            print(df)
            # if first run get the total of records
            if count_total:
                # try/except block because api return different results if there is no new reocords
                try:
                    total_results = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])    
                except:
                    total_results = int(df.loc['result']['rsp']['total_results'])
            # total for the iteration
            # try/except block because api return different results if there is no new reocords
            try:
                total_for_period = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])
            except:
                total_for_period = int(df.loc['result']['rsp']['total_results'])
            print(total_for_period)
            # record the dataframe with the needed data
            # if total for period is 1 or more there is different output so we use different code to get the data in dataframe
        
            if total_for_period == 1:
                df = df.loc['result']['rsp']
                df = pd.DataFrame([df['list']], columns=df['list'].keys())
            # if there is no data for update return False
            elif total_for_period == 0:
                return False
            else:
                df = json_normalize(pd.DataFrame.from_dict(df.loc['result']['rsp'])['list'])
#    if error appeared wait 1 min and run try block again. if there is 5 errors raise error that there is to many errors
        except:
            print("API NOT WORK! TRIED AGAIN")
            error_count += 1
            if error_count == 5:
                raise Exception('Too many ERRORS!')
            time.sleep(60)
            continue
#      set max id to the max id in the dataframe so we can iterate from that ID for the next batch of 200  
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
        final_df = final_df.drop_duplicates(subset='id', keep="last")
        print('total_shape: ' + str(final_df.shape))
        # if records from final data are equal or more than total results return from API we are collected all the data and break the loop
        
        if final_df.shape[0] >= total_results:
            break
    # return all collected data
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
    
    sql = 'select max(created_at) as max_created_at, max(updated_at) as max_updated_at, min(id) as id from ' + project_id + '.' + dataset + '.' + tablename
    df_max = pandas_gbq.read_gbq(query = sql)
    return df_max

def log_update(ids_update, ids_new, table):
    '''
        log the tables and IDs which are updated
    '''
    print(f'Log table {table}')
    with open ('/home/ingest/logs/update_lists_results.csv', 'a') as fl:
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_ids_count = str(len(ids_update))
        
        if update_ids_count == '0':
            fl.write(str(curr_time) + '|' + update_ids_count + '|' + table + '|Nothing to update.\n')
        else:
            fl.write(str(curr_time) + '|' + update_ids_count + '|' + table + '|Updated successfully.\n')
        new_ids_count = str(len(ids_new))
        
        if new_ids_count == '0':
            fl.write(str(curr_time) + '|' + new_ids_count + '|' + table +  '|Nothing to upload.\n')
        else:
            fl.write(str(curr_time) + '|' + new_ids_count + '|' + table +  '|New uploaded successfully.\n')       

def clean_cols(df):
    # clean column names to fit GBQ convention
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('/', '_')
    df.columns = df.columns.str.replace('-', '_')
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('.', '_')
    df.columns = df.columns.str.replace('[^A-Za-z0-9_]+', '')
    df.columns = ['_' + x  if x[0].isdigit()  else x for x in df.columns]
    return df

def upload_data():
    # set project ID and connect to GBQ and Postgree
    project_id = "project-pulldata"
    gbq_client = get_credentials(project_id)
    postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
#   get max created and updated values to start download form that values
    max_created_updated = get_max(project_id, 'Pardot', 'Lists') 
    date_cols = ['created_at','updated_at']
#     get new created data
    new_created_df = get_new_created_data(max_created_updated.max_created_at.max(), str(int(max_created_updated.id.min()) - 1), 'created')
# check if return from API is data frame. because if not return None and if check if it is None not work when response is dataframe
# if not dataframe it is None and there is no new data
    if not isinstance(new_created_df, pd.DataFrame):
        # print
        print('There is no new created data')
        ids_new = []
        new_created_df = pd.DataFrame()
    else:
        new_created_df = new_created_df.apply(lambda x : pd.to_numeric(x, errors='ignore'))
        
        # convert to datetime
        new_created_df[date_cols] = new_created_df[date_cols].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S', errors='ignore', utc=True)
        
        # get new created IDs
        ids_new = list(new_created_df.id)
 
    # get new updated data
    new_updated_df = get_new_created_data(max_created_updated.max_updated_at.max(), str(int(max_created_updated.id.min()) - 1), 'updated')
    if not isinstance(new_updated_df, pd.DataFrame):
        print('There is no new updated data')
        ids_update = []
        new_updated_df = pd.DataFrame()
    else:
        new_updated_df = clean_cols(new_updated_df)
        new_updated_df = new_updated_df.apply(lambda x : pd.to_numeric(x, errors='ignore'))
        # get new updated IDs that are not new created too
        ids_update = list(new_updated_df.id)  
        # convert to datetime
        new_updated_df[date_cols] = new_updated_df[date_cols].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S', errors='ignore', utc=True)
        # get new updated IDs that are not new created too

    # concat new updated data from API with dataframe with final data
    final_df = pd.concat([new_created_df, new_updated_df], axis=0, ignore_index=True, sort=False)
    if final_df.shape[0] == 0:
        print('There is no new data to update')
        log_update(ids_update, ids_new, 'Lists')
        print('Log for unsucsssesful is recorded')
        return True
    final_df = clean_cols(final_df)
    
    # fix all numeric values
    final_df = final_df.apply(lambda x : pd.to_numeric(x, errors='ignore'))
    final_df[date_cols] = final_df[date_cols].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S', errors='ignore', utc=True)
    
#     final_df["last_updated_date"] = pd.NaT
    final_ids = list(set(ids_new + ids_update))
    # and drop duplicates
    final_df = final_df.drop_duplicates(keep="last")
    # fix data types
    final_df[['title','description']] = final_df[['title','description']].astype(str)
    d = {'false': False, 'true': True}
    final_df['is_public'] = final_df['is_public'].map(d)
    final_df['is_dynamic'] = final_df['is_dynamic'].map(d)
    final_df['is_crm_visible'] = final_df['is_crm_visible'].map(d)
    # delete dataframes from memory
    del new_created_df
    del new_updated_df
    update_lists = datahub_updater.Updater(project_id, 
                                            'Pardot', 'Lists', final_df,
                                            'id', ['id'],
                                             final_ids)
    print('Update Pardot Lists data')
    update_lists.update_data() 
    # record log file
    log_update(ids_update, ids_new, 'Lists')
    print('Log is recorded')    
    return True

default_args = {
    'owner': 'project-pulldata_Pardot_Lists_data_update',
    'start_date': datetime.datetime(2020, 11, 24, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_Pardot_Lists_data_update',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 19 * * *',
     ) as dag:
    
    
    update_list = PythonOperator(task_id='upload_data',
                               python_callable=upload_data)

update_list