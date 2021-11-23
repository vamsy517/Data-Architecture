import pandas as pd
import pandas_gbq
import re
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import datetime
import time
import pytz
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import bigquery_storage
from sqlalchemy import create_engine 
import requests
import json
from pandas.io.json import json_normalize
import datahub_updater
import numpy as np
import os

# set up the credentials to access the GCP database
def set_credentials():
    keyfile='/home/ingest/credentials/gbq_credentials.json'
    project_id = 'project-pulldata'
    credentials = service_account.Credentials.from_service_account_file(
        keyfile,
    )
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = project_id

    # Initialize clients.
    bqclient = bigquery.Client(credentials=credentials, project=project_id)
    bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)
    return bqclient, bqstorageclient

def download_emails():
    '''
    Get emails data from GBQ.
    Output:
        list of the emails form formsubmission events table
    
    '''
    # initialize connection to GBQ
    bqclient, bqstorageclient = set_credentials()
    # set query to pull emails data
    cur_date = datetime.datetime.today().strftime('%Y-%m-%d')
    query_string = f"""
        SELECT * FROM `project-pulldata.Pardot.PermutiveMapping_{cur_date}`
        """
    dataframe = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )
    return dataframe

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

def get_prospect_data(api_key, email):
    ''' 
        Input:  
                api_key: api_key to access api
                email: email for which we call the api and get its data
                
        Output:
                return prospect_id in case the email matches,in other case return error message
    '''
    # API url
    url = f'https://pi.pardot.com/api/prospect/version/4/do/read/email/{email}'
    # header params for Authorization
    h = {
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Pardot user_key=a3a3f52c58d97ad56d8baa4a483bab09 ,api_key={api_key}"
    }
    # get request to return the data
    response = requests.get(url, headers=h)
    # check api key, if it is not correct return message 
    if 'err code="1"' in response.text:
        print('Invalid api key.')
        return 'invalid key'
    # check if email exists in pardot data, if it doesnt exist return a message
    if 'err code="4"' in response.text or ''== response.text:
        print('The email does not exist in Pardot.')
        return 'invalid email'
    # if the email is correct and exists in pardot, get the prospect_id associated with it
    prospect_id = re.findall("<id>(.*?)</id>", response.text)[0]
    return prospect_id

def collect_all_emails_ids():
    '''
        Function to download all the matching data between Pardot and Permutive and upload the matches to the corresponding table in GBQ.
    '''
    # create a empty dataframe to fill with matching data
    mapping_table = pd.DataFrame()
    # set api_key
    api_key = get_api_key()
    # set GBQ parameters
    dataset = 'PardotB2C'
    table = 'PermutivePardotMatch'
    project_id = 'project-pulldata'
    # download emails data
    print('Downloading data for current day...')
    emails = download_emails()
    # if the emails dataframe is empty print a message
    if emails.empty:
        print('There are no new emails to process.')
    # if emails dataframe is not empty, process the emails
    else:
        print('Processing matches between Pardot and Permutive...')
        # for each row in emails
        for item in emails.values:
            # create a df for a single emails data
            single_df = pd.DataFrame()
            # try to get the prospect id for the current email
            try:
                response = get_prospect_data(api_key, item[0])
            except:
                print('Trying again...')
                # if error, wait 60 sec and try again
                time.sleep(60)
                try:
                    # try to get the prospect id
                    response = get_prospect_data(api_key, item[0])
                except:
                    # if error persists, continue for next row
                    continue
            # if the response from get_prospect_data is 'ivalid key' , set a new api key and repeat the process to obtain the id
            if response == 'invalid key':
                api_key = get_api_key()
                try:
                    response = get_prospect_data(api_key, item[0])
                except:
                    print('Trying again...')
                    time.sleep(60)
                    try:
                        response = get_prospect_data(api_key, item[0])
                    except:
                        continue
            # if the response from get_prospect_data is different from 'invalid email', get the necessary data
            if response != 'invalid email':
                prospect_id = response
                # set the prospect_id as integer
                single_df['prospect_id'] = [int(prospect_id)]
                # set the permutive id , which comes from original emails data
                single_df['permutive_id'] = [item[1]]
                # set the created date as current timestamp
                single_df['created'] = [datetime.datetime.now(pytz.utc)]
                # append the matching data to the final fataframe
                mapping_table = mapping_table.append(single_df)
        # if the mapping_Table is not empty, upload to GBQ table
        if not mapping_table.empty:
            print('Uploading dataframe with ids to GBQ.')
            mapping_table.to_gbq(destination_table=f'{dataset}.{table}',project_id = project_id, if_exists = 'append')
        # if it is empty return a message
        else:
            print('No mathing entires found.')
    return True

def update_error_log(context):
    instance = context['task_instance']
    fix_log_error()
    
def fix_log_error():
    '''
        If the dag fails save the max and min date from the downloaded dataframe 
        from permutive to avoid loosing data from one day with fails.
    '''
    emails = download_emails()
    max_date = emails.time.max()
    min_date = emails.time.min()
    with open ('/home/ingest/logs/update_permutive_pardot_b2c_match.csv', 'a') as fl:
        fl.write(f'[{str(max_date)} , {str(min_date)}]')
    return True 

default_args = {
    'owner': 'project-pulldata_permutive_pardot_b2c_match_update',
    'start_date': datetime.datetime(2021,2, 9, 21, 1, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_permutive_pardot_b2c_match_update',
     catchup=False,
     default_args=default_args,
     schedule_interval='30 1 * * *',
     ) as dag:
    
    update_ids = PythonOperator(task_id='update_ids', python_callable=collect_all_emails_ids, on_failure_callback=update_error_log)
    
update_ids