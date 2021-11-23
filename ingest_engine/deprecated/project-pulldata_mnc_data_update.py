import pandas_gbq
import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import datetime
import time
import pytz
from google.oauth2 import service_account
from google.cloud import bigquery
from sqlalchemy import create_engine 
import requests
import json
from pandas.io.json import json_normalize
import datahub_updater
import numpy as np

def check_token():
    # open file with the token
    with open('/home/ingest/credentials/mnc_api_credentials.txt', "r") as f:
        token = f.read()
    print(token[:-1])
    url = f'http://apidata.globaldata.com/NSMGAPI/api/MNCSegments/GetMNCSegmentsListing?DisplayName=nsmedia&TokenId={token[:-1]}%3D'
    # get response from API Call
    response = requests.get(url).json()
    # check if the response is string with some message inside 
    #(in case the API returns an error or no updates are present)
    if type(response) == str:
        if response == 'Incorrect Token id' or response.strip() == "Token Id Expired. Please Generate New Token Id":
            # call api to generate a new token
            url = f'http://apidata.globaldata.com/NSMGAPI/api/Token?OldTokenId={token}'
            token = requests.get(url).json()
            print('New token: ', token)
            # save the new token to a file
            with open('/home/ingest/credentials/mnc_api_credentials.txt', "w") as f:
                f.write(token)
    return token[:-1]

def fix_datetype(df, col_list):
    ''' 
        Input:  
                df: dataframe containing the date columns
                col_list: list of columns, which datetime type should be changed 
                
        Output:
                return dataframe with fixed date column
    '''
    # copy the original dataframe
    df_fixed = df.copy()
    # for each column in the list
    for col in col_list:
        # convert the column to datetime
        df_fixed[col] = pd.to_datetime(df_fixed[col])
        # change the datetime type
        df_fixed[col] = df_fixed[col].astype('datetime64[ns, UTC]')
    return df_fixed

def fixDate(column, df):
    ''' 
        Input:  
                column: date column containing a 0001-01-01 date
                df: dataframe containing the date column
        Output:
                return dataframe with fixed date column
    '''
    for date in df[column].tolist():
        if (date == 'NA') :
            (df[column][df[column] == date]) = None 
        if (date == '-') :
            (df[column][df[column] == date]) = None 
    return df

def getEntityListingOrDetails(api, api_method, token):
    ''' 
        Input:  
                api: GetEntityListing or GetEntityDetails
                token: token for the api
        Output:
                return dataframe containing the entity listing or the entity details
    '''
    page = 1
    url = f'http://apidata.globaldata.com/NSMGAPI/api/MNC{api}/GetMNC{api}{api_method}?TokenId={token}%3D&DisplayName=nsmedia'
    if api == 'Companies':
        url = f'http://apidata.globaldata.com/NSMGAPI/api/MNCSegments/GetMNCCompanies{api_method}?TokenId={token}%3D&DisplayName=nsmedia'
    # get response from API Call
    response = requests.get(url).json()
    num_pages = response['TotalRecordsDetails'][0]['NoOfPages']
    # instantiate an empty dataframe
    entities_df = pd.DataFrame()
    # in a loop over the  pages of the API results, fill the dataframe with the respective resutls
    for page in range(1, num_pages + 1):
        url = f'http://apidata.globaldata.com/NSMGAPI/api/MNC{api}/GetMNC{api}{api_method}?TokenId={token}%3D&DisplayName=nsmedia&PageNumber={page}'
        if api == 'Companies':
            url = f'http://apidata.globaldata.com/NSMGAPI/api/MNCSegments/GetMNCCompanies{api_method}?TokenId={token}%3D&DisplayName=nsmedia&PageNumber={page}'        # get response from API Call
        response = requests.get(url).json()
        if api_method == 'Listing':
            resp = pd.DataFrame.from_dict(response['CompanyDetails'])
            entities_df = entities_df.append(resp)
        elif api_method == 'Details':
            details = pd.DataFrame()
            resp = pd.DataFrame.from_dict(response['CompanyData'])
            details = details.append(resp)
            details = details[['CompanyID',api]][~details[api].isnull()]
            details = details.explode(api)
            details = pd.concat([details.drop([api], axis=1), details[api].apply(pd.Series)], axis=1)
            if api == 'Segments':
                details = fixDate('FilingsDate', details)
                lst_fixdate = ['CreatedDate','ModifiedDate','FilingsDate']
            else:
                details = fixDate('Filings_Date', details)
                lst_fixdate = ['MandA_Event_Announced_Date','CreatedDate','ModifiedDate','Filings_Date']
            details = fix_datetype(details, lst_fixdate)
            entities_df = entities_df.append(details)
    if api == 'Companies':
        entities_df = fixDate('FilingDates', entities_df)
        entities_df['RevenueYear'] = entities_df['RevenueYear'].fillna(0)
        entities_df['NoofEmp'] = entities_df['NoofEmp'].fillna(0)
        entities_df['NoofEmp'] = entities_df['NoofEmp'].astype('int32')
        entities_df['RevenueYear'] = entities_df['RevenueYear'].astype('int32')
    return entities_df

def log_update(dataset, table, process):
    '''
        log the tables 
    '''
    with open ('/home/ingest/logs/update_mnc_processing_results.csv', 'a') as fl:
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if process:
            fl.write(str(curr_time) + '|' + dataset + '.' + table + '|Replaced successfully.\n')
        else:
            fl.write(str(curr_time) + '|' + dataset + '.' + table + '|There is no data, skipping replacement.\n')
    print(f'Processing for table {table} in dataset {dataset} logged successfully.')    
    return True

def iter_by_group(df, column, chunk_size):
    ''' 
        Input:  
                df: df to split into chunks
                column: name of the column used to group by the records into chunks
                chunk_size: size of each chunk
                return each chunk
    '''
    chunk = pd.DataFrame()
    for _, group in df.groupby(column):
        if (chunk.shape[0] + group.shape[0]) > chunk_size:
            yield chunk
            chunk = group
        else:
            chunk = pd.concat([chunk, group])
    if not chunk.empty:
        yield chunk

def update_data(df, dataset, table, project_id, entity_id_col, ids_datapoints):
    ''' 
        If the df is empty, log the results
        Else initialize class Updater, update data and metadata
        Input:
            df: dataframe to upload
            dataset: dataset to which we upload the df
            table: specific table which will contain the df
            project_id: project ID    
            entity_id_col: entity indicator column
            ids_datapoints: datapoint id columns
        Output:
            return True if all operations are successfull
    '''
    # check if dataframe is empty
    if df.empty:
        print(f'There is no data for {dataset}.{table}. Skipping replacement.')
        log_update(dataset, table, False)
    else:
        # get list of entity_ids
        entity_ids = df[entity_id_col].tolist()
        
        print(f'Initialize class Updater for {dataset}.{table}')
        update_table = datahub_updater.Updater(project_id, 
                                                dataset, table, df,
                                                entity_id_col, ids_datapoints,
                                                 entity_ids)
        print(f'Update {dataset}.{table}')
        update_table.update_data()
        # log updated table
        log_update(dataset, table, True)
    return True

def process_data():
    '''
        Download full MNC data from GD API, transform nested columns, 
        update tables and metadata in both Postgre and GBQ via class Updater.
    '''
    # set project ID
    project_id = "project-pulldata"
    # Download company data from API
    # set the token id
    token = check_token()
    # set dataset
    dataset = 'MNC'
    # get MNC Companies Listing
    print(f'Start to download full data for MNC Companies Listing')
    df_listing_companies = getEntityListingOrDetails('Companies', 'Listing', token)
    print(f'Finished download of full data for MNC Companies Listing')
    #fix as on date column type
    df_listing_companies = fix_datetype(df_listing_companies, ['As_On_Date'])
    # update data for Companies Listing
    update_data(df_listing_companies, dataset, 'MNCCompaniesListing', project_id, 'CompanyID', ['CompanyID'])
    # get MNC Segments listing
    print(f'Start to download full data for MNC Segments')
    df_listing_segments = getEntityListingOrDetails('Segments', 'Listing', token)
    print(f'Finished download of full data for MNC Segments')
    # update data for Segments Listing
    update_data(df_listing_segments, dataset, 'MNCSegmentsListing', project_id, 'CompanyID', ['CompanyID'])
    # get MNC Segments details
    print(f'Start to download full data for MNC Segments details')
    df_details_segments = getEntityListingOrDetails('Segments', 'Details', token)
    print(f'Finished download of full data for MNC Segments details')
    # update data for Segments Details
    update_data(df_details_segments, dataset, 'MNCSegmentsDetails', project_id, 'CompanyID', ['CompanyID','SegmentType','SegmentTitle','Year','Value_Millions'])
    
    # get MNC Subsidiaries listing
    print(f'Start to download full data for MNC Subsidiaries')
    df_listing_subsidiaries = getEntityListingOrDetails('Subsidiaries', 'Listing', token)
    print(f'Finished download of full data for MNC Subsidiaries')
    # update data for Subsidiaries Listing
    update_data(df_listing_subsidiaries, dataset, 'MNCSubsidiariesListing', project_id, 'CompanyID', ['CompanyID'])
    # get MNC Subsidiaries details
    print(f'Start to download full data for MNC Subsidiaries details')
    df_details_subsidiaries = getEntityListingOrDetails('Subsidiaries', 'Details', token)
    print(f'Finished download of full data for MNC Subsidiaries details')
    # split final data in chunks and run updater class on every chunk
    # we split the data because is to big and can't be uploaded at once
    for i,c in enumerate(iter_by_group(df_details_subsidiaries, 'CompanyID', 30000)):
        print('Processing chunk: ', i)
        update_data(c, dataset, 'MNCSubsidiariesDetails', project_id, 'CompanyID', ['CompanyID','URN'])
    return True

default_args = {
    'owner': 'project-pulldata_mnc_data_update',
    'start_date': datetime.datetime(2021, 1, 10, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_mnc_data_update',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 21 * * *',
     ) as dag:
    
    
    mnc_update = PythonOperator(task_id='process_data',
                               python_callable=process_data)

mnc_update