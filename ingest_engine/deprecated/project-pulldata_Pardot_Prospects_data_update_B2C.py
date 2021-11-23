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
    url = 'https://pi.pardot.com/api/prospect/version/4/do/query'
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
        "Authorization": f"Pardot user_key=a3a3f52c58d97ad56d8baa4a483bab09 ,api_key={api_key}"
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
                total_results = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])    
                
            # total for the iteration
            total_for_period = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])
            # record the dataframe with the needed data
            df = json_normalize(pd.DataFrame.from_dict(df.loc['result']['rsp'])['prospect'])
        # if error appeared wait 1 min and run try block again. if there is 5 errors raise error that there is to many errors
        except:
            print("API NOT WORK! TRIED AGAIN")
            error_count += 1
            if error_count == 5:
                raise Exception('Too many ERRORS!')
            time.sleep(60)
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
        final_df = final_df.drop_duplicates(subset='id', keep="last")
        print('total_shape: ' + str(final_df.shape))
        # if records from final data are equal or more than total results return from API we are collected all the data and break the loop
        
        if final_df.shape[0] >= total_results:
            break
    # return all collected data
    return final_df

def get_table(project_id, dataset, tablename):
    '''
    Get data from GBQ
    Input:
        project_id: id of project
        dataset: name of dataset
        tablename: name of table
    Output:
        dataframe with the data from GBQ
    '''
    sql = 'select * from ' + project_id + '.' + dataset + '.' + tablename
    df_processed = pandas_gbq.read_gbq(query = sql)
    return df_processed

def log_update(ids_update, ids_new, table):
    '''
        log the tables and IDs which are updated
    '''
    print(f'Log table {table}')
    with open ('/home/ingest/logs/update_prospects_results_b2c.csv', 'a') as fl:
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_ids_count = str(len(ids_update))
        fl.write(str(curr_time) + '|' + update_ids_count + '|' + table + '|Updated successfully.\n')
        new_ids_count = str(len(ids_new))
        fl.write(str(curr_time) + '|' + new_ids_count + '|New uploaded successfully.\n')
        
def delete_data(table, postgre_engine, gbq_client):
    #delete Postgres:
    sql = f'DROP TABLE pardotb2c."{table}"'
    postgre_engine.execute(sql)
    print(f"Table {table} deleted from Postgre")
    #delete GBQ:
    sql = f'DROP TABLE PardotB2C.`{table}`'        
    query_job = gbq_client.query(sql)  # API request
    query_job.result()  # Waits for statement to finish
    print(f"Table {table} deleted from GBQ")
    return True

def delete_metadata(dataset, table, postgre_engine, gbq_client):
    ''' 
        Delete data for specific dataset and table from Metadata table in Postgre and BigQuery
        Input:
            dataset: specific dataset
            table: specific table
            postgre_engine: connection to Postgre
            gbq_client: connection to GBQ
        Output:
            return True if the data for specific dataset and table from Metadata table in Postgre and GBQ is deleted
    '''
    dataset_s = "'" + dataset + "'"
    table_s = "'" + table + "'"
    # delete rows from Postgre for the specific dataset and table:
    sql_meta = f'delete from metadata."MetaData_V1" where "dataset" = {dataset_s} and "table" = {table_s}'
    postgre_engine.execute(sql_meta)
    print(f"Delete rows from MetaData.MetaData_V1 table from Postgre for table {table} in dataset {dataset}")
    # delete rows from GBQ for the specific dataset and table:
    sql_gbq_meta = (f'delete from `MetaData.MetaData_V1` where dataset = "{dataset}" and table = "{table}"')
    query_job = gbq_client.query(sql_gbq_meta)  # API request
    query_job.result()  # Waits for statement to finish
    print(f"Delete rows from MetaData.MetaData_V1 table from GBQ for table {table} in dataset {dataset}")
    return True

def update_metadata(df_updated, dataset, table, postgre_engine, gbq_client, project_id):
    ''' 
        Update data for specific dataset and table from Metadata table in Postgre and BigQuery
        Input:
            df_updated: dataframe, containing the new data
            dataset: specific dataset
            table: specific table
            postgre_engine: connection to Postgre
            gbq_client: connection to GBQ
            project_id: Project ID in GBQ
        Output:
            return True if the data for specific dataset and table from Metadata table in Postgre and GBQ is updated
    '''
    # sql statement, which selects everything from the Metadata table in GBQ
    sql = "select * from `project-pulldata.MetaData.MetaData_V1`"
    # create a dataframe, containing the metadata table
    df_meta = pandas_gbq.read_gbq(query = sql)
    # create a dataframe, containing data only for the specific dataset table
    df_selected = df_meta[(df_meta['dataset'] == dataset) & (df_meta['table'] == table)]
    # get set of deleted columns
    deleted_columns = set(df_selected['column']) - set(df_updated.columns)
    # create a copy of the dataframe
    df_meta_updated = df_selected.copy()
    # for each deleted column
    for column in deleted_columns:
        # update deleted column
        df_meta_updated.loc[df_meta_updated['column'] == column, 'deleted'] = True
    # update update_date with current date
    df_meta_updated[['update_date']] = datetime.datetime.now(pytz.utc)
    # check if there are any new columns in the updated data
    new_columns = set(df_updated.columns) - set(df_meta_updated['column'])
    # for each new column
    for column in new_columns:
        # create a new row for the new column
        df_new_row = pd.DataFrame({'dataset':[dataset], 'table':[table], 
                                   'column' :[column],
                                   'create_date' : [datetime.datetime.now(pytz.utc)],
                                   'update_date' : [datetime.datetime.now(pytz.utc)],
                                   'deleted' : [False]})
        df_meta_updated = pd.concat([df_meta_updated,df_new_row])
    # reset the index
    df_meta_updated.reset_index()
    # delete the current metadata for the specific dataset and table
    delete_metadata(dataset, table, postgre_engine, gbq_client)
    # upload metadata to Postgre
    df_meta_updated.to_sql('MetaData_V1',schema='metadata',
                      con=postgre_engine,method='multi', chunksize = 100000, if_exists = 'append')
    print(f"Metadata for dataset {dataset} and table {table} is successfully updated in Postgre")
    # upload metadata to GBQ
    df_meta_updated.to_gbq(destination_table='MetaData.MetaData_V1',
                      project_id = project_id, if_exists = 'append')
    print(f"Metadata for dataset {dataset} and table {table} is successfully updated in GBQ")
    
    return True

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
    # get old table from GBQ
    old_df = get_table(project_id, 'PardotB2C', 'Prospects')

    # get new created data
    new_created_df = get_new_created_data(old_df.created_at.max(), str(int(old_df.id.max()) - 1), 'created')
    new_created_df = clean_cols(new_created_df)
    new_created_df = new_created_df.apply(lambda x : pd.to_numeric(x, errors='ignore'))
    # drop sensetive data
    new_created_df = new_created_df.drop(['salutation','first_name','last_name','email','password','phone','fax', 'address_one', 'address_two'], axis = 1)  
    try:
        new_created_df = new_created_df.drop(['mobile_phone'], axis = 1)
    except:
        pass
    # cols with date data for convert to datetime
    date_cols = ['created_at','updated_at','crm_last_sync',
         'last_activity_at',
         'last_activityvisitor_activitycreated_at',
         'last_activityvisitor_activityupdated_at']
    # convert to datetime
    new_created_df[date_cols] = new_created_df[date_cols].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S', errors='ignore', utc=True)
    # get new created IDs
    ids_new = list(new_created_df.id)
    # concat the data with new created record to old data from database and drop duplicates 
    final_df = pd.concat([old_df, new_created_df], axis=0, ignore_index=True, sort=False)
    final_df = final_df.loc[final_df.astype(str).drop_duplicates().index]
    print(final_df.shape)
    # get new updated data
    new_updated_df = get_new_created_data(old_df.updated_at.max(), str(int(old_df.id.min()) - 1), 'updated')
    new_updated_df = clean_cols(new_updated_df)
    new_updated_df = new_updated_df.apply(lambda x : pd.to_numeric(x, errors='ignore'))
    new_updated_df = new_updated_df.drop(['salutation','first_name','last_name','email','password','phone','fax', 'address_one', 'address_two'], axis = 1)
    try:
        new_updated_df = new_updated_df.drop(['mobile_phone'], axis = 1)
    except:
        pass
    # convert to datetime
    new_updated_df[date_cols] = new_updated_df[date_cols].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S', errors='ignore', utc=True)
    # get new updated IDs that are not new created too
    ids_update = list(new_updated_df.id)
    # drop these IDs from final dataframe
    new_created_df = new_created_df[~new_created_df.id.isin(ids_update)]
    # concat new updated data from API with dataframe with final data and drop duplicates
    final_df = pd.concat([new_created_df, new_updated_df], axis=0, ignore_index=True, sort=False)
    final_df = clean_cols(final_df)
    final_df = final_df.drop_duplicates(subset='id', keep="last")
    final_df = final_df.loc[:,~final_df.columns.duplicated()]
    # fix all numeric values
    final_df = final_df.apply(lambda x : pd.to_numeric(x, errors='ignore'))
    final_df[date_cols] = final_df[date_cols].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S', errors='ignore', utc=True)
    # delete dataframes from memory
    del new_created_df
    del new_updated_df
    #del old_df
    print('All updated and created data is collected')
    # delete the data from GBQ and postgre
    delete_data('Prospects', postgre_engine, gbq_client)
    # upload data to gbq
    final_df.to_gbq(destination_table='PardotB2C.Prospects',
              project_id = project_id, if_exists = 'append')
    print('Table Prospects uploaded successfully to GBQ.')
    # upload data to postgres
    final_df.to_sql('Prospects',schema='pardotb2c', con=postgre_engine,method='multi',
              chunksize = 10000, if_exists = 'append')
    print('Table Prospects uploaded successfully to Postgre.')
    # update metadata
    update_metadata(final_df, 'PardotB2C', 'Prospects', postgre_engine, gbq_client, project_id) 
    # record log file
    log_update(ids_update, ids_new, 'Prospects')
    print('Metadata is updated and log is recorded')
    
    return True

default_args = {
    'owner': 'project-pulldata_Pardot_Prospects_data_update_B2C',
    'start_date': datetime.datetime(2020, 11, 8, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_Pardot_Prospects_data_update_B2C',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 18 * * *',
     ) as dag:
    
    
    update_pardot = PythonOperator(task_id='upload_data',
                               python_callable=upload_data)

update_pardot