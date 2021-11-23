#imports
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

from google.oauth2 import service_account
from google.cloud import bigquery
from sqlalchemy import create_engine 

import pandas as pd
import pandas_gbq
import requests
import json
from pandas.io.json import json_normalize
import datetime
import pytz

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

def update_data():
    df = pd.read_csv('https://docs.google.com/spreadsheets/d/111A0ToYthfN_aHC5AUplvRctB0seanTII82rZtFDsH8/export?gid=0&format=csv')
    df.rename(columns={'Campaign name': 'campaign_name', 'URL': 'url'}, inplace=True)
    project_id = "project-pulldata"
    postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
    gbq_client = get_credentials(project_id)
    df.to_gbq(destination_table='CampaignTracker.CampaignsList_DailyUpdated',
                  project_id = project_id, if_exists = 'replace')
    update_metadata(df, 'CampaignTracker', 'CampaignsList_DailyUpdated', postgre_engine, gbq_client, project_id)
    return True

default_args = {
    'owner': 'project-pulldata',
    'start_date': datetime.datetime(2020, 10, 15, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_campaignslist_dailyupdate',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 4 * * *',
     ) as dag:
    
    
    update_covid_data = PythonOperator(task_id='update_data',
                               python_callable=update_data)

update_covid_data