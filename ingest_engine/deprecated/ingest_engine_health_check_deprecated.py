import datetime
from datetime import date, timedelta
import os

import pandas_gbq
import pandas as pd
import scipy.stats as stats
import numpy as np

from google.oauth2 import service_account
from google.cloud import bigquery

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from sqlalchemy import create_engine 

import requests
import json
from pandas.io.json import json_normalize

# Start script for healthcheck --------------

def get_credentials(project_id):
    keyfile= '/home/ingest/credentials/gbq_credentials.json'
    credentials = service_account.Credentials.from_service_account_file(
        keyfile,
    )
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = project_id
    client = bigquery.Client.from_service_account_json(keyfile)
    return client

# this function takes project_id, dataset and tablename
# return a dataframe, containing the specific table from the specific dataset and project
def get_table(project_id, dataset, tablename):
    sql = 'select * from ' + project_id + '.' + dataset + '.' + tablename
    df_processed = pandas_gbq.read_gbq(query = sql)
    return df_processed

# Create a function, which returns a dataframe with the TableName, ColumnName, ColumnType and sample values from the column
# and takes TableName, DataFrameTable and Number of Samples to return
def check_table(dataset, tableName, df_to_check, nr_samples):
    # Construct an empty dataframe with the following columns:
    # Table Name, Column Name, Column Type, Sample Values
    df_types = pd.DataFrame(columns = ('Dataset', 'Table', 'Column_Name', 'Column_Type', 'Sample_Values'))
    for (columnName,columnData) in df_to_check.iteritems():
        sample_values_list = df_to_check[columnName].sample(n=nr_samples).to_list()
        new_row = pd.Series({'Dataset' : dataset,
                             'Table' : tableName ,
                             'Column_Name' : columnName,
                             'Column_Type' : df_to_check.dtypes[columnName],
                             'Sample_Values' : "|".join([str(elem) for elem in sample_values_list])})
        df_types=df_types.append(new_row,ignore_index = True)
    return df_types

# this function takes the dataframe, containing a specific table from BigQuery and the summary dataframe, 
# containing type for each column and sample results
# and calculates coverage of all numeric columns in a dataframe
def calculate_coverage(df_table, df_types):
    # create a copy of the summary df
    df_coverage = df_types.copy()
    # create a new column for Coverage
    df_coverage['Coverage'] = 0.0
#     # get a list of all numeric columns
#     numeric_columns = df_table.select_dtypes([np.number]).columns
    # for each numeric column in the table
    for col in df_table.columns:
        # calculate coverage by dividing number of non-null values to number of all values
        # NOTE: Postgre not working with numpy
        coverage = df_table[col].count()/df_table[col].shape[0]
        # get the index of the column in the summary dataframe
        index = df_types.Column_Name[df_types.Column_Name == col].index[0]
        # set the calculated coverage in the summary table
        df_coverage.loc[index,'Coverage'] = coverage.item()
    return df_coverage
    
# this function takes a dataframe, specific column and number of samples to return
# calculates Z-Score for the specific column in the dataframe 
# returns a list of TOP number of samples tuples(value : corressponding Z-Score)
def calculate_z_score_column(df, column, nr_samples):
    # create a dataframe with the specific column
    df_new = pd.DataFrame(df[column], columns=[column])
    # calculate Z-Score for the specific column
    df_new['Z'] = abs(stats.zscore(df[column], nan_policy = 'omit'))
    # sort the new dataframe by Z-Score
    df_new.sort_values(by=['Z'], inplace=True, ascending = False)
    lst = []
    # for each row from TOP number of samples
    for index in df_new.head(nr_samples).index:
        # append value : Z-Score to a list
        lst.append( str(df_new.loc[index, column]) + ' : ' + str(df_new.loc[index, 'Z']))
    return lst

# this function takes the dataframe, containing a specific table from BigQuery and the summary dataframe, 
# containing type for each column and sample results
# and calculates Z-Score of all numeric columns in a dataframe
def calculate_z_score_table(df_table, df_types):
    # create a copy of the summary df
    df_z_score = df_types.copy()
    # create a new column for Coverage
    df_z_score['Z_Score'] = np.nan
    # get a list of all numeric columns
    numeric_columns = df_table.select_dtypes([np.number]).columns
    # for each numeric column in the table
    for col in numeric_columns:
        # get the TOP 3 tuples (value : Z-Score)
        z_score_list = calculate_z_score_column(df_table, col, 2)
        # get the index of the column in the summary dataframe
        index = df_types.Column_Name[df_types.Column_Name == col].index[0]
        # set the calculated coverage in the summary table
        df_z_score.loc[index,'Z_Score'] = "  |  ".join(z_score_list)
    return df_z_score

# this is a function which gets project_id, dataset and table
# returns a new summary dataframe, which contains tablename, column name, column type, sample values for each column,
# coverage score and Z-Score for all numeric columns
def perform_health_check(project_id, dataset, table):
    
    # set credentials for BigQuery
    get_credentials(project_id)
    # get the table, which will be checked
    df = get_table(project_id, dataset, table)
    # check if the downloaded table is empty
    if df.shape[0]==0:
        # create empty dataframe to return
        df_result = pd.DataFrame()
    else:
        # create a summary table for the specific table
        df_types = check_table(dataset,table,df, 3)
        # calculate coverage for all numeric columns
        df_coverage = calculate_coverage(df, df_types)
        # calculate Z-Score for all numeric columns
        df_result = calculate_z_score_table(df,df_coverage)
    return df_result

# End sctipt for healcheck ------------------

# Chech if there is new data and return array with unique dataset.table names:
def read_log():
    # read health check log to find the last health check
    df_time = pd.read_csv('/home/ingest/logs/health_check_log.csv', header = None)
    df_time[0]= pd.to_datetime(df_time[0]).dt.date
    date = df_time.iloc[0,0]
    # list all log file in ingest logs folder
    logs_dir = os.listdir('/home/ingest/logs/')
    # keep those logs, which should be processed by the healthcheck
    healthcheck_file_list = [list_item for list_item in logs_dir if list_item.endswith('processing_results.csv')]
    # create new df to store the final results
    df_tocheck = pd.DataFrame()
    # for each file
    for file in healthcheck_file_list:
        # read the file with | separator
        df = pd.read_csv('/home/ingest/logs/' + file, header = None, sep='|')
        # for the log file from commercial update, remove the unnecessary column
        if ('update_processing_results.csv' in file):
            df=df.drop(1, axis=1)
        # rename all columns
        df.columns = ['Timestamp', 'Filename', 'Message']
        # convert Timestamp string to Timestamp object
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], infer_datetime_format=True)
        # get date from Timestamp object
        df['Date'] = df['Timestamp'].dt.date
        # keep only valid entries
        df_new = df[(df['Date']>date) & (df['Message'].str.endswith('successfully.'))]
        # concat all into one df
        df_tocheck = pd.concat([df_tocheck, df_new])
    if df_tocheck.shape[0] == 0:
        return False
    else:
        # return unique list of tables
        return df_tocheck.Filename.unique() 
    
# record the date for the last performed health check
def record_log():
    with open ('/home/ingest/logs/health_check_log.csv', 'w') as fl:
        curr_time = date.today() - timedelta(days=1)
        fl.write(str(curr_time))
    return True 

def delete_healthcheck_data(dataset, table, postgre_engine, gbq_client):
    ''' 
        Delete data for specific dataset and table from Healthcheck table in Postgre and BigQuery
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
    sql_meta = f'delete from metadata."HealthCheck" where "Dataset" = {dataset_s} and "Table" = {table_s}'
    postgre_engine.execute(sql_meta)
    print(f"Delete rows from MetaData.HealthCheck table from Postgre for table {table} in dataset {dataset}")
    # delete rows from GBQ for the specific dataset and table:
    sql_gbq_meta = (f'delete from `MetaData.HealthCheck` where Dataset = "{dataset}" and Table = "{table}"')
    query_job = gbq_client.query(sql_gbq_meta)  # API request
    query_job.result()  # Waits for statement to finish
    print(f"Delete rows from MetaData.HealthCheck table from GBQ for table {table} in dataset {dataset}")
    return True

# perform health check and upload the data to gbq
def input_data_health_check():
    project_id = 'project-pulldata'
    # set credentials for BigQuery and Postgre
    gbq_client = get_credentials(project_id)
    postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
    # check if there is new data
    # TODO: DO NOT READ FROM LOGS
    if read_log() is not False: 
        # iterate over dataset with new files from processing_results, perform health check and append result
        for item in read_log():
            # strip spaces before and after item
            item = item.strip()
            dataset, table = item.split('.')
            # get list of all datasets in GBQ
            sql_dataset = 'SELECT * FROM INFORMATION_SCHEMA.SCHEMATA'
            dataset_list = pd.read_gbq(sql_dataset)['schema_name'].tolist()
            if dataset in dataset_list:
                # get list of all tables from GBQ
                sql = 'SELECT * FROM '+dataset+'.INFORMATION_SCHEMA.TABLES' 
                tables_list = pd.read_gbq(sql)['table_name'].tolist()
                if table in tables_list:
                    print(f'Start Health Check on {dataset}.{table}')
                    # perform healthcheck fpor the specific table
                    df_health_check = perform_health_check(project_id, dataset, table)
                    # check if the results are empty, due to emtpy table in BigQuery
                    if df_health_check.shape[0]>0:
                        df_health_check['Column_Type'] = df_health_check['Column_Type'].astype(str)
                        df_health_check = df_health_check.astype('object')
                        print(f'End Health Check on {dataset}.{table}') 
                        # delete old data from healthcheck table that is updated
                        delete_healthcheck_data(dataset, table, postgre_engine, gbq_client)
                        print(f'Delete data for {dataset}.{table} from HealthCheck table') 
                        # upload and append the data to gbq HealthCheck table   
                        df_health_check.to_gbq(destination_table='MetaData.HealthCheck', project_id = project_id, if_exists = 'append')
                        print(f'Upload data for {dataset}.{table} to GBQ HealthCheck table')
                        # upload and append the data to Postgre HealthCheck table 
                        df_health_check.to_sql('HealthCheck',schema='metadata',
                                  con=postgre_engine,method='multi', chunksize = 100000, if_exists = 'append')
                        print(f'Upload data for {dataset}.{table} to Postgre HealthCheck table')
                    else:
                        print(f'Table {dataset}.{table} is empty. Skipping Health Check.') 
                else:
                    print(f'There is no table {table} in dataset {dataset} in GBQ')
            else:
                print(f'There is no dataset {dataset} in GBQ')
                
        return True
    print('There are no new tables.')
    return True


default_args = {
    'owner': 'ingest_engine_input_data_health_check',
    'start_date': datetime.datetime(2020, 8, 30, 5, 1, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('Input_data_health_check',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 5 * * *',
     ) as dag:
    
    
    check_input = PythonOperator(task_id='input_data_health_check',
                               python_callable=input_data_health_check)
    
    record_log = PythonOperator(task_id='record_log', python_callable=record_log)

check_input >> record_log