#imports
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from google.oauth2 import service_account
from google.cloud import bigquery
from sqlalchemy import create_engine 

import pandas as pd
import pandas_gbq
import requests
import datetime
import pytz


def get_apple_last_date_json():
    '''
    get last date from gbq from apple website json file
    '''
    return requests.get('https://covid19-static.cdn-apple.com/covid19-mobility-data/current/v3/index.json').json()['mobilityDataVersion'].split(":",1)[1]

# dict with table names for keys and link for download for values
data_source = {
    'jhu_daily_covid_deaths_timeseries_global':'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv',
    'jhu_daily_covid_deaths_timeseries_us':'https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv?raw=true',
    'jhu_daily_covid_cases_timeseries_global':'https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv',
    'jhu_daily_covid_cases_timeseries_us':'https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv?raw=true',
    'our_world_in_data_testing_data_global':'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/testing/covid-testing-all-observations.csv',
    'google_covid19_mobility_data':'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv',
    'oxford_covid19_government_response_tracker_oxcgrt':'https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv',
    'oxford_covid19_government_response_tracker_oxcgrt_with_notes':'https://github.com/OxCGRT/covid-policy-tracker/raw/master/data/OxCGRT_latest_withnotes.csv',
    'apple_covid19_mobility_data':f'https://covid19-static.cdn-apple.com/covid19-mobility-data/current/v3/en-us/applemobilitytrends-{get_apple_last_date_json()}.csv',
    'worldwide_governance_indicators':'https://govdata360-backend.worldbank.org/api/v1/datasets/51/dump.csv',
    'excess_mortality_during_the_covid19_pandemic':'https://github.com/Financial-Times/coronavirus-excess-mortality-data/raw/master/data/ft_excess_deaths.csv'
}

def download_data(data_source):
    '''
    Input: dict with table names for keys and link for download for values
    Output: yield datafame with downloaded data from input links
    '''
    # loop through data source dict
    for table, source in data_source.items(): 
        # check if there is new data for apple_covid19_mobility_data
        if table == 'apple_covid19_mobility_data':
            if get_apple_last_date_json() == get_apple_last_date_gbq():
                print('There is no new data for apple_covid19_mobility_data')
                log_update('covid19data', table, False)
                continue
                
        # download in dataframe every csv form source url
        df = pd.read_csv(source, low_memory=False)
        # clean all columns names to be compatible with postgres and gbq column name convention
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.replace('/', '_')
        df.columns = df.columns.str.replace('-', '_')
        df.columns = df.columns.str.replace(' ', '_')
        df.columns = df.columns.str.replace('[^A-Za-z0-9_]+', '')
        df.columns = ['_' + x  if x[0].isdigit()  else x for x in df.columns]
        # fix all colums that are with name 'date' to be datetime type
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(
                df['date'].astype(str).str.replace('-', ''),
                format='%Y%m%d') 
        # yield table name and dataframe pairs
        yield table, df
        
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

def get_apple_last_date_gbq():
    '''
    get last date from gbq from apple table
    '''
    sql = '''
    SELECT column_name
    FROM `project-pulldata`.covid19data.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'apple_covid19_mobility_data'
    ORDER BY ORDINAL_POSITION DESC 
    LIMIT 1;
    '''
    df_max = pandas_gbq.read_gbq(query = sql)
    return df_max.values[0][0].replace("_", "-")[1:]

def log_update(dataset, table, apple):
    '''
        log the tables 
    '''
    with open ('/home/ingest/logs/update_covid19_processing_results.csv', 'a') as fl:
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if not apple:
            fl.write(str(curr_time) + '|' + dataset + '.' + table + '|There is no new data.\n')
        else:
            fl.write(str(curr_time) + '|' + dataset + '.' + table + '|Replaced successfully.\n')
    print(f'Processing for table {table} in dataset {dataset} logged successfully.')    
    return True

def update_data():
    project_id = "project-pulldata"
    postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
    gbq_client = get_credentials(project_id)
    
    for table, df in download_data(data_source):
        print(table)  
        # upload data to gbq
        df.to_gbq(destination_table='covid19data.'+str(table),
                  project_id = project_id, if_exists = 'replace')
        print(f'Table {table} uploaded successfully to GBQ.')
        # upload data to postgres
        df.to_sql(table,schema='covid19data', con=postgre_engine,method='multi',
                  chunksize = 100000, if_exists = 'replace')
        print(f'Table {table} uploaded successfully to Postgre.')
        # update metadata
        update_metadata(df, 'covid19data', table, postgre_engine, gbq_client, project_id)
        log_update('covid19data', table, True)
        
    return True

default_args = {
    'owner': 'project-pulldata_covid19_data_update',
    'start_date': datetime.datetime(2020, 10, 15, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_covid19_data_update',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 6 * * *',
     ) as dag:
    
    
    update_covid_data = PythonOperator(task_id='update_data',
                               python_callable=update_data)

update_covid_data
