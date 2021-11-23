from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.cloud import bigquery_storage
from datahub_updater import Updater
import numpy as np
import datetime
from sqlalchemy import create_engine 
import pytz


# set up the credentials to access the GCP database
def get_clients():
    project_id = 'project-pulldata'
    bqclient = bigquery.Client(project=project_id)
    bqstorageclient = bigquery_storage.BigQueryReadClient()
    # return authorized client connections
    return bqclient, bqstorageclient

def upload_to_gbq(df,destination_table, schema=None):
    """
    Upload df to destination_table in GBQ
    """
    bqclient, bqstorageclient = get_clients()
    print(f'Uploading {destination_table} to GBQ')
    # upload table to GBQ
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = bqclient.load_table_from_dataframe(df, destination_table, job_config=job_config)
    # Wait for the load job to complete.
    job.result()
    return True


def country_codes_download():
    # initialize connection to GBQ
    bqclient, bqstorageclient = get_clients()
    # fix domain with names that we used in GBQ 
    # set query to pull evaluation data
    query_string = f"""
        SELECT rtCode FROM `project-pulldata.ComTrade.CountryCodes`
        """
    dataframe = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )
    return dataframe

def construct_request_url(type_,freq,period,who,cat_):
    root = 'https://comtrade.un.org/api/get/bulk/'
    # type_ : commodity or service C or S
    # freq: annual - A  or monthly - M
    # period: [2019,2020] for annual, [201901,201902] for monthly
    # who: country code - take for db
    # cat_ : hs - commodities, EB02 - services 
    par_sep = '?'
    token = 'token=jqDrjAqfC0yILt101ICLkOiG/pnGLRSnGNDJYqcGeFVbPUF1ZSAW7V2JCSGj4FkeRQ0ZBfz3ki4anRdJhNBfbqlTb+Ah62Up93Zh4YXCapIy0QSXWtAQ/n8BMhjiijGRnaCxtIwAk2aCgsIdxQLIIueaXg655rpbAh8CJoeFwmyHjpq+YPTRkIKEIQcMjIp9'
    string = root+type_+freq+period+str(who)+'/'+cat_+par_sep+token
    return string

def delete_metadata(dataset, table, postgre_engine):
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
    bqclient, bqstorageclient = get_clients()
    dataset_s = "'" + dataset + "'"
    table_s = "'" + table + "'"
    # delete rows from Postgre for the specific dataset and table:
    sql_meta = f'delete from metadata."MetaData_V1" where "dataset" = {dataset_s} and "table" = {table_s}'
    postgre_engine.execute(sql_meta)
    print(f"Delete rows from MetaData.MetaData_V1 table from Postgre for table {table} in dataset {dataset}")
    # delete rows from GBQ for the specific dataset and table:
    sql_gbq_meta = (f'delete from `MetaData.MetaData_V1` where dataset = "{dataset}" and table = "{table}"')
    query_job = bqclient.query(sql_gbq_meta)  # API request
    query_job.result()  # Waits for statement to finish
    print(f"Delete rows from MetaData.MetaData_V1 table from GBQ for table {table} in dataset {dataset}")
    return True

def update_metadata(df_updated, dataset, table, postgre_engine, project_id):
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
    delete_metadata(dataset, table, postgre_engine)
    # upload metadata to Postgre
    df_meta_updated.to_sql('MetaData_V1',schema='metadata',
                      con=postgre_engine,method='multi', chunksize = 100000, if_exists = 'append')
    print(f"Metadata for dataset {dataset} and table {table} is successfully updated in Postgre")
    # upload metadata to GBQ
    upload_to_gbq(df_meta_updated,'MetaData.MetaData_V1')
    #df_meta_updated.to_gbq(destination_table='MetaData.MetaData_V1',
    #                  project_id = project_id, if_exists = 'append')
    print(f"Metadata for dataset {dataset} and table {table} is successfully updated in GBQ")
    
    return True

def download_data(year, country_id, type_period, type_):
    final_df = pd.DataFrame()
    # Commodities
    if type_ == 'C':
        cat = 'HS'
    else:
        cat = 'EB02'
    if type_period == 'A':
        # construct string for request
        string_annual_commodities = construct_request_url(f'{type_}/','A/',f'{year}/',country_id,cat)
        # get thata
        print(f'Downloading annual data for country: {country_id} and year: {year}')
        try:
            final_df = pd.read_csv(string_annual_commodities, compression='zip', low_memory=False)
        except:
            print(f'No data for country code: {country_id}')
    elif type_period == 'M':
        for month in ['01','02','03','04','05','06','07','08','09','10','11','12']:
            single_df = pd.DataFrame()
            # construct string for request
            string_annual_commodities = construct_request_url(f'{type_}/','M/',f'{year}{month}/',country_id,cat)
            # get thata
            print(f'Downloading monthly data for country: {country_id} and year: {year} and month: {month}')
            try:
                single_df = pd.read_csv(string_annual_commodities, compression='zip', low_memory=False)
            except:
                print(f'No data for country code: {country_id}')
                continue
            final_df = final_df.append(single_df)
    return final_df

def transform_and_upload(df_entities, type_, type_period):
    # set project_id
    project_id = 'project-pulldata'
    # create postgre engine
    postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
    # gbq client
    #bqclient, bqstorageclient = get_clients()
    # set table names
    if type_ == 'C' and type_period == 'A':
        table = 'CommoditiesAnnual'
        table2 = 'CommoditiesMapping_v1'
    elif type_ == 'C' and type_period == 'M':
        table = 'CommoditiesMonthly'
        table2 = 'CommoditiesMapping_v1'
    elif type_ == 'S' and type_period == 'A':
        table = 'ServicesAnnual'
        table2 = 'ServicesMapping_v1'
    elif type_ == 'S' and type_period == 'M':
        table = 'ServicesMonthly'
        table2 = 'ServicesMapping_v1'
    # transform resulting df
    if not df_entities.empty:    
        # keep only necessary columns
        single_df = df_entities[['Classification','Year','Period','Trade Flow Code','Reporter Code','Partner Code','Commodity Code', 'Trade Value (US$)']]
        #rename columns
        single_df.columns = ['pfCode','yr','period','rgCode','rtCode','ptCode','cmdCode','TradeValue']
        single_df = single_df.drop_duplicates()
        single_df = single_df[~single_df['pfCode'].isnull()]
        single_df = single_df[~single_df['pfCode'].str.contains('No data matches your query or your query is')]
        single_df['yr'] = single_df['yr'].astype('int32')
        single_df['period'] = single_df['period'].astype('int64')
        single_df['rgCode'] = single_df['rgCode'].astype('int64')
        single_df['rtCode'] = single_df['rtCode'].astype('int64')
        single_df['ptCode'] = single_df['ptCode'].astype('int64')
        # cmdCode to integer if it is Services data
        if type_ == 'S':
            single_df['cmdCode'] = single_df['cmdCode'].astype('int64')
        single_df['TradeValue'] = single_df['TradeValue'].astype('int64')
        # add last_updated_date column
        single_df['last_updated_date'] = pd.NaT
        # get mapping data
        df_mapping = df_entities[['Commodity Code','Commodity']]
        df_mapping = df_mapping.drop_duplicates(subset=['Commodity Code'],keep='last').reset_index(drop=True)
        df_mapping.columns = ['cmdCode','cmdDescE']
        df_mapping = df_mapping[~df_mapping['cmdCode'].isnull()]
        # cmdCode to integer if it is Services data
        if type_ == 'S':
            df_mapping['cmdCode'] = df_mapping['cmdCode'].astype('int64')
        # add last_updated_date column
        #df_mapping['last_updated_date'] = pd.NaT
        print('Starting to upload to postgre...')
        # upload to gbq and postgre
        single_df.to_sql(table,schema='comtrade',
                          con=postgre_engine,method='multi', chunksize = 200000, if_exists = 'append')
        print('Starting to upload to GBQ...')
        upload_to_gbq(single_df,f'ComTrade.{table}')
        #single_df.to_gbq(destination_table=f'ComTrade.{table}',chunksize = 200000,
        #                  project_id = project_id, if_exists = 'append')
        print('Uploading mapping table to GBQ and Postgre.')
        updater1 = Updater(
                    'project-pulldata', 
                    'ComTrade', table2, df_mapping,
                    'cmdCode', ['cmdCode'],
                    list(df_mapping.cmdCode)
                    )
        updater1.update_data()
        
        # update metadata
        update_metadata(single_df, 'comtrade', table, postgre_engine, project_id)
        
    #     chunks = split(single_df, 30000)
    #     for c in chunks:
    #         ids_details = list(c.rtCode)
    #         update_list = Updater('project-pulldata', 
    #                                             'ComTrade', table, c,
    #                                             'rtCode', ['pfCode','yr','period','rgCode','rtCode','ptCode','cmdCode','TradeValue'],
    #                                             ids_details)
    #         update_list.update_data()
    
    return True

def index_marks(nrows, chunk_size):
    return range(1 * chunk_size, (nrows // chunk_size + 1) * chunk_size, chunk_size)
def split(dfm, chunk_size):
    indices = index_marks(dfm.shape[0], chunk_size)
    return np.split(dfm, indices)

def get_data():
    # 1: Get country codes
    df_codes = country_codes_download()
    # 2: Set period for which we want to download data
    period = []
    # COMMODITIES ANNUAL
    print('Processing data for Commodities Annual.')
    # set type_period variable ('M' - monthly; 'A' - annual)
    type_period = 'A'
    # set the type_ variable ('C' - commodities data;  'S' - services data)
    type_ = 'C'
    # 3: perform lambda to the list of ids with the download_data  method
    for year in period:
    	list_of_df = map(lambda x: download_data(year, x, type_period, type_), list(df_codes.rtCode.values))
    	# 4:concat the list of dataframes into one final dataframe
    	df_entities = pd.concat(list_of_df)
    	# 5: Transform final dataframe to obtain the 2 datframes we need and upload them to GBQ and Postgre
    	# we need to specify the type_ and type_period in order to know to which table we want to upload
    	print('Preparing Commodities Annual data to upload.')
    	transform_and_upload(df_entities, type_, type_period)
    # COMMODITIES MONTHLY
    period = [2011]
    print('Processing data for Commodities Monthly.')
    # set type_period variable ('M' - monthly; 'A' - annual)
    type_period = 'M'
    # set the type_ variable ('C' - commodities data;  'S' - services data)
    type_ = 'C'
    # 3: perform lambda to the list of ids with the download_data  method
    for year in period:
        list_of_df = map(lambda x: download_data(year, x, type_period, type_),list(df_codes.rtCode.values))
        # 4:concat the list of dataframes into one final dataframe
        df_entities = pd.concat(list_of_df)
        # 5: Transform final dataframe to obtain the 2 datframes we need and upload them to GBQ and Postgre
        # we need to specify the type_ and type_period in order to know to which table we want to upload
        print('Preparing Commodities Monthly data to upload.')
        transform_and_upload(df_entities, type_, type_period)
    # SERVICES ANNUAL
    print('Processing data for Services Annual.')
    # set type_period variable ('M' - monthly; 'A' - annual)
    type_period = 'A'
    # set the type_ variable ('C' - commodities data;  'S' - services data)
    type_ = 'S'
    # 3: perform lambda to the list of ids with the download_data  method
    for year in period:
        list_of_df = map(lambda x: download_data(year, x, type_period, type_), list(df_codes.rtCode.values))
        # 4:concat the list of dataframes into one final dataframe
        df_entities = pd.concat(list_of_df)
        # 5: Transform final dataframe to obtain the 2 datframes we need and upload them to GBQ and Postgre
        # we need to specify the type_ and type_period in order to know to which table we want to upload
        print('Preparing Services Annual data to upload.')
        transform_and_upload(df_entities, type_, type_period)
    # SERVICES MONTHLY
    #print('Processing data for Services Monthly.')
    # set type_period variable ('M' - monthly; 'A' - annual)
    #type_period = 'M'
    # set the type_ variable ('C' - commodities data;  'S' - services data)
    #type_ = 'S'
    # 3: perform lambda to the list of ids with the download_data  method
    #for year in period:
     #   list_of_df = map(lambda x: download_data(year, x, type_period, type_), list(df_codes.rtCode.values))
        # 4:concat the list of dataframes into one final dataframe
      #  df_entities = pd.concat(list_of_df)
        # 5: Transform final dataframe to obtain the 2 datframes we need and upload them to GBQ and Postgre
        # we need to specify the type_ and type_period in order to know to which table we want to upload
       # print('Preparing Services Monthly data to upload.')
        #transform_and_upload(df_entities, type_, type_period)
    return True

default_args = {
	'owner': 'project-pulldata_comtrade_data_update',
	'start_date': datetime.datetime(2021, 1, 17, 20, 00, 00),
	'concurrency': 1,
	'retries': 0
}

with DAG('project-pulldata_comtrade_data_update',
     catchup=False,
     default_args=default_args,
     schedule_interval='@monthly',
     ) as dag:
    
    
    comtrade_update = PythonOperator(task_id='get_data',
                               python_callable=get_data)

comtrade_update