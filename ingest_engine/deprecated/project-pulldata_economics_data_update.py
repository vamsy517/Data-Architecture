import pandas_gbq
import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import datetime
import pytz
from google.oauth2 import service_account
from google.cloud import bigquery
from sqlalchemy import create_engine 
import requests
import json
from pandas.io.json import json_normalize


def check_api_for_updates(api, name, token, fromDate, entity):
    ''' 
        Input:  api: API,
                name: API display name,
                token: auth token,
                fromDate: from which date updates should be displayed
                entity: City or Country
        Output:
                return distinct values of updated IDs
    '''
    # create empty list for updated city IDs
    updated_ids = []
    # url for API Call
    url = f'http://apidata.globaldata.com/EconomicsNSMG/api/Content/{api}?FromDate={fromDate}&PageNumber=1&DisplayName={name}&TokenId={token}%3D&api_key={token}%3D'
    # get response from API Call
    response = requests.get(url).json()
    # check if the response is string with some message inside 
    #(in case the API returns an error or no updates are present)
    if type(response) == str:
        print(response)
    # check if the response is a dictionary
    elif type(response) == dict:
        # check if the dictionary contains CityData or CountryData
        if ((entity + "Data") in response.keys()):
            # get number of pages
            num_pages = response['TotalRecordsDetails'][0]['NoOfPages']
            # in a loop over the  pages of the API results
            for page in range(1, num_pages + 1):
                # apply the complete API call url
                url = f'http://apidata.globaldata.com/EconomicsNSMG/api/Content/{api}?FromDate={fromDate}&PageNumber={page}&DisplayName={name}&TokenId={token}%3D&api_key={token}%3D'
                # GET the response json for CityData
                data_list = requests.get(url).json()[entity + 'Data']
                # iterate for each city in the city data list
                for item in data_list:
                    # append city ID
                    updated_ids.append(item[entity + 'ID'])
        else:
            print(f'There is no key {entity}Data in the returned response.')
    else:
        print(f'The returned response {response} is in INVALID format.')
    return set(updated_ids)
    
def get_data(list_of_ids, api, name, token, entity):
    ''' 
        Input:  
                list_of_ids: list of ID of entity
                api: API,
                name: API display name,
                token: auth token,
                entity: City or Country
        Output:
                return dataframe containing all data for all IDs from list_of_ids
    '''

    # create df for all data from updated list
    df_entities = pd.DataFrame()
    for ID in list_of_ids:
        print(f'Processing {entity} with ID: {ID}') 
        # url for API Call
        url = f'http://apidata.globaldata.com/EconomicsNSMG/api/Content/{api}?{entity}ID={ID}&PageNumber=1&DisplayName={name}&TokenId={token}%3D&api_key={token}%3D'
        # get response from API Call
        response = requests.get(url).json()
        # get number of pages
        num_pages = response['TotalRecordsDetails'][0]['NoOfPages']
        entity_df = pd.DataFrame()
        # in a loop over the  pages of the API results
        for page in range(1, num_pages + 1):
            # apply the complete API call url
            url = f'http://apidata.globaldata.com/EconomicsNSMG/api/Content/{api}?{entity}ID={ID}&PageNumber={page}&DisplayName={name}&TokenId={token}%3D&api_key={token}%3D'
            # GET the response json for entity Data
            response = requests.get(url).json()
            # parse the response to a pandas dataframe
            json_df = pd.DataFrame.from_dict(response)
            curr_df = json_normalize(json_df[entity + 'Data'])
            indicator_df = pd.DataFrame(curr_df['Indicators'][0])
            entity_df = pd.concat([entity_df, indicator_df])
        # concatenate the city dataframe to the cities dataframe
        df_entities = pd.concat([df_entities, entity_df]) 
    return df_entities

def get_year_data(df):
    ''' 
        Input:  
                dataframe, containing both details and year data for cities/countries
                
        Output:
                return a dataframe containing year data for cities/countires
    '''
    df_year = df['YearWiseData'].apply(json_normalize)
    # get the list of all individual dataframes
    years_list = df_year.tolist()
    # concat all the individual dataframes to a single larger dataframe, and reset its index
    years_data_df = pd.concat(years_list)
    years_data_df = years_data_df.reset_index(drop = True)
    return years_data_df

# get credentials for GBQ and return GBQ client for bigquery

def get_credentials(project_id):
    keyfile='/home/ingest/credentials/gbq_credentials.json'
    credentials = service_account.Credentials.from_service_account_file(
        keyfile,
    )
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = project_id
    
    client = bigquery.Client.from_service_account_json(keyfile)
    return client

def delete_data(id_list, entity, postgre_engine, gbq_client):
    '''
        Delete data from City/Country tables for specific list of IDs
        Input:
            id_list: list with updated IDs
            entity: City or Country
            postgre_engine: connection to postgree
            gbq_client: connection to gbq
            
        Output:
            return True if data is deleted
    '''
    # convert set of IDs to string
    converted_id_list = [str(element) for element in list(id_list)]
    inds_str = ",".join(list(converted_id_list))
    # delete rows from postgre for the input id_list:
    if entity == "City":
        sql_details = f'delete from {entity}."{entity}EconomicsDetails" where "{entity}ID" in ({inds_str})'
        sql_year = f'delete from {entity}."{entity}YearData" where "{entity}ID" in ({inds_str})'
    else:
        sql_details = f'delete from {entity}."MacroeconomicsDetails" where "{entity}ID" in ({inds_str})'
        sql_year = f'delete from {entity}."MacroYearData" where "{entity}ID" in ({inds_str})'        
    postgre_engine.execute(sql_details)
    print("Delete rows from Details data table from Postgre")
    postgre_engine.execute(sql_year)
    print("Delete rows from Year data table from Postgre")
    # delete rows from GBQ for the input id_list:
    if entity == "City":
        sql_details_gbq = (f'delete from `{entity}.{entity}EconomicsDetails` where {entity}ID in ({inds_str})')
        sql_year_gbq = (f'delete from `{entity}.{entity}YearData` where {entity}ID in ({inds_str})')
    else:
        sql_details_gbq = (f'delete from `{entity}.MacroeconomicsDetails` where {entity}ID in ({inds_str})')
        sql_year_gbq = (f'delete from `{entity}.MacroYearData` where {entity}ID in ({inds_str})')        
    query_job = gbq_client.query(sql_details_gbq)  # API request
    query_job.result()  # Waits for statement to finish
    print("Delete rows from Details data table from GBQ")
    query_job = gbq_client.query(sql_year_gbq)  # API request
    query_job.result()  # Waits for statement to finish
    print("Delete rows from Year data table from GBQ")    
    return True

def upload_data(dataframe, entity, postgre_engine, project_id):
    ''' 
        Upload years and details data for City/Country to database (postgre/GBQ)
        Input:
            dataframe: dataframe to upload
            entity: City or Country
            postgre_engine: connection to Postgre
            project_id: project ID
        Output:
            return Details dataframe and Year data dataframe
    '''
    # create details and year tables from dataframe with all data
    df_details = dataframe.drop(['IndicatorsPathCode', 'YearWiseData'], axis = 1)
    df_year = get_year_data(dataframe)
    # upload details and year tables to postgre with lower chunksize to avoid bottleneck
    if entity == "City":
        df_details.to_sql(f'CityEconomicsDetails',schema=entity.lower(),
                          con=postgre_engine,method='multi', chunksize = 100000, if_exists = 'append')
        print(f"Details table for {entity} is successfully updated in Postgre")
        df_year.to_sql(f'CityYearData',schema=entity.lower(),
                          con=postgre_engine,method='multi', chunksize = 100000, if_exists = 'append')
        print(f"Year table for {entity} is successfully updated in Postgre")
    else:
        df_details.to_sql(f'MacroeconomicsDetails',schema=entity.lower(),
                          con=postgre_engine,method='multi', chunksize = 100000, if_exists = 'append')
        print(f"Details table for {entity} is successfully updated in Postgre")
        df_year.to_sql(f'MacroYearData',schema=entity.lower(),
                          con=postgre_engine,method='multi', chunksize = 100000, if_exists = 'append')
        print(f"Year table for {entity} is successfully updated in Postgre")
        
    # upload details and year tables to GBQ
    if entity == "City":
        df_details.to_gbq(destination_table=f'{entity}.CityEconomicsDetails',
                          project_id = project_id, if_exists = 'append') 
        print(f"Details table for {entity} is successfully updated in GBQ")
        df_year.to_gbq(destination_table=f'{entity}.CityYearData',
                       project_id = project_id, if_exists = 'append')  
        print(f"Year table for {entity} is successfully updated in GBQ")
    else:
        df_details.to_gbq(destination_table=f'{entity}.MacroeconomicsDetails',
                          project_id = project_id, if_exists = 'append') 
        print(f"Details table for {entity} is successfully updated in GBQ")
        df_year.to_gbq(destination_table=f'{entity}.MacroYearData',
                       project_id = project_id, if_exists = 'append')  
        print(f"Year table for {entity} is successfully updated in GBQ")
    return df_details, df_year

def log_update(ids, table):
    '''
        log the tables and IDs which are updated
    '''
    with open ('/home/ingest/logs/update_processing_results.csv', 'a') as fl:
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        converted_id_list = [str(element) for element in list(ids)]
        inds_str = ",".join(list(converted_id_list))
        fl.write(str(curr_time) + '|' + inds_str + '|' + table + '|Updated successfully.\n')
        
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
            df_updated: dataframe, containing the updated data
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
    '''
        Input: ID for the project in GBQ
        Detect updated data from API and update data and metadata that needs to be updated.
    '''
    
    # set project ID
    project_id = "project-pulldata"
    # create connection to postgree and GBQ
    # NOTE!!! ALSO instantiate clients / engines closer to when needed - connections terminated otherwise
    postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
    gbq_client = get_credentials(project_id)
    # get last performed update date from log file
    from_date_df = pd.read_csv('/home/ingest/logs/update_log.csv', header = None)
    from_date_df[0][0] = from_date_df[0][0].split("_")[0]
    from_date_unformatted = pd.to_datetime(from_date_df[0]).dt.date
    from_date = from_date_unformatted.values[0].strftime("%m/%d/%Y").replace('/', '%2F')
    print(f'Check API for updated City data from {from_date}')
    # get list of updated City/Country IDs
    city_ids = check_api_for_updates('GetCityeconomicsDetails','nsmedia', 'F5B59B09E77E41078ADB',from_date,'City')
    city_ids = []
    # check if there are any City data to update
    if len(city_ids) > 0:
        # get COMPLETE City data for updated IDs
        # results with 'from_date' do not represent full data for the entity;
        # we elect to get full data for the entity in a separate API call and perform delete > append,
        # rather than get only the data that is updated and do a cell-by-cell update
        print(f'Start to download full data for City')
        df_city = get_data(city_ids, 'GetCityEconomicsDetails','nsmedia','F5B59B09E77E41078ADB','City')
        print(f'Finished download of full data for City')
        # create connection to postgree and GBQ
        # NOTE!!! ALSO instantiate clients / engines closer to when needed - connections terminated otherwise
        postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
        gbq_client = get_credentials(project_id)
        # delete existing data for updated IDs
        delete_data(city_ids, 'City', postgre_engine, gbq_client)
        # upload complete data for updated IDs and set details and year dataframes
        df_details , df_year = upload_data(df_city, 'City', postgre_engine, project_id)
        # update the metadata for City and City Details table
        update_metadata(df_details, 'City', 'CityEconomicsDetails', postgre_engine, gbq_client, project_id)
        # update the metadata for City and City Year table
        update_metadata(df_year, 'City', 'CityYearData', postgre_engine, gbq_client, project_id)
        # log updated IDs and Tables
        log_update(city_ids, 'City.CityYearData')
        log_update(city_ids, 'City.CityEconomicsDetails')
    else:
        print('There is no new City Data')
    print(f'Check API for updated Country data from {from_date}')
    country_ids_download=check_api_for_updates('GetMacroeconomicsDetails','nsmedia','F5B59B09E77E41078ADB',from_date,'Country')
    # TODO: remove Venezuela from list
    country_ids = [item for item in country_ids_download if item != 23424982]
    country_ids = []
    # check if there are any Country data to update    
    if len(country_ids) > 0:
        # get COMPLETE Country data for updated IDs
        # results with 'from_date' do not represent full data for the entity;
        # we elect to get full data for the entity in a separate API call and perform delete > append,
        # rather than get only the data that is updated and do a cell-by-cell update
        print(f'Start to download full data for Country')
        df_country = get_data(country_ids, 'GetMacroeconomicsDetails','nsmedia','F5B59B09E77E41078ADB','Country')
        print(f'Finished download of full data for Country')
        # create connection to postgree and GBQ
        # NOTE!!! ALSO instantiate clients / engines closer to when needed - connections terminated otherwise
        postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
        gbq_client = get_credentials(project_id)
        # delete existing data for updated IDs
        delete_data(country_ids, 'Country', postgre_engine, gbq_client)
        # upload complete data for updated IDs and set details and year dataframes
        df_details , df_year = upload_data(df_country, 'Country', postgre_engine, project_id)
        # update the metadata for Country and Country Details table
        update_metadata(df_details, 'Country', 'MacroeconomicsDetails', postgre_engine, gbq_client, project_id)
        # update the metadata for Country and Country Year table
        update_metadata(df_year, 'Country', 'MacroYearData', postgre_engine, gbq_client, project_id)
        # log updated IDs and Tables
        log_update(country_ids, 'Country.MacroYearData')
        log_update(country_ids, 'Country.MacroeconomicsDetails')
    else:
        print('There is no new Country Data')        
        
    return True

# record the date for the last performed update check
def record_log():
    with open ('/home/ingest/logs/update_log.csv', 'a') as fl:
        curr_time = datetime.date.today() - datetime.timedelta(days=1)
        fl.write("_" + str(curr_time))
    return True 

# remove suffix from date
def fix_log():
    with open ('/home/ingest/logs/update_log.csv', 'r+') as fl:
        line = fl.readline()
        new_date = line.split("_")[1]
        fl.seek(0)
        fl.write(new_date)
        fl.truncate()
    return True 



default_args = {
    'owner': 'project-pulldata_economics_data_update',
    'start_date': datetime.datetime(2020, 9, 14, 21, 1, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_economics_data_update',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 19 * * *',
     ) as dag:
    
    record_log = PythonOperator(task_id='record_log', python_callable=record_log)    
    
    update_data = PythonOperator(task_id='update_data', python_callable=update_data)
    
    fix_log = PythonOperator(task_id='fix_log', python_callable=fix_log)  
    
    
record_log >> update_data >> fix_log