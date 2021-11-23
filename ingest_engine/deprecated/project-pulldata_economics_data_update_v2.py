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
import os

def check_token():
    # open file with the token
    with open('/home/ingest/credentials/economics_api_credentials.txt', "r") as f:
        token = f.read()
    print(f'Used token: {token}')
    url = f'http://apidata.globaldata.com/EconomicsNSMG/api/Content/GetCityEconomicsListing?DisplayName=nsmedia&TokenId={token}'
    # get response from API Call
    response = requests.get(url).json()
    # check if the response is string with some message inside 
    #(in case the API returns an error or no updates are present)
    if type(response) == str:
        if response == 'Incorrect Token id' or response.strip() == "Token Id Expired. Please Generate New Token Id":
            # call api to generate a new token
            url = f'http://apidata.globaldata.com/EconomicsNSMG/api/Token?OldTokenId={token}'
            token = requests.get(url).json()
            # save the new token to a file
            with open('/home/ingest/credentials/economics_api_credentials.txt', "w") as f:
                f.write(token)
    return token


def download_page(api, from_date, to_date, name, token, entity, page,df_details_col, df_year_col):
    ''' 
        Input:  
                api: API,
                from_date: from which date updates should be displayed
                to_date: to which date updates should be displayed
                name: API display name,
                token: auth token,
                entity: City or Country
                page: number of downloaded page
                df_details_col: details df with existing in GBQ column types
                df_year_col: year df with existing in GBQ column types
                
        Output:
                return dataframe containing updated data for entity
    '''
    # set counter to count number of retries to download and check column types
    counter = 0
    print(f'Downloading updated ', entity, ' data from page: ', page)
    # apply the complete API call url
    url = f'http://apidata.globaldata.com/EconomicsNSMG/api/Content/{api}?FromDate={from_date}&ToDate={to_date}&PageNumber={page}&DisplayName={name}&TokenId={token}'
    # GET the response json for entity Data
    try:
        response = requests.get(url).json()[entity + 'Data']
        # parse the response to a pandas dataframe
        json_df = pd.DataFrame.from_dict(response)
    except:
        print(f'Sleep for 60 seconds')
        # wait 60 seconds to try get the entity data again
        time.sleep(60)
        print(f'Trying to get the updated data again for ', entity, ' from page: ', page)
        response = requests.get(url).json()[entity + 'Data']   
        # parse the response to a pandas dataframe
        json_df = pd.DataFrame.from_dict(response)
    # create dataframe with updated data for entity    
    entity_df = pd.DataFrame(json_df['Indicators'][0])
    while counter < 5:
        df_details_api = entity_df.drop(['YearWiseData'], axis = 1)
        df_year_api = get_year_data(entity_df)
        if not check_column_types(df_details_col, df_details_api) and not check_column_types(df_year_col, df_year_api):
            print(f'Data type for Details is not the same for {entity} in page {page}')
            time.sleep(60)
            response = requests.get(url).json()[entity + 'Data']
            # parse the response to a pandas dataframe
            json_df = pd.DataFrame.from_dict(response)                
            curr_df = json_normalize(json_df[entity + 'Data'])
            entity_df = pd.DataFrame(curr_df['Indicators'][0])
            counter += 1
        else: 
            counter=5
    return entity_df


def get_data(api, name, token, entity, from_date, to_date):
    ''' 
        Input:  
                api: API,
                name: API display name,
                token: auth token,
                entity: City or Country
                from_date: from which date updates should be displayed
                to_date: to which date updates should be displayed
                
        Output:
                return dataframe containing updated data for entity
    '''
    # create df for updated data
    df_entities = pd.DataFrame()
    # get column types from GBQ
    if entity == 'City':
        df_details_col = get_table_types('City', 'CityEconomicsDetails')
        df_year_col = get_table_types('City', 'CityYearData')
    else:
        df_details_col = get_table_types('Country', 'MacroeconomicsDetails')
        df_year_col = get_table_types('Country', 'MacroYearData')
        
    # url for API Call
    url = f'http://apidata.globaldata.com/EconomicsNSMG/api/Content/{api}?FromDate={from_date}&ToDate={to_date}&DisplayName={name}&TokenId={token}'
    # get response from API Call
    response = requests.get(url).json()
    if type(response) == str:
        print(response)
    # check if the response is a dictionary
    elif type(response) == dict:
        # check if the dictionary contains CityData or CountryData
        if ((entity + "Data") in response.keys()):        
            # to avoid failing when downloading data for a entity which is being updated at exactly that moment
            # try getting the number of  pages, if it is not possible wait for 60 seconds and make the call again.
            try:
                # get number of pages
                num_pages = response['TotalRecordsDetails'][0]['NoOfPages']
            except:
                print(f'Failed response: ', response)
                # wait 60 seconds to try get the entity data again
                time.sleep(60)
                # get response from API Call
                response = requests.get(url).json()
                # get number of pages
                num_pages = response['TotalRecordsDetails'][0]['NoOfPages']
            # download data from each page   
            list_of_df=map(lambda x: download_page(api, from_date, to_date, name, token, entity, x,df_details_col, df_year_col),list(range(1, num_pages + 1)))   
            # concatenate all downloaded pages into one dataframe
            df_entities = pd.concat(list_of_df)
        else:
            print(f'There is no key {entity}Data in the returned response.')
    else:
        print(f'The returned response {response} is in INVALID format.')
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

def log_update(ids, table):
    '''
        log the tables and IDs which are updated
    '''
    print(f'Log table {table}')
    with open ('/home/ingest/logs/update_processing_results.csv', 'a') as fl:
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        converted_id_list = [str(element) for element in list(ids)]
        inds_str = ",".join(list(converted_id_list))
        fl.write(str(curr_time) + '|' + inds_str + '|' + table + '|Updated successfully.\n')

def get_credentials(project_id):
    '''
    set credentials for GBQ to download needed data
    '''
    keyfile= '/home/ingest/credentials/gbq_credentials.json'
    credentials = service_account.Credentials.from_service_account_file(
    keyfile,
    )
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = project_id
    
    return True

def get_table_types(dataset, tablename):
    '''
     Input:
        dataset: specific dataset in GBQ
        tablename: specific table in GBQ
     Output:
         return table with column types for specific table
    '''
    
    sql = f'''
    select column_name, data_type
    from `project-pulldata.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    where table_name = '{tablename}' and column_name != 'last_updated_date'
    '''
    df_processed = pandas_gbq.read_gbq(query = sql)
    return df_processed

def check_column_types(df, df_to_compare):
    '''
    Input:
        df: table with column types from GBQ
        df_to_compare: raw data from API (year or details)
    Output: return True or False if the column types are identical
    '''
    # lower the colume types string
    df_lower = df.data_type.str.lower()
    # convert and get types of dataframe
    df_to_compare = df_to_compare.convert_dtypes().dtypes
    df_to_compare = df_to_compare.reset_index()[0]
    df_to_compare = df_to_compare.astype(str)
    df_to_compare = df_to_compare.str.lower()
    return df_lower.equals(df_to_compare)

###########################################################################################################
def update_data():
    '''
        Input: ID for the project in GBQ
        Detect updated data from API and update data and metadata that needs to be updated.
    '''

    # set project ID
    project_id = "project-pulldata"
    get_credentials(project_id)
    # get last performed update date from log file
    from_date_df = pd.read_csv('/home/ingest/logs/update_log.csv', header = None)
    from_date_df[0][0] = from_date_df[0][0].split("_")[0]
    from_date_unformatted = pd.to_datetime(from_date_df[0]).dt.date
    from_date = from_date_unformatted.values[0].strftime("%m/%d/%Y").replace('/', '%2F')
    # set yseterday date for API to download the data up to that day to keep one day buffer
    to_date_unformatted = datetime.datetime.now() - datetime.timedelta(1)
    to_date = to_date_unformatted.strftime("%m/%d/%Y").replace('/', '%2F')
    print(f'Check API for updated City data from {from_date} to {to_date}')
    # check and set the token
    token = check_token()
    print(f'Start to download updated data for City')
    df_city = get_data('GetCityEconomicsDetails','nsmedia',token,'City', from_date, to_date)
    print(f'Finished download of updated data for City')
    # check if there are any City data to update
    if df_city.shape[0] > 0:
        print('Transformed City Year and Details data')
        df_details = df_city.drop(['YearWiseData'], axis = 1)
        df_year = get_year_data(df_city)
        df_details['ForecastDate'] = df_details['ForecastDate'].apply(lambda x: int(np.nan_to_num(x)) if x != np.nan else x)
        df_details['EndDate'] = df_details['EndDate'].apply(lambda x: int(np.nan_to_num(x)) if x != np.nan else x)
        df_details['StartDate'] = df_details['StartDate'].apply(lambda x: int(np.nan_to_num(x)) if x != np.nan else x)
        # create list with unique entity ids
        city_ids=list(set(df_year['CityID']))
        # record year data to csv:
        df_year.to_csv('/home/ingest/economics_data/df_city_year.csv')
        print('Initialize class Updater for City Year data')
        update_city_year = datahub_updater.Updater(project_id, 
                                                'City', 'CityYearData', df_year,
                                                'CityID', ['CityID', 'Indicator', 'Year'],
                                                 city_ids)
        print('Update CityYearData')
        update_city_year.update_data()
        # log updated CityYearData
        # remove file if it is processed
        os.remove('/home/ingest/economics_data/df_city_year.csv')
        log_update(city_ids, 'City.CityYearData')
       
        
        # create list with unique entity ids
        city_ids=list(set(df_details['CityID']))
        print('Initialize class Updater for City Details data')
        # record details data to csv:
        df_details.to_csv('/home/ingest/economics_data/df_city_details.csv')
        update_city_details = datahub_updater.Updater(project_id, 
                                                'City', 'CityEconomicsDetails', df_details,
                                                'CityID', ['CityID', 'IndicatorCategory', 'Indicator' ],
                                                 city_ids)
        
        print('Update CityEconomicsDetails')
        update_city_details.update_data()        
        # log updated CityEconomicsDetails
        log_update(city_ids, 'City.CityEconomicsDetails')
        # remove file if it is processed
        os.remove('/home/ingest/economics_data/df_city_details.csv')
    else:
        print('There is no new City Data')
        
        
    # check and set the token
    token = check_token()
    print(f'Check API for updated Country data from {from_date} to {to_date}')
    print(f'Start to download updated data for Country')
    df_country = get_data('GetMacroeconomicsDetails','nsmedia',token,'Country', from_date, to_date)
    print(f'Finished download of updated data for Country')
    if df_country.shape[0] > 0:
        # remove Venezuela
        df_country = df_country[df_country.CountryID != 23424982]
        print('Transformed Country Year and Details data')
        df_details = df_country.drop(['YearWiseData'], axis = 1)
        df_year = get_year_data(df_country)
        # create list with unique entity ids
        country_ids=list(set(df_year['CountryID']))
        # record year data to csv:
        df_year.to_csv('/home/ingest/economics_data/df_country_year.csv')
        print('Initialize class Updater for Country Details data')
        update_country_year = datahub_updater.Updater(project_id,
                                                      'Country', 'MacroYearData', df_year,
                                                      'CountryID', ['CountryID', 'Indicator', 'Year'],
                                                      country_ids)
        print('Update MacroYearData')
        update_country_year.update_data()
        # log updated MacroYearData
        log_update(country_ids, 'Country.MacroYearData')
        # remove file if it is processed
        os.remove('/home/ingest/economics_data/df_country_year.csv')
        # create list with unique entity ids
        country_ids=list(set(df_details['CountryID']))
        print('Initialize class Updater for Country Year data')
        # record details data to csv:
        df_details.to_csv('/home/ingest/economics_data/df_country_details.csv')
        update_country_details = datahub_updater.Updater(project_id,
                                                         'Country', 'MacroeconomicsDetails', df_details,
                                                         'CountryID', ['CountryID', 'IndicatorCategory', 'Indicator' ],
                                                         country_ids)
        print('Update MacroeconomicsDetails')
        update_country_details.update_data()
        # log updated MacroeconomicsDetails
        log_update(country_ids, 'Country.MacroeconomicsDetails')
        # remove file if it is processed
        os.remove('/home/ingest/economics_data/df_country_details.csv')
    else:
        print('There is no new Country Data')
 
    return True

# record the date for the last performed update check
def record_log():
    with open ('/home/ingest/logs/update_log.csv', 'a') as fl:
        curr_time = datetime.date.today() - datetime.timedelta(days=2)
        fl.write("_" + str(curr_time))
    return True 

# keep next date
def fix_log():
    with open ('/home/ingest/logs/update_log.csv', 'r+') as fl:
        line = fl.readline()
        new_date = line.split("_")[1]
        fl.seek(0)
        fl.write(new_date)
        fl.truncate()
    return True 
# keep original date
def fix_log_error():
    with open ('/home/ingest/logs/update_log.csv', 'r+') as fl:
        line = fl.readline()
        new_date = line.split("_")[0]
        fl.seek(0)
        fl.write(new_date)
        fl.truncate()
    return True 
    
def update_error_task(context):
    instance = context['task_instance']
    fix_log_error()
    
default_args = {
    'owner': 'project-pulldata_economics_data_update_v2',
    'start_date': datetime.datetime(2020, 9, 14, 21, 1, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_economics_data_update_v2',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 19 * * *',
     ) as dag:
    
    record_log = PythonOperator(task_id='record_log', python_callable=record_log)    
    
    update_data = PythonOperator(task_id='update_data', python_callable=update_data, on_failure_callback=update_error_task)
    
    fix_log = PythonOperator(task_id='fix_log', python_callable=fix_log)  
    
    
record_log >> update_data >> fix_log