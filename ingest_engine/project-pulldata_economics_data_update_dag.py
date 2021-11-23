import pandas as pd
import numpy as np
import os
import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ingest_utils.class_datahub_updater import Updater
from ingest_utils.constants import LOGS_FOLDER, ECONOMICS_UPDATE_LOG, CREDENTIALS_FOLDER
from global_data_utils.constants import ECONOMICS_API_CREDENTIALS, ECONOMICS_API_URL, ECONOMICS_API, GD_API_DISPLAY_NAME
from global_data_utils.utils import check_gd_api_token, get_economics_data, get_economics_year_data,\
    log_economics_update, log_last_run_date_economics, log_next_run_date_economics_failure, log_next_run_date_economics


def update_economics_data() -> bool:
    """
    Detect updated data from Economics GD API and update data and metadata that needs to be updated
    :return: True if all operations are successful
    """
    # get last performed update date from log file
    from_date_df = pd.read_csv(f'{LOGS_FOLDER}{ECONOMICS_UPDATE_LOG}', header=None)
    from_date_df[0][0] = from_date_df[0][0].split("_")[0]
    from_date_unformatted = pd.to_datetime(from_date_df[0]).dt.date
    from_date = from_date_unformatted.values[0].strftime("%m/%d/%Y").replace('/', '%2F')
    # set yesterday date for API to download the data up to that day to keep one day buffer
    to_date_unformatted = datetime.datetime.now() - datetime.timedelta(1)
    to_date = to_date_unformatted.strftime("%m/%d/%Y").replace('/', '%2F')
    print(f'Check API for updated City data from {from_date} to {to_date}')
    # check and set the token
    token = check_gd_api_token(f'{CREDENTIALS_FOLDER}{ECONOMICS_API_CREDENTIALS}', ECONOMICS_API_URL, ECONOMICS_API)
    print(f'Start to download updated data for City')
    df_city = get_economics_data('GetCityEconomicsDetails', GD_API_DISPLAY_NAME, token, 'City', from_date, to_date)
    print(f'Finished download of updated data for City')
    # check if there are any City data to update
    if df_city.shape[0] > 0:
        print('Transformed City Year and Details data')
        df_details = df_city.drop(['YearWiseData'], axis=1)
        df_year = get_economics_year_data(df_city)
        df_details['ForecastDate'] = df_details['ForecastDate'].apply(lambda x: int(np.nan_to_num(x)) if x != np.nan else x)
        df_details['EndDate'] = df_details['EndDate'].apply(lambda x: int(np.nan_to_num(x)) if x != np.nan else x)
        df_details['StartDate'] = df_details['StartDate'].apply(lambda x: int(np.nan_to_num(x)) if x != np.nan else x)
        # create list with unique entity ids
        city_ids = list(set(df_year['CityID']))
        # record year data to csv:
        df_year.to_csv('/home/ingest/economics_data/df_city_year.csv')
        print('Initialize class Updater for City Year data')
        update_city_year = Updater('City', 'CityYearData', df_year, 'CityID', ['CityID', 'Indicator', 'Year'], city_ids)
        print('Update CityYearData')
        update_city_year.update_data()
        # log updated CityYearData
        # remove file if it is processed
        os.remove('/home/ingest/economics_data/df_city_year.csv')
        log_economics_update(city_ids, 'City.CityYearData')
        # create list with unique entity ids
        city_ids = list(set(df_details['CityID']))
        print('Initialize class Updater for City Details data')
        # record details data to csv:
        df_details.to_csv('/home/ingest/economics_data/df_city_details.csv')
        update_city_details = Updater('City', 'CityEconomicsDetails', df_details, 'CityID',
                                      ['CityID', 'IndicatorCategory', 'Indicator'], city_ids)
        print('Update CityEconomicsDetails')
        update_city_details.update_data()        
        # log updated CityEconomicsDetails
        log_economics_update(city_ids, 'City.CityEconomicsDetails')
        # remove file if it is processed
        os.remove('/home/ingest/economics_data/df_city_details.csv')
    else:
        print('There is no new City Data')
    # check and set the token
    token = check_gd_api_token(f'{CREDENTIALS_FOLDER}{ECONOMICS_API_CREDENTIALS}', ECONOMICS_API_URL, ECONOMICS_API)
    print(f'Check API for updated Country data from {from_date} to {to_date}')
    print(f'Start to download updated data for Country')
    df_country = get_economics_data('GetMacroeconomicsDetails', GD_API_DISPLAY_NAME, token, 'Country', from_date, to_date)
    print(f'Finished download of updated data for Country')
    if df_country.shape[0] > 0:
        # remove Venezuela
        df_country = df_country[df_country.CountryID != 23424982]
        print('Transformed Country Year and Details data')
        df_details = df_country.drop(['YearWiseData'], axis=1)
        df_year = get_economics_year_data(df_country)
        # create list with unique entity ids
        country_ids = list(set(df_year['CountryID']))
        # record year data to csv:
        df_year.to_csv('/home/ingest/economics_data/df_country_year.csv')
        print('Initialize class Updater for Country Details data')
        update_country_year = Updater('Country', 'MacroYearData', df_year, 'CountryID',
                                      ['CountryID', 'Indicator', 'Year'], country_ids)
        print('Update MacroYearData')
        update_country_year.update_data()
        # log updated MacroYearData
        log_economics_update(country_ids, 'Country.MacroYearData')
        # remove file if it is processed
        os.remove('/home/ingest/economics_data/df_country_year.csv')
        # create list with unique entity ids
        country_ids = list(set(df_details['CountryID']))
        print('Initialize class Updater for Country Year data')
        # record details data to csv:
        df_details.to_csv('/home/ingest/economics_data/df_country_details.csv')
        update_country_details = Updater('Country', 'MacroeconomicsDetails', df_details, 'CountryID',
                                         ['CountryID', 'IndicatorCategory', 'Indicator'], country_ids)
        print('Update MacroeconomicsDetails')
        update_country_details.update_data()
        # log updated MacroeconomicsDetails
        log_economics_update(country_ids, 'Country.MacroeconomicsDetails')
        # remove file if it is processed
        os.remove('/home/ingest/economics_data/df_country_details.csv')
    else:
        print('There is no new Country Data')
 
    return True


def update_error_task(context):
    instance = context['task_instance']
    log_next_run_date_economics_failure(f'{LOGS_FOLDER}{ECONOMICS_UPDATE_LOG}')


default_args = {
    'owner': 'project-pulldata_economics_data_update_v2',
    'start_date': datetime.datetime(2020, 9, 14, 21, 1, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_economics_data_update_v2',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 19 * * *',) as dag:
    
    record_log = PythonOperator(task_id='log_last_run_date_economics',
                                op_kwargs={"log_file": f'{LOGS_FOLDER}{ECONOMICS_UPDATE_LOG}'},
                                python_callable=log_last_run_date_economics)
    
    update_data = PythonOperator(task_id='update_economics_data', python_callable=update_economics_data,
                                 on_failure_callback=update_error_task)
    
    log_next_run = PythonOperator(task_id='log_next_run_date_economics',
                                  op_kwargs={"log_file": f'{LOGS_FOLDER}{ECONOMICS_UPDATE_LOG}'},
                                  python_callable=log_next_run_date_economics)

    
record_log >> update_data >> log_next_run
