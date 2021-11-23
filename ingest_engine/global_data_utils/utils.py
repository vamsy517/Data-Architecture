import requests
import pandas as pd
from ingest_utils.logging import write_to_log
from ingest_utils.constants import NO_DATA_SKIP_REPLACE, SUCC_REPLACE_LOG_MSG, LOGS_FOLDER, \
    UPDATE_PROCESSING_RESULTS, SUCC_UPDATE_LOG_MSG, PULLDATA_PROJECT_ID
from ingest_utils.class_datahub_updater import Updater
import time
from datetime import date, datetime, timedelta
from pandas.io.json import json_normalize
from ingest_utils.database_gbq import get_gbq_table
from sql_queries.gbq_queries import QUERY_COLUMN_TYPES


def check_gd_api_token(credentials_path: str, url: str, gd_api: str) -> str:
    """
    Checks if the token is valid and if not create the new one
    :param credentials_path: Path to current token
    :param url: GD API url
    :param gd_api: GD API name
    :return: valid token
    """
    # open file with the token
    with open(credentials_path, "r") as f:
        token = f.read()
    print(f'Used token: {token}')
    url = f'{url}{token}'
    # get response from API Call
    response = requests.get(url).json()
    # check if the response is string with some message inside
    # (in case the API returns an error or no updates are present)
    if type(response) == str:
        if response == 'Incorrect Token id' or response.strip() == "Token Id Expired. Please Generate New Token Id":
            # call api to generate a new token
            url = f'http://apidata.globaldata.com/{gd_api}/api/Token?OldTokenId={token}'
            token = requests.get(url).json()
            # save the new token to a file
            with open(credentials_path, "w") as f:
                f.write(token)
    return token


def fix_date_type(df: pd.DataFrame, col_list: list) -> pd.DataFrame:
    """
    Fix datetime columns in DataFrame
    :param df: DataFrame containing the date columns
    :param col_list: list of columns, which datetime type should be changed
    :return: DataFrame with fixed date column
    """
    # copy the original dataframe
    df_fixed = df.copy()
    # for each column in the list
    for col in col_list:
        # convert the column to datetime
        df_fixed[col] = pd.to_datetime(df_fixed[col])
        # change the datetime type
        df_fixed[col] = df_fixed[col].astype('datetime64[ns, UTC]')
    return df_fixed


def fix_date(column: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace NA and '-' values in date column
    @param column: date column containing a 0001-01-01 date
    @param df: DataFrame containing the date column
    @return: DataFrame with fixed date column
    """

    for date in df[column].tolist():
        if date == 'NA':
            (df[column][df[column] == date]) = None
        if date == '-':
            (df[column][df[column] == date]) = None
    return df


def get_mnc_entity_listing_or_details(api: str, api_method: str, token: str) -> pd.DataFrame:
    """
    :param api: GetEntityListing or GetEntityDetails
    :param api_method: API method Listing or Details
    :param token: token for the api
    :return: DataFrame containing the data from GD
    """
    url = f'http://apidata.globaldata.com/NSMGAPI/api/MNC{api}/GetMNC{api}{api_method}?TokenId={token}&DisplayName=nsmedia'
    if api == 'Companies':
        url = f'http://apidata.globaldata.com/NSMGAPI/api/MNCSegments/GetMNCCompanies{api_method}?TokenId={token}&DisplayName=nsmedia'
    # get response from API Call
    response = requests.get(url).json()
    num_pages = response['TotalRecordsDetails'][0]['NoOfPages']
    # instantiate an empty dataframe
    entities_df = pd.DataFrame()
    # in a loop over the  pages of the API results, fill the dataframe with the respective resutls
    for page in range(1, num_pages + 1):
        url = f'http://apidata.globaldata.com/NSMGAPI/api/MNC{api}/GetMNC{api}{api_method}?TokenId={token}&DisplayName=nsmedia&PageNumber={page}'
        if api == 'Companies':
            url = f'http://apidata.globaldata.com/NSMGAPI/api/MNCSegments/GetMNCCompanies{api_method}?TokenId={token}&DisplayName=nsmedia&PageNumber={page}'
        # get response from API Call
        response = requests.get(url).json()
        if api_method == 'Listing':
            resp = pd.DataFrame.from_dict(response['CompanyDetails'])
            entities_df = entities_df.append(resp)
        elif api_method == 'Details':
            details = pd.DataFrame()
            resp = pd.DataFrame.from_dict(response['CompanyData'])
            details = details.append(resp)
            details = details[['CompanyID', api]][~details[api].isnull()]
            details = details.explode(api)
            details = pd.concat([details.drop([api], axis=1), details[api].apply(pd.Series)], axis=1)
            if api == 'Segments':
                details = fix_date('FilingsDate', details)
                lst_fixdate = ['CreatedDate', 'ModifiedDate', 'FilingsDate']
            else:
                details = fix_date('Filings_Date', details)
                lst_fixdate = ['MandA_Event_Announced_Date', 'CreatedDate', 'ModifiedDate', 'Filings_Date']
            details = fix_date_type(details, lst_fixdate)
            entities_df = entities_df.append(details)
    if api == 'Companies':
        entities_df = fix_date('FilingDates', entities_df)
        entities_df['RevenueYear'] = entities_df['RevenueYear'].fillna(0)
        entities_df['NoofEmp'] = entities_df['NoofEmp'].fillna(0)
        entities_df['NoofEmp'] = entities_df['NoofEmp'].astype('int32')
        entities_df['RevenueYear'] = entities_df['RevenueYear'].astype('int32')
    return entities_df


def update_data(df: pd.DataFrame, dataset: str, table: str, entity_id_col: str, ids_datapoints: list,
                log_file: str) -> bool:
    """
    If the df is empty, log the results
    Else initialize class Updater, update data and metadata
    :param df: DataFrame to upload
    :param dataset: dataset to which we upload the df
    :param table: specific table which will contain the df
    :param entity_id_col: entity indicator column
    :param ids_datapoints: datapoint id columns
    :param log_file: file to log
    :return:
    """
    # check if dataframe is empty
    if df.empty:
        print(f'There is no data for {dataset}.{table}. Skipping replacement.')
        write_to_log(f'{LOGS_FOLDER}{log_file}', dataset, table, NO_DATA_SKIP_REPLACE)
    else:
        # get list of entity_ids
        entity_ids = df[entity_id_col].tolist()

        print(f'Initialize class Updater for {dataset}.{table}')
        update_table = Updater(dataset, table, df, entity_id_col, ids_datapoints, entity_ids)

        print(f'Update {dataset}.{table}')
        update_table.update_data()
        # log updated table
        write_to_log(f'{LOGS_FOLDER}{log_file}', dataset, table, SUCC_REPLACE_LOG_MSG)
    return True


def get_economics_year_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract YearWiseData Data from provided dataframe
    :param df: dataframe, containing both details and year data for cities/countries
    :return: a dataframe containing year data for cities/countries
    """

    df_year = df['YearWiseData'].apply(json_normalize)
    # get the list of all individual dataframes
    years_list = df_year.tolist()
    # concat all the individual dataframes to a single larger dataframe, and reset its index
    years_data_df = pd.concat(years_list)
    years_data_df = years_data_df.reset_index(drop=True)
    return years_data_df


def check_column_types(df: pd.DataFrame, df_to_compare: pd.DataFrame) -> bool:
    """
    Compare columns types between two dataframes
    :param df: table with column types
    :param df_to_compare: raw data from API (year or details)
    :return: True or False if the column types are identical
    """
    # lower the column types string
    df_lower = df.data_type.str.lower()
    # convert and get types of dataframe
    df_to_compare = df_to_compare.convert_dtypes().dtypes
    df_to_compare = df_to_compare.reset_index()[0]
    df_to_compare = df_to_compare.astype(str)
    df_to_compare = df_to_compare.str.lower()
    return df_lower.equals(df_to_compare)


def log_economics_update(ids: list, table: str) -> bool:
    """
    Log the table and list of IDs, which are updated
    :param ids: list of IDs, which are updated
    :param table: specific table, which is processed
    :return: True if the logging is successful
    """
    with open (f'{LOGS_FOLDER}{UPDATE_PROCESSING_RESULTS}', 'a') as fl:
        curr_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        converted_id_list = [str(element) for element in list(ids)]
        inds_str = ",".join(list(converted_id_list))
        fl.write(str(curr_time) + '|' + inds_str + '|' + table + f'|{SUCC_UPDATE_LOG_MSG} \n')
    print(f'Processing for table {table} logged successfully.')
    return True


def download_economics_page(api: str, from_date: str, to_date: str, name: str, token: str, entity: str,
                            page: str, df_details_col: pd.DataFrame, df_year_col: pd.DataFrame) -> pd.DataFrame:
    """
    Download Economics data from specific page
    :param api: GD API,
    :param from_date: from which date updates should be displayed
    :param to_date: to which date updates should be displayed
    :param name: API display name,
    :param token: auth token,
    :param entity: City or Country
    :param page: number of downloaded page
    :param df_details_col: details df with existing in GBQ column types
    :param df_year_col: year df with existing in GBQ column types
    :return: DataFrame, containing the downloaded data from the specific page and time period
    """
    # set counter to count number of retries to download and check column types
    counter = 0
    print(f'Downloading updated {entity} data from page: {page}')
    # apply the complete API call url
    url = f'http://apidata.globaldata.com/EconomicsNSMG/api/Content/{api}?FromDate={from_date}&ToDate={to_date}&PageNumber={page}&DisplayName={name}&TokenId={token}'
    # GET the response json for entity Data
    try:
        # get the response for the specific page
        response_raw = requests.get(url)
        # check if the page contains data
        if response_raw.text != '':
            response = response_raw.json()[entity + 'Data']
        else:
            return pd.DataFrame()
        # parse the response to a pandas dataframe
        json_df = pd.DataFrame.from_dict(response)
    except:
        print(f'Sleep for 60 seconds')
        # wait 60 seconds to try get the entity data again
        time.sleep(60)
        print(f'Trying to get the updated data again for {entity} from page: {page}')
        response = requests.get(url).json()[entity + 'Data']
        # parse the response to a pandas dataframe
        json_df = pd.DataFrame.from_dict(response)
    # create dataframe with updated data for entity
    entity_df = pd.DataFrame(json_df['Indicators'][0])
    while counter < 5:
        df_details_api = entity_df.drop(['YearWiseData'], axis=1)
        df_year_api = get_economics_year_data(entity_df)
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
            counter = 5
    return entity_df

def get_economics_data(api: str, name: str, token: str, entity: str, from_date: str, to_date: str) -> pd.DataFrame:
    """
    Download data from Economics API for specific time period and entity
    :param api: GD API,
    :param name: GD API display name,
    :param token: auth token,
    :param entity: City or Country
    :param from_date: from which date updates should be displayed
    :param to_date: to which date updates should be displayed
    :return: dataframe containing updated data for entity
    """
    # create df for updated data
    df_entities = pd.DataFrame()
    # get column types from GBQ
    if entity == 'City':
        df_details_col = get_gbq_table(PULLDATA_PROJECT_ID,
                                       QUERY_COLUMN_TYPES.format(project_id=PULLDATA_PROJECT_ID,
                                                                 dataset='City', table='CityEconomicsDetails'))
        df_year_col = get_gbq_table(PULLDATA_PROJECT_ID,
                                    QUERY_COLUMN_TYPES.format(project_id=PULLDATA_PROJECT_ID,
                                                              dataset='City', table='CityYearData'))
    else:
        df_details_col = get_gbq_table(PULLDATA_PROJECT_ID,
                                       QUERY_COLUMN_TYPES.format(project_id=PULLDATA_PROJECT_ID,
                                                                 dataset='Country', table='MacroeconomicsDetails'))
        df_year_col = get_gbq_table(PULLDATA_PROJECT_ID,
                                    QUERY_COLUMN_TYPES.format(project_id=PULLDATA_PROJECT_ID,
                                                              dataset='Country', table='MacroYearData'))

    # url for API Call
    url = f'http://apidata.globaldata.com/EconomicsNSMG/api/Content/{api}?FromDate={from_date}&ToDate={to_date}&DisplayName={name}&TokenId={token}'
    # get response from API Call
    response = requests.get(url).json()
    if type(response) == str:
        print(response)
    # check if the response is a dictionary
    elif type(response) == dict:
        # check if the dictionary contains CityData or CountryData
        if (entity + "Data") in response.keys():
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
            list_of_df = map(lambda x: download_economics_page(api, from_date, to_date, name, token, entity, x,
                                                               df_details_col, df_year_col),
                             list(range(1, num_pages + 1)))
            # concatenate all downloaded pages into one dataframe
            df_entities = pd.concat(list_of_df)
        else:
            print(f'There is no key {entity}Data in the returned response.')
    else:
        print(f'The returned response {response} is in INVALID format.')
    return df_entities


def log_last_run_date_economics(log_file: str) -> bool:
    """
    Concat last performed update check date to the existing date in the log file
    :param log_file: full path to the log file
           Example: '/home/ingest/logs/update_log.csv'
    :return: True when the log is updated
    """
    with open(log_file, 'a') as fl:
        curr_time = date.today() - timedelta(days=2)
        fl.write("_" + str(curr_time))
    return True


def log_next_run_date_economics(log_file: str) -> bool:
    """
    Modify log file to keep the date for the next update check
    :param log_file: full path to the log file
           Example: '/home/ingest/logs/update_log.csv'
    :return: True when the log is updated
    """
    with open(log_file, 'r+') as fl:
        line = fl.readline()
        new_date = line.split("_")[1]
        fl.seek(0)
        fl.write(new_date)
        fl.truncate()
    return True


def log_next_run_date_economics_failure(log_file: str) -> bool:
    """
    Modify log file to keep the same date for the next update check upon failure
    :param log_file: full path to the log file
           Example: '/home/ingest/logs/update_log.csv'
    :return: True when the log is updated
    """
    with open(log_file, 'r+') as fl:
        line = fl.readline()
        new_date = line.split("_")[0]
        fl.seek(0)
        fl.write(new_date)
        fl.truncate()
    return True


