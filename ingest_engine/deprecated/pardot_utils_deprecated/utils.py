import requests
import re
import time
import pandas as pd
from datetime import datetime, timedelta
from pardot_utils.constants import PARDOT_LOGIN_URL, GET_BATCH_EMAIL_URL
from ingest_utils.constants import SUCC_UPDATE_LOG_MSG
from pandas.io.json import json_normalize
import xmltodict
import numpy as np


def get_api_key(email: str, password: str, user_key: str, url: str = PARDOT_LOGIN_URL) -> str:
    """
    Get API key
    :param email: user email
    :param password: user password
    :param user_key: user key
    :param url: Pardot login url
    :return: API key for Authorization
    """
    # json with data that need to be pass via post request
    d = {
        'email': email,
        'password': password,
        'user_key': user_key
    }
    # get response via post request
    response = requests.post(url, data=d)
    # get api from response by searching what is inside <api_key> tag
    api_key = re.findall('<api_key>(.*?)</api_key>', response.text)[0]
    return api_key


def get_batch(url: str, api_key: str, user_key: str, after_mark: datetime = None,
              before_mark: datetime = None, created_updated: str = None,
              max_id: str = None, params: bool = True) -> requests.models.Response:

    """
    Get batch of first 200 recodes
    :param url: Specific Pardot API URL
    :param api_key: api key for Authorization
    :param user_key: user key for Authorization
    :param after_mark: last date that we need to start download
    :param before_mark: end date that we need to finish download
    :param created_updated: param for created or updated data
    :param max_id: max pardot id
    :param params: True if params required
    :return: response from the request
    """

    if params:
        assert after_mark is not None, "after_mark should not be None"
        assert before_mark is not None, "before_mark should not be None"
        assert created_updated is not None, "created_updated should not be None"
        assert max_id is not None, "max_id should not be None"
        # if created use params for return new created data
        # if 'updated' use params for return new updated data
        print(created_updated)
        p = {
            'id_greater_than': max_id,
            f'{created_updated}_after': after_mark,
            f'{created_updated}_before': before_mark,
            'sort_by': 'id',
            'sort_order': 'ascending'
        }
    else:
        p = {}
    # header params for Authorization
    h = {
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Pardot user_key={user_key} ,api_key={api_key}"
    }
    # get request to return the data
    response = requests.get(url, headers=h, params=p)
    return response


def get_email_metrics(email: str, password: str, user_key: str, final_df: pd.DataFrame) -> pd.DataFrame:
    """
    Call the Pardot API to get the Email metrics using emails from already downloaded data
    :param email: user email
    :param password: user password
    :param user_key: user key
    :param final_df: already downloaded data for Emails
    :return: DataFrame with Email metrics
    """
    # for every ID from New Email Clicks
    api_key = get_api_key(email, password, user_key)
    df_full_email = pd.DataFrame()
    for list_email_id in final_df.list_email_id.unique():
        url = GET_BATCH_EMAIL_URL.format(list_email_id=list_email_id)
        response = get_batch(url, api_key, user_key, params=False)
        df = pd.DataFrame.from_dict(xmltodict.parse(response.text))
        try:
            df = json_normalize(df.loc['stats']['rsp'])
            df['id'] = list_email_id
            df_full_email = df_full_email.append(df, ignore_index=True)
        except:
            pass
    print('Emails metrics data is collected')
    return df_full_email


def get_new_created_updated_data(url: str, last_date: datetime, get_id: str,
                                 crated_updated: str, email: str,
                                 password: str, user_key: str,
                                 data_name: str,
                                 before_mark_delta: int = 24) -> pd.DataFrame:
    """
    Get all created/updated data
    :param url: Specific Pardot API URL
    :param last_date: date to start from
    :param get_id: ID to iterate the data
    :param crated_updated: if 'created' download new created, if 'updated' new updated
    :param email: user email
    :param password: user password
    :param user_key: user key
    :param data_name: name for data in the xml response
    :param before_mark_delta: timedelta for download period
    :return: all collected data (created new or updated new)
    """

    # get api_key
    global total_for_period
    api_key = get_api_key(email, password, user_key)
    # get first ID from where will start to collect the data
    max_id = get_id
    # date from were we will start to collect the data
    after_mark = last_date - timedelta(seconds=1)
    print(after_mark)
    time_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # create empty dataframe for apending the data
    final_df = pd.DataFrame()
    # set empty dataframe that will append data for the period

    while True:
        period_df = pd.DataFrame()
        # set count_total to true for first iteration
        count_total = True
        # set error counter to 0
        error_count = 0
        # set total records to 0
        total_record = 0
        # start loop to iterate to all data 200 by 200
        while True:

            # set period for 8 hours
            before_mark = after_mark + timedelta(hours=before_mark_delta)
            # try except block to handle errors because API not respond or hit limits
            try:
                # get bach of 200 records
                response = get_batch(url, api_key, user_key, after_mark, before_mark, crated_updated, max_id)

                # find error if API key expired get new API key and run again get_bach to return the data
                find_err_1 = re.findall('<err code="1">(.*?)</err>', response.text)
                if len(find_err_1) != 0:
                    if find_err_1[0] == 'Invalid API key or user key':
                        api_key = get_api_key(email, password, user_key)
                        response = get_batch(url, api_key, user_key, after_mark, before_mark, crated_updated, max_id)
                # dataframe with response
                df = pd.DataFrame.from_dict(xmltodict.parse(response.text))
                print(df)
                # if first run get the total of records
                if count_total:
                    try:
                        total_results = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])
                    except:
                        total_results = int(df.loc['result']['rsp']['total_results'])
                    # count total to false
                    count_total = False
                # try/except block because api return different results if there is no new reocords
                try:
                    total_for_period = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])
                except:
                    total_for_period = int(df.loc['result']['rsp']['total_results'])
                # if we have 0 new record return the final dataframe with all reuslts

                if total_for_period == 1:
                    df = df.loc['result']['rsp']
                    df = pd.DataFrame([df[data_name]], columns=df[data_name].keys())
                # if there is no data for update return False
                elif total_for_period == 0:
                    if datetime.strptime(before_mark.strftime('%Y-%m-%d %H:%M:%S'),
                                         '%Y-%m-%d %H:%M:%S') >= datetime.strptime(time_now, '%Y-%m-%d %H:%M:%S'):
                        return final_df
                    after_mark = after_mark + timedelta(hours=before_mark_delta - 1)
                    continue
                else:
                    df = json_normalize(pd.DataFrame.from_dict(df.loc['result']['rsp'])[data_name])

            # if error appeared wait 1 min and run try block again.
            # if there is 5 errors raise error that there is to many errors
            except:
                print("API NOT WORK! TRIED AGAIN")
                error_count += 1
                if error_count == 5:
                    raise Exception('Too many ERRORS!')
                time.sleep(60)
                continue
            # set max id to the max id in the dataframe
            max_id = df.id.max()
            print('Total_results: ' + str(total_results))
            print('total_for_period: ' + str(total_for_period))
            print(after_mark)
            print('shape:' + str(df.shape))
            # drop these IDs from final dataframe
            ids = list(df.id)

            # concatenata dataframe from current iteration to final dataframe and drop duplicates rows if any
            period_df = pd.concat([period_df, df], axis=0, ignore_index=True, sort=False)
            period_df = period_df.drop_duplicates(subset='id', keep="last")
            print('total_shape: ' + str(period_df.shape))
            # if records from final data are equal or more than total results
            # return from API we are collected all the data and break the loop
            if period_df.shape[0] >= total_results:
                break
        # If both dataframes are not empty print max and min ID to check that we collected all the data
        if not final_df.empty and not period_df.empty:
            print('Max min between periods:')
            print(final_df.id.max(), period_df.id.min())
        # concatenate dataframe from current iteration to final dataframe
        final_df = pd.concat([final_df, period_df], axis=0, ignore_index=True, sort=False)
        # drop duplicates rows if any
        final_df = final_df.drop_duplicates(subset='id', keep="last")
        if period_df.empty:
            return final_df
        if crated_updated == 'created':
            if total_for_period == 1:
                after_mark = datetime.strptime(period_df.created_at.max(),
                                               '%Y-%m-%d %H:%M:%S')
            else:
                after_mark = datetime.strptime(period_df.created_at.max(),
                                                    '%Y-%m-%d %H:%M:%S') - timedelta(seconds=1)
        else:
            if total_for_period == 1:
                after_mark = datetime.strptime(period_df.updated_at.max(),
                                               '%Y-%m-%d %H:%M:%S')
            else:
                after_mark = datetime.strptime(period_df.updated_at.max(),
                                                    '%Y-%m-%d %H:%M:%S') - timedelta(seconds=1)
        max_id = str(int(period_df.id.max()) - 1)
        if datetime.strptime(before_mark.strftime('%Y-%m-%d %H:%M:%S'),
                                      '%Y-%m-%d %H:%M:%S') >= datetime.strptime(time_now, '%Y-%m-%d %H:%M:%S'):
            return final_df


def log_update(log_file: str, ids: list, table: str) -> bool:
    """
    Write into the specified log file
    :param log_file: destination log file
    :param ids: list of processed IDs
    :param table:  specific table which already exists
    :return: True if logging is successful
    """
    with open(log_file, 'a') as fl:
        curr_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        update_ids_count = str(len(ids))
        fl.write(str(curr_time) + '|' + update_ids_count + '|' + table + f'|{SUCC_UPDATE_LOG_MSG}\n')
    print(f'Processing for table {table} logged successfully.')
    return True


def index_marks(nrows, chunk_size):
    return range(1 * chunk_size, (nrows // chunk_size + 1) * chunk_size, chunk_size)


def split(dfm, chunk_size):
    indices = index_marks(dfm.shape[0], chunk_size)
    return np.split(dfm, indices)


def integration_log(n_users: int, n_updates: int) -> bool:
    """
    Log if there are new added users or updated
    :param n_users: number of new users
    :param n_updates: number of updated users
    :return: True if logged successfully
    """
    print(f'Log integration information')
    with open('/home/ingest/logs/alchemer_pardot_integration_results.csv', 'a') as fl:
        curr_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if n_users == 0:
            fl.write(str(curr_time) + '| No new users added to Pardot data.\n')
        else:
            fl.write(str(curr_time) + '|' + str(n_users) + 'new users added to Pardot data.\n')
        if n_updates == 0:
            fl.write(str(curr_time) + '| No updates added to Pardot data.\n')
        else:
            fl.write(str(curr_time) + '| ' + str(n_updates) + ' user(s) updated in Pardot data.\n')
    return True
