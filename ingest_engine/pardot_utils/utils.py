import requests
import re
import time
import pandas as pd
from datetime import datetime, timedelta
from pardot_utils.constants import PARDOT_CLIENT_ID, PARDOT_CLIENT_SECRET, PARDOT_USERNAME, PARDOT_PASSWORD, \
    PARDOT_BUSINESS_UNIT_ID, GET_BATCH_EMAIL_URL, PARDOT_LOGIN_URL

from ingest_utils.constants import SUCC_UPDATE_LOG_MSG
from pandas.io.json import json_normalize
import xmltodict
import numpy as np


def get_api_key(url: str = PARDOT_LOGIN_URL) -> str:
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
        "grant_type": "password",
        "client_id": PARDOT_CLIENT_ID,
        "client_secret": PARDOT_CLIENT_SECRET,
        "username": PARDOT_USERNAME,
        "password": PARDOT_PASSWORD,

    }
    # get response via post request
    response = requests.post(url, data=d)
    # get api from response by searching what is inside <api_key> tag
    api_key = response.json()['access_token']
    return api_key


def get_batch(url: str, api_key: str, after_mark: datetime = None,
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
        'Authorization': f'Bearer {api_key}',
        'Pardot-Business-Unit-Id': PARDOT_BUSINESS_UNIT_ID,
    }
    # get request to return the data
    response = requests.get(url, headers=h, params=p)
    return response


def get_email_metrics(final_df: pd.DataFrame, counter_25: int) -> (pd.DataFrame, int):
    """
    Call the Pardot API to get the Email metrics using emails from already downloaded data
    :param final_df: already downloaded data for Emails
    :param counter_25: contains the number of api calls
    :return: DataFrame with Email metrics and api calls counter
    """
    # for every ID from New Email Clicks
    api_key = get_api_key()
    df_full_email = pd.DataFrame()
    lst_emails = final_df.list_email_id.unique()
    final_df.to_csv('final_df_emails.csv')
    len_lst = len(lst_emails)
    for i, list_email_id in enumerate(lst_emails):
        if counter_25 < 25000:
            print(f'Processing {i} email_id: {list_email_id} (out of {len_lst})')
            url = GET_BATCH_EMAIL_URL.format(list_email_id=list_email_id)
            print(url)
            response = get_batch(url, api_key, params=False)
            counter_25 = counter_25 + 1
            df = pd.DataFrame.from_dict(xmltodict.parse(response.text))
            try:
                df = json_normalize(df.loc['stats']['rsp'])
                df['id'] = list_email_id
                df_full_email = df_full_email.append(df, ignore_index=True)
            except:
                pass
        else:
            print('Reached daily 25k calls limit.')
            break
    print('Emails metrics data is collected')
    return df_full_email, counter_25


def get_new_created_updated_data(url: str, last_date: datetime,
                                 crated_updated: str,
                                 data_name: str,
                                 endtime: datetime,
                                 counter_25: int,
                                 before_mark_delta: int = 24,
                                 max_calls: int = 25000
                                 ) -> pd.DataFrame:
    """
    Get all created/updated data
    :param url: Specific Pardot API URL
    :param last_date: date to start from
    :param crated_updated: if 'created' download new created, if 'updated' new updated
    :param data_name: name for data in the xml response
    :param endtime: time to end the download
    :param counter_25: daily api calls limit counter
    :param before_mark_delta: timedelta for download period
    :param max_calls: max API calls
    :return: all collected data (created new or updated new) and updated counter
    """
    # get api_key
    api_key = get_api_key()
    # date from were we will start to collect the data
    after_mark = last_date - timedelta(seconds=1)
    print(after_mark)
    # create empty dataframe for apending the data
    final_df = pd.DataFrame()
#     global final_df
    while True and counter_25 <= max_calls:
        # set empty dataframe that will append data for the period
        period_df = pd.DataFrame()
        # set count_total to true for first iteration
        count_total = True
        # set error counter to 0
        error_count = 0
        # set total records to 0
        total_record = 0
        # max ID set to 10
        max_id = 10
        # start loop to iterate to all data 200 by 200
        while True and counter_25 <= max_calls:
            print('max_id: ', max_id)
            print('25k limit: ', counter_25)
            # set period for 8 hours
            before_mark = after_mark + timedelta(hours=before_mark_delta)
            if before_mark > endtime:
                before_mark = endtime
                if after_mark >= before_mark:
                    print('Before mark reached prospects last date.')
                    break
            # try except block to handle errors because API not respond or hit limits
            print('before_mark: ', before_mark)
            print('after_mark: ', after_mark)
            try:
                # get bach of 200 records
                response = get_batch(url, api_key, after_mark, before_mark, crated_updated, max_id)
                counter_25 = counter_25 + 1
                # find error if API key expired get new API key and run again get_bach to return the data
                find_err_1 = re.findall('<err code="1">(.*?)</err>', response.text)
                if len(find_err_1) != 0:
                    if find_err_1[0] == 'Invalid API key or user key':
                        api_key = get_api_key()
                        response = get_batch(url, api_key, after_mark, before_mark, crated_updated, max_id)
                        counter_25 = counter_25 + 1
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
                                         '%Y-%m-%d %H:%M:%S') >= datetime.strptime(endtime.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S'):
                        return final_df, counter_25
                    else:
                        df = pd.DataFrame()
                else:
                    df = json_normalize(pd.DataFrame.from_dict(df.loc['result']['rsp'])[data_name])
            # if error appeared wait 1 min and run try block again.
            # if there is 5 errors raise error that there is to many errors
            except:
                print("API NOT WORK! TRIED AGAIN")
                error_count += 1
                if error_count == 20:
                    raise Exception('Too many ERRORS!')
                time.sleep(60)
                continue
            # set max id to the max id in the dataframe
            df = df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
            print('Total_results: ' + str(total_results))
            print('total_for_period: ' + str(total_for_period))
            print('After mark: ', after_mark)
            print('shape:' + str(df.shape))
            # concatenata dataframe from current iteration to final dataframe and drop duplicates rows if any
            period_df = pd.concat([period_df, df], axis=0, ignore_index=True, sort=False)
            print('total_shape: ' + str(period_df.shape))
            if period_df.shape[0] != 0:
                max_id = period_df.id.max()
                # check if df batch is empty if it is break the loop
                if df.shape[0] == 0:
                    break
#             if records from final data are equal or more than total results
#             return from API we are collected all the data and break the loop
            if period_df.shape[0] == total_results:
                break
        # If both dataframes are not empty print max and min ID to check that we collected all the data
        if not final_df.empty and not period_df.empty:
            print('Max min between periods:')
            print(final_df.id.max(), period_df.id.min())
        # concatenate dataframe from current iteration to final dataframe
        final_df = pd.concat([final_df, period_df], axis=0, ignore_index=True, sort=False)
        # drop duplicates rows if any
        final_df = final_df.drop_duplicates(subset='id', keep="last")
        if final_df.empty or period_df.empty:
            after_mark = before_mark
        else:
            if crated_updated == 'created':
                after_mark = datetime.strptime(final_df.created_at.max(),
                                             '%Y-%m-%d %H:%M:%S')
            else:
                after_mark = datetime.strptime(final_df.updated_at.max(),
                                             '%Y-%m-%d %H:%M:%S')                                        
        if datetime.strptime(before_mark.strftime('%Y-%m-%d %H:%M:%S'),
                                      '%Y-%m-%d %H:%M:%S') >= datetime.strptime(endtime.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S'):
            return final_df, counter_25
    return final_df, counter_25


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


def slice_per(source, step):
    return [source[i::step] for i in range(step)]


def get_batch_visits(url, api_key, max_id, ids_sub):
    '''
    Get bach of first 200 recodrs
    Args:
        api_key: api key for Authorization
        max_id: id from where we continue download
        ids_sub: Prospect IDs
    Output: first 200 records
    '''

    # if created use params for return new created data

    p = {
        'prospect_ids': ids_sub,
        'format': 'bulk',
        'id_greater_than': max_id,

        'sort_by': 'id',
        'sort_order': 'ascending'
    }

    # header params for Authorization
    h = {
        'Authorization': f'Bearer {api_key}',
        'Pardot-Business-Unit-Id': '0Uv0c0000008OMqCAM',
    }
    # get request to retur the data
    response = requests.get(url, headers=h, params=p)
    return response


def get_new_created_data_visits(url, get_id, ids_sub, data_name):
    '''
    Get all created/updated data
    Args:
        :param get_id: ID to interate the data
        :param ids_sub: Prospect IDs
        :param url: Specific Pardot API URL
        :param data_name: name for data in the xml response

    Output:
        return all collected data (creaded new or updated new)
    '''
    # get api_key
    api_key = get_api_key()
    # get first ID from where will start to collect the data
    max_id = str(int(get_id) - 1)
    # date from were we will start to collect the data

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
        #         try:
        # get bach of 200 records
        response = get_batch_visits(url, api_key, max_id, ids_sub)
        # find error if API key expired get new API key and run again get_bach to return the data
        print(response)
        totalll = re.findall("<total_results>(.*?)</total_results>", response.text)

        if totalll[0] == '0':
            print('Total results 0')
            return final_df
        if int(totalll[0]) > 199:

            print('>200 Resulsts')

            slice_id = slice_per(ids_sub, 25)
            df_all_1 = pd.DataFrame()
            for ids in slice_id:
                response = get_batch_visits(url, api_key, max_id, ids_sub)
                totalll = re.findall('<total_results>(.*?)</total_results>', response.text)
                if totalll[0] == '0':
                    print('Total results 0')
                    continue
                    # dataframe with response
                find_err_1 = re.findall('<err code="1">(.*?)</err>', response.text)
                if len(find_err_1) != 0:
                    if find_err_1[0] == 'Invalid API key or user key':
                        api_key = get_api_key()
                        response = get_batch_visits(url, api_key, max_id, ids)
                df = pd.DataFrame.from_dict(xmltodict.parse(response.text))
                print(df)
                # if first run get the total of records
                if count_total:
                    # try/except block because api return different results if there is no new reocords
                    try:
                        total_results = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])
                    except:
                        total_results = int(df.loc['result']['rsp']['total_results'])
                # total for the iteration
                # try/except block because api return different results if there is no new reocords
                try:
                    total_for_period = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])
                except:
                    total_for_period = int(df.loc['result']['rsp']['total_results'])
                print(total_for_period)
                # record the dataframe with the needed data
                # if total for period is 1 or more there is different output so we use different code to get the data in dataframe

                if total_for_period == 1:
                    df = df.loc['result']['rsp']
                    df = pd.DataFrame([df[data_name]], columns=df[data_name].keys())
                # if there is no data for update return False
                elif total_for_period == 0:
                    return False
                else:
                    df = json_normalize(pd.DataFrame.from_dict(df.loc['result']['rsp'])['visit'])
                df_all_1 = pd.concat([df_all_1, df], axis=0, ignore_index=True, sort=False)
            df = df_all_1
            print("shape of > 200 df: ", df.shape)

        else:
            find_err_1 = re.findall('<err code="1">(.*?)</err>', response.text)
            if len(find_err_1) != 0:
                if find_err_1[0] == 'Invalid API key or user key':
                    api_key = get_api_key()
                    response = get_batch(url, api_key, max_id, ids_sub)
            # dataframe with response
            df = pd.DataFrame.from_dict(xmltodict.parse(response.text))
            print(df)
            # if first run get the total of records
            if count_total:
                # try/except block because api return different results if there is no new reocords
                try:
                    total_results = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])
                except:
                    total_results = int(df.loc['result']['rsp']['total_results'])
            # total for the iteration
            # try/except block because api return different results if there is no new reocords
            try:
                total_for_period = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])
            except:
                total_for_period = int(df.loc['result']['rsp']['total_results'])
            print(total_for_period)
            # record the dataframe with the needed data
            # if total for period is 1 or more there is different output so we use different code to get the data in dataframe

            if total_for_period == 1:
                df = df.loc['result']['rsp']
                df = pd.DataFrame([df[data_name]], columns=df[data_name].keys())
            # if there is no data for update return False
            elif total_for_period == 0:
                return False
            else:
                df = json_normalize(pd.DataFrame.from_dict(df.loc['result']['rsp'])[data_name])
        #    if error appeared wait 1 min and run try block again. if there is 5 errors raise error that there is to many errors
        #         except:
        #             print("API NOT WORK! TRIED AGAIN")
        #             print(response.text)
        #             error_count += 1
        #             if error_count == 15:
        #                 raise Exception('Too many ERRORS!')
        #             time.sleep(60)
        #             continue
        #      set max id to the max id in the dataframe so we can iterate from that ID for the next batch of 200
        max_id = df.id.max()

        print('Total_results: ' + str(total_results))
        print('total_for_period: ' + str(total_for_period))
        print('shape:' + str(df.shape))
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


def unnest_page_views(df):
    def fix_cols(row):
        row_df = json_normalize(row['visitor_page_views'])
        value1 = row_df['visitor_page_view.id'].values[0]
        row['visitor_page_views.visitor_page_view.id'] = row_df['visitor_page_view.id'].values[0]
        row['visitor_page_views.visitor_page_view.url'] = row_df['visitor_page_view.url'].values[0]
        row['visitor_page_views.visitor_page_view.title'] = row_df['visitor_page_view.title'].values[0]
        row['visitor_page_views.visitor_page_view.created_at'] = row_df['visitor_page_view.created_at'].values[0]

    df_1 = df[df.visitor_page_view_count == '1']
    df_1_without_none = df_1[~df_1['visitor_page_views.visitor_page_view.id'].isnull()]
    df_1_without_none = df_1_without_none.drop(['visitor_page_views.visitor_page_view'], axis=1)
    df_1_none = df_1[df_1['visitor_page_views.visitor_page_view.id'].isna()]
    df_1_none_2 = df_1_none.apply(lambda x: fix_cols(x), axis=1)
    df_1_none = df_1_none.drop(['visitor_page_views.visitor_page_view'], axis=1)
    df_all_1 = pd.concat([df_1_without_none, df_1_none], axis=0, ignore_index=True, sort=False)
    df_many = df[df.visitor_page_view_count != '1']
    df_many_visitor_page_views = df_many[~df_many['visitor_page_views.visitor_page_view'].isnull()]
    # json_normalize(pd.DataFrame.from_dict(df.loc['result']['rsp'])['visit'])
    df = pd.DataFrame()
    for index, row in df_many_visitor_page_views.iterrows():
        some_df = pd.DataFrame()
        df_2 = pd.DataFrame.from_dict(row['visitor_page_views.visitor_page_view'])
        df_2['view_id'] = row['id']

        df = pd.concat([df, df_2], axis=0, ignore_index=True, sort=False)
    # for row in :
    #     df = pd.concat([df, pd.DataFrame.from_dict(row)], axis=0, ignore_index=True, sort=False)
    df_merged2 = pd.merge(df_many_visitor_page_views, df, left_on='id', right_on='view_id', how='left')
    df_merged2 = df_merged2.drop(['visitor_page_views.visitor_page_view.id',
                                  'visitor_page_views.visitor_page_view.url',
                                  'visitor_page_views.visitor_page_view.title',
                                  'visitor_page_views.visitor_page_view.created_at',
                                  'visitor_page_views.visitor_page_view',
                                  'created_at_y',
                                  'view_id',
                                  ], axis=1)
    df_merged2 = df_merged2.rename(columns={'id_x': 'id',
                                            'created_at_x': 'created_at',
                                            'id_y': 'visitor_page_views.visitor_page_view.id',
                                            'url': 'visitor_page_views.visitor_page_view.url',
                                            'title': 'visitor_page_views.visitor_page_view.title',
                                            }, inplace=False)
    df_all = pd.concat([df_all_1, df_merged2], axis=0, ignore_index=True, sort=False)
    return df_all
