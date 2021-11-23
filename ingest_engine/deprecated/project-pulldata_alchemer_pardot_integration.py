from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

from pandas.io.json import json_normalize
import requests
import numpy as np
import json
import xmltodict
import re
import pandas as pd
import requests
import pandas_gbq
from google.oauth2 import service_account
from google.cloud import bigquery
from pandas.io.json import json_normalize
from bs4 import BeautifulSoup
from collections import defaultdict 
from sqlalchemy import create_engine
import datetime
import pytz
import time

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

def log_update(n_users,n_updates):
    '''
        log if there are new added users or updated 
    '''
    print(f'Log integration information')
    with open ('/home/ingest/logs/alchemer_pardot_integration_results.csv', 'a') as fl:
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if n_users == 0:
            fl.write(str(curr_time) + '| No new users added to Pardot data.\n')
        else:
            fl.write(str(curr_time) + '|' + str(n_users) + 'new users added to Pardot data.\n')
        if n_updates == 0:
            fl.write(str(curr_time) + '| No updates added to Pardot data.\n')
        else:
            fl.write(str(curr_time) + '| ' + str(n_updates) + ' user(s) updated in Pardot data.\n')
            

def get_last_time(gbq_client, project_id):
    '''
    Get data from GBQ
    Input:
        project_id: id of project
        dataset: name of dataset
        tablename: name of table
    Output:
        max date
    '''
    
    sql = 'select max(date_submitted) as max_date from ' + project_id + '.AlchemerPardotIntegration.ResponsesDescription'
    df_max = pandas_gbq.read_gbq(query = sql)
    date_time = (df_max.max()).values[0]
    a = pd.to_datetime(date_time)
    b = a.tz_localize('utc')
    return b

def get_api_key():
    '''
    Get API key
    Return : API key for Authorization
    '''
    # API url for login
    url = 'https://pi.pardot.com/api/login/version/4'
    # json with data that need to be pass via post request
    d = {
        'email':'leads@verdict.co.uk',
        'password':'Leads@123',
        'user_key':'bfe54a8f04e0a7d4239d5bcd90e79cbd'
    }
    #get response via post request
    response = requests.post(url, data=d)
    # get api from response by searching what is inside <api_key> tag
    api_key = re.findall("<api_key>(.*?)</api_key>", response.text)[0]
    return api_key

# code for download prospect IDs from Pardot API:
##################
def get_batch(api_key, after_mark, max_id):
    '''
    Get bach of first 200 recodrs of List API
    Args:
        api_key: api key for Authorization
        after_mark: last date that we need to start download
        max_id: id from where we continue download
    Output: first 200 records
    '''
    # API url
    url = 'https://pi.pardot.com/api/list/version/4/do/query'
    p = {
        'id_greater_than': max_id,
        'created_after': after_mark,
        
        'sort_by': 'id',
        'sort_order': 'ascending'
    }
    # header params for Authorization
    h = {
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Pardot user_key=bfe54a8f04e0a7d4239d5bcd90e79cbd ,api_key={api_key}"
    }
    # get request to retur the data
    response = requests.get(url, headers=h, params=p)
    return response

def get_new_created_data(last_date, get_id):
    '''
    Get all created data from lists api for 2020
    Args:
        last_date: date to start from
        get_id: ID to interate the data
    Output:
        return all collected data (creaded new or updated new)
    '''
    # get api_key
    api_key = get_api_key()
    # get first ID from where will start to collect the data
    max_id = str(int(get_id) - 1)
    # date from were we will start to collect the data
    after_mark = last_date
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
        try:
            # get bach of 200 records
            response = get_batch(api_key, after_mark, max_id)

            # find error if API key expired get new API key and run again get_bach to return the data
            find_err_1 = re.findall('<err code="1">(.*?)</err>', response.text)
            if len(find_err_1) != 0:
                if find_err_1[0] == 'Invalid API key or user key':
                    api_key = get_api_key()
                    response = get_batch(api_key, after_mark, crated_updated, max_id)
            # dataframe with response
            df = pd.DataFrame.from_dict(xmltodict.parse(response.text))
            
            # if first run get the total of records
            if count_total:
                total_results = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])    
            # total for the iteration
            total_for_period = int(pd.DataFrame.from_dict(df.loc['result']['rsp'])['total_results'][0])
            
            # record the dataframe with the needed data
            df = json_normalize(pd.DataFrame.from_dict(df.loc['result']['rsp'])['list'])
#    if error appeared wait 1 min and run try block again. if there is 5 errors raise error that there is to many errors
        except:
            print("API NOT WORK! TRIED AGAIN")
            error_count += 1
            if error_count == 5:
                raise Exception('Too many ERRORS!')
            time.sleep(60)
#      set max id to the max id in the dataframe so we can iterate from that ID for the next batch of 200  
        max_id = df.id.max()
        # drop these IDs from final dataframe
        ids = list(df.id)
        # if not first run remove data with IDs from new dataframe and set count total to false
        if count_total == False:
            final_df = final_df[~final_df.id.isin(ids)]
        count_total = False    
        # concatenata dataframe from current iteration to final dataframe and drop duplicates rows if any
        final_df = pd.concat([final_df, df], axis=0, ignore_index=True, sort=False)
        final_df = final_df.drop_duplicates(subset='id', keep="last")
        # if records from final data are equal or more than total results return from API we are collected all the data and break the loop
        
        if final_df.shape[0] >= total_results:
            break
    # return all collected data
    return final_df


def get_list_data_api():
    '''
    return dataframe with all Lists for 2020
    '''
    new_created_df = get_new_created_data('2020-01-01 00:00:00', '1')
    new_created_df = new_created_df.apply(lambda x : pd.to_numeric(x, errors='ignore'))   
    return new_created_df
#####################################

#code for download data form Alchemer:
############################
def lst_subquestions(subquestions):
    ''' 
        Input:
            subquestions: list of dictionaries with subquestions data
            
        Output:
            return list of all subquestion ids or None
    '''
    # check if the list of dictionaries is not np.nan
    if subquestions is not np.nan:
        # create empty list ot store the subquestions ids
        lst_subquestions=[]
        # for each dictionary in the provided list
        for each in subquestions:
            # get the subquestion id and append it to the final list
            lst_subquestions.append(str(each['id']))
        # return string with all subquestion ids
        return ",".join(lst_subquestions)
    else:
        # return None, in case the list is empty
        return None

# unpack options from answers
def unpack_options(row):
    result = pd.DataFrame()
    for k, v in row['options'].items():
        row['answer_id'] = k
        row['answer'] = v['answer']
        current_row = [[list(row.index)[i], k] if i == 5 else [list(row.index)[i], list(row.values)[i]] for i in range(len(list(row.values)))]
        current_row = [[list(row.index)[i], v['answer']] if i == 4 else [list(row.index)[i], list(row.values)[i]] for i in range(len(list(row.values)))]
        single = pd.DataFrame(current_row)
        single.set_index(0, inplace= True)
        single = single.T
        result = result.append(single)
    return result

def get_integration_data(survey_id):
    '''
        return:
            df_questions : information for questions in the survey
            df_options_raw : values of checkbox questions we have questionID=51 as checkbox
            df_responses_desc : information for survey respondents and their answers
            df_final_sub_answers : information and answers of all textbox questions
            df_answers_transformed : information and answers of checkbox question
    '''
    project_id = "project-pulldata"
    gbq_client = get_credentials(project_id)
    headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0' }
    # set token and api secret, used in the call to the Alchemer API
    token = '5eba4d051916f33ab54c5e05849fb6f87d8f573c90f87fe1cb'
    api_secret = 'A9oIXVYJxsMWU'
    # GET DATA FOR SUB OBJECT QUESTIONS
    # create empty dataframe for questions
    df_questions_raw = pd.DataFrame()
    print(f'Download questions for NS Media Group surveys')
    # for each survey collect questions
    surveys_lst = [90271405]
    for survey in surveys_lst:
        # set url to get number of pages for the corresponding survey
        url = f'https://api.alchemer.eu/v5/survey/{survey}/surveyquestion?api_token={token}&api_token_secret={api_secret}'
        response_question = requests.get(url,headers=headers).json()
        # get number of pages
        nb_pages = response_question['total_pages']
        # for each page collect the questions
        for page in range(nb_pages):
            # set url for each page
            url = f'https://api.alchemer.eu/v5/survey/{survey}/surveyquestion?api_token={token}&api_token_secret={api_secret}&page={page+1}'
            response_question_page = requests.get(url,headers=headers).json()
            # transform response into dataframe
            single_page = pd.DataFrame(response_question_page['data'])
            single_page['surveyID'] = survey
            single_page['page'] = page
            # append single page to the final questions dataframe
            df_questions_raw = df_questions_raw.append(single_page)
    # normalize title
    df_questions_raw['normalized_title'] =  json_normalize(df_questions_raw['title'])
    df_questions_raw['title'] = df_questions_raw['normalized_title'].map(lambda x:  BeautifulSoup(x, "lxml").get_text(strip = True))
    # drop unnescesary columns        
    df_questions = df_questions_raw.drop(['properties','options','normalized_title'], axis = 1)
    # transform subquestions section to a list of subquestion ids
    df_questions['sub_questions'] = df_questions_raw['sub_questions'].map(lst_subquestions)
    # rename id to questionID
    df_questions = df_questions.rename(columns={"id": "questionID"})
    # set type int to df_questions id columns
    df_questions['questionID'] = df_questions['questionID'].astype('int32')
    df_questions['surveyID'] = df_questions['surveyID'].astype('int32')
    df_questions['page'] = df_questions['page'].astype('int32')
    # cast list columns to string as Postgre returns an error when uploading the data
    df_questions['varname'] = df_questions['varname'].astype('str')
    df_questions['description'] = df_questions['description'].astype('str')
    # cast string to boolean
    df_questions['comment'] = df_questions['comment'].astype('bool')
    df_questions['has_showhide_deps'] = df_questions['has_showhide_deps'].astype('bool') #RETURN df_questions
    print(f'Finished download of questions for NS Media Group surveys')
    # GET DATA FOR SUB OBJECT QUESTION OPTIONS
    # create empty dataframe for questions options
    print(f'Download questions options for NS Media Group surveys')
    df_options_raw = pd.DataFrame()
    # for each survey and question pair of ids 
    for index, row in df_questions.iterrows():
        # set url
        url = f'https://api.alchemer.eu/v5/survey/{row["surveyID"]}/surveyquestion/{row["questionID"]}/surveyoption?api_token={token}&api_token_secret={api_secret}'
        response_options = requests.get(url,headers=headers).json()
        # get number of pages
        nb_pages = response_options['total_pages']
        # for eah page get options data
        for page in range(nb_pages):
            url = f'https://api.alchemer.eu/v5/survey/{row["surveyID"]}/surveyquestion/{row["questionID"]}/surveyoption?api_token={token}&api_token_secret={api_secret}&page={page+1}'
            response_options_page = requests.get(url,headers=headers).json()
            # transform response into dataframe
            single_page = pd.DataFrame(response_options_page['data'])
            single_page['surveyID'] = row["surveyID"]
            single_page['page'] = page
            single_page['questionID'] = row["questionID"]
            # append single page to the final OPTIONS dataframe
            df_options_raw = df_options_raw.append(single_page)
    # transform columns
    df_option_properties_raw = df_options_raw.properties.apply(pd.Series)
    # add disabled column to options dataframe
    df_options_raw['disabled'] = df_option_properties_raw[['disabled']]
    # set type to boolean
    df_options_raw['disabled'] = df_options_raw['disabled'].astype('bool')
    # drop properties column after unnesting it
    df_options_raw = df_options_raw.drop(['properties','title'], axis = 1)
    # rename columns
    df_options_raw = df_options_raw.rename(columns={'id':'optionID'})
    # set type int to df_options id column
    df_options_raw['optionID'] = df_options_raw['optionID'].astype('int32')
    df_options_raw['questionID'] = df_options_raw['questionID'].astype('int32')
    df_options_raw['page'] = df_options_raw['page'].astype('int32') # RETURN df_options_raw
    print(f'Finished download of questions options for NS Media Group surveys')
    # GET RESPONSES DATA
    # create empty dataframe for responses descriptions
    df_responses_desc = pd.DataFrame()
    # create empty dataframe for answers for each response
    df_answers = pd.DataFrame()
    print(f'Download responses for NS Media Group surveys')
    #get last updated date
    last_date = get_last_time(gbq_client, project_id)
    # for each survey collect the responses
    for survey in surveys_lst:
        # set response url petition
        url = f'https://api.alchemer.eu/v5/survey/{survey}/surveyresponse?api_token={token}&api_token_secret={api_secret}'
        response_resp = requests.get(url,headers=headers).json()
        # get number of pages for each survey  and collect data for each page
        for page in range(response_resp['total_pages']):
            # set url for each page
            url = f'https://api.alchemer.eu/v5/survey/{survey}/surveyresponse?api_token={token}&api_token_secret={api_secret}&page={page+1}'
            response_resp_page = requests.get(url,headers=headers).json()
            # transform response to dataframe
            single_response_desc = pd.DataFrame.from_dict(response_resp_page['data'])
            # rename id column to responseID
            single_response_desc = single_response_desc.rename(columns={"id": "responseID"})
            # set type int to df_responses_desc id columns
            single_response_desc['date_submitted'] = pd.to_datetime(single_response_desc['date_submitted'])
            single_response_desc = single_response_desc[single_response_desc['date_submitted'] > last_date]
            # separate responseID and survey data to process them separately
            answers = single_response_desc[['responseID','survey_data']]
            # drop unnecesary columns
            single_response_desc = single_response_desc.drop(['url_variables','survey_data'], axis = 1)
            # set surveyID for the response
            single_response_desc['surveyID'] = survey
            # append single response description to final dataframe
            df_responses_desc = df_responses_desc.append(single_response_desc)

            # process each answer (row)
            for index, row in answers.iterrows():
                # transform the row from dictionary to dataframe
                single = pd.DataFrame.from_dict(row['survey_data'], orient= 'index')
                # set the response ID
                single['responseID'] = row['responseID']
                # set the survey ID
                single['surveyID'] = survey
                # append answer to final answers dataframe
                df_answers = df_answers.append(single)
    print(f'Finished download of responses for NS Media Group surveys')
    # set type int to df_responses_desc id columns
    df_responses_desc['responseID'] = df_responses_desc['responseID'].astype('int32')
    # set type datetime to date column
    df_responses_desc['date_submitted'] = pd.to_datetime(df_responses_desc['date_submitted'])
    df_responses_desc['date_started'] = pd.to_datetime(df_responses_desc['date_started'])
    # set type str to list/dictionary columns
    df_responses_desc['data_quality'] = df_responses_desc['data_quality'].astype('str') # RETURN df_responses_desc
    if len(df_answers) > 0:
        # TRANSFORM ANSWERS OPTIONS
        print(f'Transforming answers options')
        # normalize question column            
        df_answers['question'] = df_answers['question'].apply(lambda x: BeautifulSoup(x, "lxml").get_text(strip = True))
        # rename question id column for each answer to questionID
        df_answers = df_answers.rename(columns={"id": "questionID"})
        # set type int to df_answers id columns
        df_answers['surveyID'] = df_answers['surveyID'].astype('int32')
        df_answers['responseID'] = df_answers['responseID'].astype('int32')
        df_answers['section_id'] = df_answers['section_id'].astype('int32')
        # set type str to list/dictionary columns
        #df_answers['answer'] = df_answers['answer'].astype('str')
        # unpack options column 
        # first get the answers without options
        df_answers_options_nan = df_answers[df_answers['options'].isnull()]
        # get the answers with options
        df_answers_options = df_answers[~df_answers['options'].isnull()]
        # applay unpack_options function to the rows with options
        df_answers_options['unpacked_options'] = df_answers_options.apply(lambda x: unpack_options(x), axis = 1)
        df_unpacked_options = pd.concat(df_answers_options['unpacked_options'].tolist())
        # join the two dataframes to obtain a unified df_answers 
        df_answers_transformed = df_answers_options_nan.append(df_unpacked_options)
        # drop options column , now it is split and added to respective columns
        df_answers_transformed = df_answers_transformed.drop(['options'], axis = 1)
        df_subquestions_answers = df_answers_transformed[~df_answers_transformed['subquestions'].isnull()]
        df_final_sub_answers = pd.DataFrame()
        for item in df_subquestions_answers[['subquestions','responseID']].values:
            df_single = pd.DataFrame(item[0]).T
            df_single['responseID'] = item[1]
            df_final_sub_answers = df_final_sub_answers.append(df_single)
        df_final_sub_answers['id'] = df_final_sub_answers['id'].astype('int32')
        df_final_sub_answers['parent'] = df_final_sub_answers['parent'].astype('int32')
        df_answers_transformed = df_answers_transformed.drop(['subquestions','section_id'], axis = 1) # RETURN df_final_sub_answers
        df_answers_transformed = df_answers_transformed[~df_answers_transformed['answer'].isnull()] # RETURN df_answers_transformed
        df_answers_transformed['surveyID'] = df_answers_transformed['surveyID'].astype('int32')
        df_answers_transformed['questionID'] = df_answers_transformed['questionID'].astype('int32')
        df_answers_transformed['responseID'] = df_answers_transformed['responseID'].astype('int32')
        df_answers_transformed['answer_id'] = df_answers_transformed['answer_id'].astype('int32')
        df_answers_transformed['shown'] = df_answers_transformed['shown'].astype('bool')
    else:
        print('No new data from survey')
        return False
    return df_questions,df_options_raw, df_responses_desc, df_final_sub_answers, df_answers_transformed
########################################################


def get_sub_answer(question, final_sub_answers):
    return final_sub_answers['answer'][final_sub_answers['question']==question][0]

def limit_response_id(table, response_id):
    # fltered dataframes only for the sepcific respondent
    return table[table['responseID']==response_id]

def get_info_user(response_id, df_responses_desc, df_answers_transformed, df_final_sub_answers):
    '''
    Get information from survey for specific response id
    Input:
    response_id: unique id for the respondent that we use to get information for specific respondent
    df_responses_desc : information for survey respondents and their answers
    df_answers_transformed : information and answers of checkbox question 
    df_final_sub_answers : information and answers of all textbox questions
    
    Output:
    information from the survey:
    forename
    surname
    job_title
    company
    country
    list_checkbox : list for selected answers from checkbox for subscriptions
    
    '''
    
    # fltered dataframes only for the sepcific respondent
    df_sys_location = limit_response_id(df_responses_desc, response_id)
    df_sign_up = limit_response_id(df_answers_transformed, response_id)
    final_sub_answers = limit_response_id(df_final_sub_answers, response_id)
    
    # get data for every question separated
    forename = get_sub_answer('Forename:',final_sub_answers)
    surname = get_sub_answer('Surname:',final_sub_answers)
    job_title = get_sub_answer('Job title:',final_sub_answers)
    company = get_sub_answer('Company:',final_sub_answers)
    country = get_sub_answer('Country:',final_sub_answers)
    # if there is no answer for country we get the country from system detected location
    if country == '':
        country = df_sys_location.country.values[0]
    # we get city data from system detected location
    city = df_sys_location.city.values[0]
    # get list of selected from checkbox question for subscriptions from df_sign_up dataframe
    list_sign_up = df_sign_up.values
    list_checkbox = []
    for el in list_sign_up:
        list_checkbox.append(el[7])
        list_checkbox
    return forename, surname, job_title, company, country, list_checkbox
    
def get_email_df(df_final_sub_answers):
    '''
        Input:
        df_final_sub_answers: dataframe with information and answers of all textbox questions to extract emails
        Output:
        dataframe with data for emails that we collect from survey
        
    '''
    email_df = df_final_sub_answers[(df_final_sub_answers['question']=='Email address:') & ((df_final_sub_answers['answer'] != ''))]
    return email_df    

def get_mails(email, api_key):
    url = f'https://pi.pardot.com/api/prospect/version/4/do/read/email/{email}'

    # header params for Authorization
    h = {
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Pardot user_key=bfe54a8f04e0a7d4239d5bcd90e79cbd ,api_key={api_key}"
    }
    # get request to retur the data
    response = requests.get(url, headers=h)   
    return response

def check_if_mail_exist(email, api_key):
    response = get_mails(email, api_key)
    find_err_1 = re.findall('<err code="1">(.*?)</err>', response.text)
    if len(find_err_1) != 0:
        api_key = get_api_key()
        response = get_mails(email, api_key)
    find_err_4 = re.findall('<err code="4">(.*?)</err>', response.text)
    if len(find_err_4) != 0:
        return False
    prospect_id = re.findall('<prospect>\n      <id>(.*?)</id>', response.text)[0]
    return prospect_id

def get_list_id(data_list, name, newsletter = False):
    '''
    input:
    datalist: dataframe with list data from API
    name: name that we will search if exist in question response that is selected for subsciption
    newsletter: if True we search if there is Newsletter in the name
    output:
    get ID for list that is in Pardot API
    
    '''
    if newsletter == False:
        list_id = data_list['id'][data_list['name'].str.contains(name)]
    else:
        list_id = data_list['id'][(data_list['name'].str.contains(name)) & (data_list['name'].str.contains('Newsletter'))]
    return list(list_id)

def get_all_ids(list_checkbox, data_list):
    '''
    input: list with answers from subscription question and the data for lists from Pardot api
    output:
     dict in format that we will put trough API for create and update list subsciptions
    '''
    # check if specific text is in the response that is selected and get the List IDs that are associated for that
    # add all IDs in one list
#     all_ids_list = [315801] # hardcode for 315801 - NSM -  Monitor Daily Newsletter
    all_ids_list = [317154, 317158, 317156]
    if any('City Monitor' in string for string in list_checkbox):
        all_ids_list.extend([317152])
    if any('New Statesman' in string for string in list_checkbox):
        all_ids_list.extend(get_list_id(data_list, 'New Statesman', newsletter = False))  
    if any('carefully selected third parties' in string for string in list_checkbox):
        all_ids_list.extend(get_list_id(data_list, 'Tech Monitor', newsletter = True))
    dict_ids = defaultdict() 
    # create dict with list ids in format that we use as params for the API call to create/update user with subscriptions
    for l in all_ids_list:
        dict_ids['list_'+str(l)] = '1'
    return dict_ids, all_ids_list

def get_info(job_title, company, country):
    '''
    Set info for users from textbox questions in format that we use as params for API call
    if there is no info for some of the questions we not add anything
    '''
    dict_info = defaultdict()
    if job_title != '':
        dict_info['job_title'] = job_title
    if company != '':
        dict_info['company'] = company    
    if country != '':
        dict_info['country'] = country    
    return dict_info

def add_pardot(api_key, email, forename, surname, dict_info, dict_ids):
     
    url = f'https://pi.pardot.com/api/prospect/version/4/do/create/email/{email}'
    # params for return new created data
    p = {
        'first_name':forename,
        'last_name':surname,
#         'Compelo_Core_data': 'TRUE',
#         'Compelo_Marketing_opt_in': 'true',
#         'Compelo_Marketing_data': 'TRUE',
#         'City_Monitor_Insights':'TRUE',

#         'opted_out':'0',
    }
    p.update(dict_info)
    p.update(dict_ids)
    # header params for Authorization
    h = {
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Pardot user_key=bfe54a8f04e0a7d4239d5bcd90e79cbd ,api_key={api_key}"
    }
    # get request to retur the data
    response = requests.get(url, headers=h, params=p)
    return True

def update_pardot(api_key, prospect_id, forename, surname, dict_info, dict_ids):
     
    url = f'https://pi.pardot.com/api/prospect/version/4/do/update/id/{prospect_id}'
    # params for return new created data
    p = {
        'first_name':forename,
        'last_name':surname,
#         'Compelo_Core_data':'TRUE',
#         'Compelo_Marketing_opt_in':'true',
#         'Compelo_Marketing_data':'TRUE',
#         'City_Monitor_Insights':'TRUE',

#         'opted_out':'0',
    }
    p.update(dict_info)
    p.update(dict_ids)
    # header params for Authorization
    h = {
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Pardot user_key=bfe54a8f04e0a7d4239d5bcd90e79cbd ,api_key={api_key}"
    }
    # get request to retur the data
    
    response = requests.get(url, headers=h, params=p)
    return True

def upload_survey_data(final_df, table, mode, postgre_engine, gbq_client, project_id):
        # upload data to gbq
    final_df.to_gbq(destination_table=f'AlchemerPardotIntegration.{table}',
              project_id = project_id, if_exists = mode)
    print(f'Table {table} uploaded successfully to GBQ.')
    # upload data to postgres
    final_df.to_sql(f'{table}',schema='alchemerpardotintegration', con=postgre_engine,method='multi',
              chunksize = 10000, if_exists = mode)
    print(f'Table {table} uploaded successfully to Postgre.')
    
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

def update_list(api_key, prospect_id, list_id):
    '''
    Get bach of first 200 recodrs
    Args:
        api_key: api key for Authorization
        after_mark: last date that we need to start download
        create_updated: if 'created' download new created, if 'updated' new updated
        max_id: id from where we continue download
    Output: first 200 records
    '''
    # API url
    url = f'https://pi.pardot.com/api/listMembership/version/4/do/create/list_id/{list_id}/prospect_id/{prospect_id}'
    # if created use params for return new created data
    p = {
        'opted_out':'0'
    }
    # header params for Authorization
    h = {
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Pardot user_key=bfe54a8f04e0a7d4239d5bcd90e79cbd ,api_key={api_key}"
    }
    # get request to retur the data
    response = requests.get(url,  headers=h, params=p)
    return response

def update_all_lists(api_key, prospect_id, all_ids_list):
    for list_id in all_ids_list:
        response = update_list(api_key, prospect_id, list_id)
        print(response.text)
    return True
    

def update_create_prospects():
    '''
    main function for update or add new prospect users from Alchemer survey
    '''
    project_id = "project-pulldata"
    gbq_client = get_credentials(project_id)
    postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
    # get all tables from Alchemer survey
    data_survey = get_integration_data(90271405)
    if data_survey:
        df_questions, df_options_raw, df_responses_desc, df_final_sub_answers, df_answers_transformed = data_survey
        # get dataframe with all provided emails, we exclude respondents without emails
        email_df = get_email_df(df_final_sub_answers)
        # get new api key
        api_key = get_api_key()
        # dataframe with all Lists for 2020
        data_list = get_list_data_api()
        # iterate for every row of email dataframe
        count_new_users = 0
        count_updates = 0
        for index, row in email_df.iterrows():
            # get the information from the survey by respondent id
            forename, surname, job_title, company, country, list_checkbox = get_info_user(row.responseID, df_responses_desc, df_answers_transformed, df_final_sub_answers)
            # get all IDS for subscriptions that respondent is selected to subscribe from Pardot data with all lists
            # we need this ID to create and update subscriptions with Pardot API
            # the data is in dict format
            dict_ids, all_ids_list = get_all_ids(list_checkbox, data_list)
            print(all_ids_list)
            # get infor for job_title, company, country to dict format
            dict_info = get_info(job_title, company, country)
            # check if email exist if not return false if exist return the id of prosepect user form Pardot system
            update_id = check_if_mail_exist(row.answer, api_key)
            print(dict_ids)
            print(dict_info)
            if update_id:
                print('update')
                count_updates+=1
                # update the user
                update_pardot(api_key, update_id, forename, surname, dict_info, dict_ids)
                update_all_lists(api_key, update_id, all_ids_list)
            else:
                print('create')
                count_new_users+=1
                # create new user
                add_pardot(api_key, row.answer, forename, surname, dict_info, dict_ids)
                update_id = check_if_mail_exist(row.answer, api_key)
                if update_id:
                    update_all_lists(api_key, update_id, all_ids_list)
                else:
                    print('ERROR the User is not created')
                    
        # record log file
        log_update(count_new_users, count_updates)
        print('Log is recorded') 
        # upload data
        list_tables = [[df_questions, 'Questions', 'replace'],
                       [df_options_raw, 'QuestionsOptions', 'replace'],
                       [df_responses_desc, 'ResponsesDescription', 'append'],
                       [df_final_sub_answers, 'SubquestionAnswers', 'append'],
                       [df_answers_transformed, 'Answers', 'append'],
                      ]
        for table in list_tables:
            dataframe, tablename, mode = table
            upload_survey_data(dataframe, tablename, mode,
                               postgre_engine, gbq_client, project_id)
            update_metadata(dataframe,
                            'AlchemerPardotIntegration', tablename,
                            postgre_engine, gbq_client, project_id)
            
    return True

default_args = {
    'owner': 'project-pulldata_alchemer_pardot_integration',
    'start_date': datetime.datetime(2020, 12, 21, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_alchemer_pardot_integration',
     catchup=False,
     default_args=default_args,
     schedule_interval='*/5 * * * *',
     ) as dag:
    
    integrate_alchemer_pardot = PythonOperator(task_id='update_create_prospects',
                               python_callable=update_create_prospects)
integrate_alchemer_pardot