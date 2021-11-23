import datetime
import re
from collections import defaultdict
import requests
import pandas as pd
from ingest_utils.constants import PULLDATA_PROJECT_ID
from pardot_utils.utils import integration_log, get_new_created_updated_data, get_api_key
from pardot_utils.constants import B2B_EMAIL, B2B_PASSWORD, B2B_USER_KEY, LISTS_DATA_NAME, GET_EMAIL, CREATE_EMAIL, \
    PROSPECTS_API, UPDATE_PROSPECT, UPDATE_LIST, LIST_MEMBERSHIP_API
from alchemer_data_update.gbq_queries import QUERY_MAX_INTEGRATION_TIME
from alchemer_data_update.utils import download_surveys_questions_data, download_surveys_questions_options_data, \
    get_last_time, download_survey_responses_data, unpack_options
from bs4 import BeautifulSoup


def get_list_data_api(url: str) -> pd.DataFrame:
    """
    Download lists for 2020
    :param url: Pardot API url
    :return: dataframe with lists
    """
    new_created_df = get_new_created_updated_data(url, datetime.datetime.strptime('2020-01-01 00:00:00',
                                                                                  '%Y-%m-%d %H:%M:%S'), '1', 'created',
                                                  B2B_EMAIL, B2B_PASSWORD, B2B_USER_KEY, LISTS_DATA_NAME,
                                                  before_mark_delta=3600)
    new_created_df = new_created_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
    return new_created_df


def get_integration_data(survey_id: list) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """
    Download alchemer survey data
    :param survey_id:
    :return:df_questions : information for questions in the survey
            df_options_raw : values of checkbox questions we have questionID=51 as checkbox
            df_responses_desc : information for survey respondents and their answers
            df_final_sub_answers : information and answers of all textbox questions
            df_answers_transformed : information and answers of checkbox question
    """
    # GET DATA FOR SUB OBJECT QUESTIONS
    df_questions = download_surveys_questions_data(survey_id)
    # GET DATA FOR SUB OBJECT QUESTION OPTIONS
    df_options_raw = download_surveys_questions_options_data(df_questions)
    # GET RESPONSES DATA
    # get last updated date
    last_date = get_last_time(PULLDATA_PROJECT_ID, QUERY_MAX_INTEGRATION_TIME.format(project_id=PULLDATA_PROJECT_ID,
                                                                                     dataset='AlchemerPardotIntegration',
                                                                                     table='ResponsesDescription'))
    df_responses_desc, df_answers = download_survey_responses_data(survey_id, last_date=last_date)
    if len(df_answers) > 0:
        # TRANSFORM ANSWERS OPTIONS
        print(f'Transforming answers options')
        # normalize question column
        df_answers['question'] = df_answers['question'].apply(lambda x: BeautifulSoup(x, "lxml").get_text(strip=True))
        # rename question id column for each answer to questionID
        df_answers = df_answers.rename(columns={"id": "questionID"})
        # set type int to df_answers id columns
        df_answers['surveyID'] = df_answers['surveyID'].astype('int32')
        df_answers['responseID'] = df_answers['responseID'].astype('int32')
        df_answers['section_id'] = df_answers['section_id'].astype('int32')
        # unpack options column
        # first get the answers without options
        df_answers_options_nan = df_answers[df_answers['options'].isnull()]
        # get the answers with options
        df_answers_options = df_answers[~df_answers['options'].isnull()]
        # apply unpack_options function to the rows with options
        df_answers_options['unpacked_options'] = df_answers_options.apply(lambda x: unpack_options(x), axis=1)
        df_unpacked_options = pd.concat(df_answers_options['unpacked_options'].tolist())
        # join the two dataframes to obtain a unified df_answers
        df_answers_transformed = df_answers_options_nan.append(df_unpacked_options)
        # drop options column , now it is split and added to respective columns
        df_answers_transformed = df_answers_transformed.drop(['options'], axis=1)
        df_subquestions_answers = df_answers_transformed[~df_answers_transformed['subquestions'].isnull()]
        df_final_sub_answers = pd.DataFrame()
        for item in df_subquestions_answers[['subquestions', 'responseID']].values:
            df_single = pd.DataFrame(item[0]).T
            df_single['responseID'] = item[1]
            df_final_sub_answers = df_final_sub_answers.append(df_single)
        df_final_sub_answers['id'] = df_final_sub_answers['id'].astype('int32')
        df_final_sub_answers['parent'] = df_final_sub_answers['parent'].astype('int32')
        df_answers_transformed = df_answers_transformed.drop(['subquestions', 'section_id'], axis=1)
        df_answers_transformed = df_answers_transformed[~df_answers_transformed['answer'].isnull()]
        df_answers_transformed['surveyID'] = df_answers_transformed['surveyID'].astype('int32')
        df_answers_transformed['questionID'] = df_answers_transformed['questionID'].astype('int32')
        df_answers_transformed['responseID'] = df_answers_transformed['responseID'].astype('int32')
        df_answers_transformed['answer_id'] = df_answers_transformed['answer_id'].astype('int32')
        df_answers_transformed['shown'] = df_answers_transformed['shown'].astype('bool')
    else:
        print('No new data from survey')
        return False
    return df_questions, df_options_raw, df_responses_desc, df_final_sub_answers, df_answers_transformed


def get_sub_answer(question: str, final_sub_answers: pd.DataFrame) -> pd.DataFrame:
    """
    Get sub answers from answers dataframe
    :param question: specific question for which we want to get the sub answers
    :param final_sub_answers: dataframe which contains the answers
    :return: dataframe with sub answers
    """
    return final_sub_answers['answer'][final_sub_answers['question'] == question][0]


def limit_response_id(table: pd.DataFrame, response_id: str) -> pd.DataFrame:
    """
    Filter responses for specific respondent
    :param table: dataframe with responses
    :param response_id: response id
    :return: dataframe with filtered responses
    """
    return table[table['responseID'] == response_id]


def get_info_user(response_id: str, df_responses_desc: pd.DataFrame, df_answers_transformed: pd.DataFrame,
                  df_final_sub_answers: pd.DataFrame) -> (str, str, str, str, str, list):
    """
    Get information from survey for specific response id
    :param response_id: unique id for the respondent for which we get information
    :param df_responses_desc: information for survey respondents and their answers
    :param df_answers_transformed: information and answers of checkbox question
    :param df_final_sub_answers: information and answers of all textbox questions
    :return: information from the survey: forename, surname, job_title, company, country ,list_checkbox (list for
                selected answers from checkbox for subscriptions)
    """
    # filtered dataframes only for the specific respondent
    df_sys_location = limit_response_id(df_responses_desc, response_id)
    df_sign_up = limit_response_id(df_answers_transformed, response_id)
    final_sub_answers = limit_response_id(df_final_sub_answers, response_id)
    # get data for every question separated
    forename = get_sub_answer('Forename:', final_sub_answers)
    surname = get_sub_answer('Surname:', final_sub_answers)
    job_title = get_sub_answer('Job title:', final_sub_answers)
    company = get_sub_answer('Company:', final_sub_answers)
    country = get_sub_answer('Country:', final_sub_answers)
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


def get_email_df(df_final_sub_answers: pd.DataFrame) -> pd.DataFrame:
    """
    Get dataframe with emails
    :param df_final_sub_answers: dataframe with information and answers of all textbox questions to extract emails
    :return: dataframe with data for emails that we collect from survey
    """
    email_df = df_final_sub_answers[
        (df_final_sub_answers['question'] == 'Email address:') & (df_final_sub_answers['answer'] != '')]
    return email_df


def get_mails(email: str, api_key: str, user_key: str):
    """
    Get emails
    :param email: specific email
    :param api_key: pardot api key
    :param user_key: pardot user key
    :return: response for specific email
    """
    url = GET_EMAIL.format(pardot_api=PROSPECTS_API, email=email)
    # header params for Authorization
    h = {
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Pardot user_key={user_key} ,api_key={api_key}"
    }
    # get request to return the data
    response = requests.get(url, headers=h)
    return response


def check_if_mail_exist(email: str, api_key: str, user_key: str):
    """
    Check if email exists in Pardot
    :param email: specific email
    :param api_key: api key for pardot
    :param user_key: user key for api
    :return: False if it doesnt exist, or the respective prospect id if it exists
    """
    response = get_mails(email, api_key, user_key)
    find_err_1 = re.findall('<err code="1">(.*?)</err>', response.text)
    if len(find_err_1) != 0:
        api_key = get_api_key()
        response = get_mails(email, api_key)
    find_err_4 = re.findall('<err code="4">(.*?)</err>', response.text)
    if len(find_err_4) != 0:
        return False
    prospect_id = re.findall('<prospect>\n      <id>(.*?)</id>', response.text)[0]
    return prospect_id


def get_list_id(data_list, name, newsletter=False) -> list:
    """
    Get the ID for list that is in Pardot API
    :param data_list: dataframe with list data from API
    :param name: name that we will search if exist in question response that is selected for subscription
    :param newsletter: if True we search if there is Newsletter in the name
    :return: list id
    """
    if not newsletter:
        list_id = data_list['id'][data_list['name'].str.contains(name)]
    else:
        list_id = data_list['id'][
            (data_list['name'].str.contains(name)) & (data_list['name'].str.contains('Newsletter'))]
    return list(list_id)


def get_all_ids(list_checkbox: list, data_list: list) -> (dict, list):
    """

    :param list_checkbox: list with answers from subscription question
    :param data_list: data for lists from Pardot api
    :return: dict in format that we will put trough API for create and update list subscriptions
    """
    # check if specific text is in the response that is selected and get the List IDs that are associated for that
    # add all IDs in one list
    # all_ids_list = [315801] # hardcode for 315801 - NSM -  Monitor Daily Newsletter
    all_ids_list = [317154, 317158, 317156]
    if any('City Monitor' in string for string in list_checkbox):
        all_ids_list.extend([317152])
    if any('New Statesman' in string for string in list_checkbox):
        all_ids_list.extend(get_list_id(data_list, 'New Statesman', newsletter=False))
    if any('carefully selected third parties' in string for string in list_checkbox):
        all_ids_list.extend(get_list_id(data_list, 'Tech Monitor', newsletter=True))
    dict_ids = defaultdict()
    # create dict with list ids in format that we use as params for the API call to create/update user with
    # subscriptions
    for l in all_ids_list:
        dict_ids['list_'+str(l)] = '1'
    return dict_ids, all_ids_list


def get_info(job_title: str, company: str, country: str) -> dict:
    """
    Set info for users from textbox questions in format that we use as params for API call
    if there is no info for some of the questions we not add anything
    :param job_title: job title
    :param company: company
    :param country: country
    :return: dictionary with requested information
    """
    dict_info = defaultdict()
    if job_title != '':
        dict_info['job_title'] = job_title
    if company != '':
        dict_info['company'] = company
    if country != '':
        dict_info['country'] = country
    return dict_info


def add_pardot(api_key: str, user_key: str, email: str, forename: str, surname: str, dict_info: dict,
               dict_ids: dict) -> bool:
    """
    Add a new user to pardot
    :param api_key: pardot api key
    :param user_key: pardot user key
    :param email: users email
    :param forename: user's forename
    :param surname: user's surname
    :param dict_info: dictionary with user's information
    :param dict_ids: dictionary with ids
    :return: True if added successfully to pardot users
    """
    url = CREATE_EMAIL.format(pardot_api=PROSPECTS_API, email=email)
    # params for return new created data
    p = {
        'first_name': forename,
        'last_name': surname,
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
        "Authorization": f"Pardot user_key={user_key} ,api_key={api_key}"
    }
    # get request to return the data
    requests.get(url, headers=h, params=p)
    return True


def update_pardot(api_key: str, user_key: str, prospect_id: str, forename: str, surname: str, dict_info: dict,
                  dict_ids: dict) -> bool:
    """
    Update data for existing prospect id
    :param api_key: pardot api key
    :param user_key: pardot user key
    :param prospect_id: prospect id to update
    :param forename: forename
    :param surname: surname
    :param dict_info: dictionary with info for specific prospect
    :param dict_ids: dictionary with ids for specific prospect
    :return: True if updated successfully
    """
    url = UPDATE_PROSPECT.format(pardot_api=PROSPECTS_API, prospect_id=prospect_id)
    # params for return new created data
    p = {
        'first_name': forename,
        'last_name': surname,
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
        "Authorization": f"Pardot user_key={user_key} ,api_key={api_key}"
    }
    # get request to return the data
    requests.get(url, headers=h, params=p)
    return True


def update_list(api_key: str, user_key: str, prospect_id: str, list_id: str):
    """
    Update existing list from pardot
    :param api_key: pardot api key
    :param user_key: pardot user key
    :param prospect_id: specific prospect to update
    :param list_id: specific list to update
    :return:
    """
    # API url
    url = UPDATE_LIST.format(pardot_api=LIST_MEMBERSHIP_API, list_id=list_id, prospect_id=prospect_id)
    # if created use params for return new created data
    p = {
        'opted_out': '0'
    }
    # header params for Authorization
    h = {
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded",
        "Authorization": f"Pardot user_key={user_key} ,api_key={api_key}"
    }
    # get request to return the data
    response = requests.get(url,  headers=h, params=p)
    return response


def update_all_lists(api_key: str, user_key: str, prospect_id: str, all_ids_list: list) -> bool:
    """
    Update all lists for user
    :param api_key: pardot api key
    :param user_key: pardot user key
    :param prospect_id: specific prospect to update
    :param all_ids_list: list of lits ids to update
    :return: True if lists updated successfully
    """
    for list_id in all_ids_list:
        response = update_list(api_key, user_key,prospect_id, list_id)
        print(response.text)
    return True

