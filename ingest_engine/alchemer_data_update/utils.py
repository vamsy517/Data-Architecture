import datetime
from alchemer_data_update.logging import log_update
from sql_queries.gbq_queries import QUERY_DATASET_INFORMATION_SCHEMA
from ingest_utils.database_gbq import get_gbq_table, delete_table_from_gbq, upload_table_to_gbq, get_gbq_clients
from ingest_utils.database_postgre import delete_table_from_postgre, upload_table_to_postgre
from ingest_utils.metadata import update_metadata
from alchemer_data_update.constants import BASE_URL, TOKEN, API_SECRET, HEADERS
import pandas as pd
import numpy as np
import requests
from pandas.io.json import json_normalize
from bs4 import BeautifulSoup
import re
from google.cloud import bigquery


def replace_data(project_id: str, postgre_engine: str, df: pd.DataFrame, dataset: str, table: str, log_file: str,
                 lst_ids=None, schema=None, json_data=None) -> bool:
    """
    Delete specific table from postgre and bq
    Upload the new df to postgre and bq
    Update metadata
    Log results
    :param project_id: Project ID
    :param postgre_engine: connection to Postgre
    :param df: dataframe to replace
    :param dataset: dataset to which we upload the df
    :param table: specific table which will contain the df
    :param log_file: path to log file where to log processing results
    :param lst_ids: list of survey IDs, for which there is either no report or statistics
    :param schema: schema provided for table SurveysDescriptions
    :param json_data: json data to upload to gbq table. Default None
    :return: return True if all operations are successful
    """
    # check if dataframe is empty
    if df.empty:
        print(f'There is no data for {dataset}.{table}. Skipping replacement.')
        log_update(log_file, dataset, table, False)
    else:
        # check if table exists in Postgre and BigQuery, if not skip deleting
        df_dataset_schema = get_gbq_table(project_id,
                                          QUERY_DATASET_INFORMATION_SCHEMA.format(dataset=dataset))
        tables_list = df_dataset_schema['table_name'].tolist()
        if table in tables_list:
            # delete table from postgre and BigQuery
            delete_table_from_postgre(postgre_engine, dataset, table)
            delete_table_from_gbq(project_id, dataset, table)
        # upload new table to postgre and bq
        if table == 'SurveysDescriptions':
            # upload from json
            upload_table_to_gbq_from_json(project_id, json_data, dataset, table, schema)
        else:
            upload_table_to_gbq(project_id, df, dataset, table, method='replace', schema=schema)
        upload_table_to_postgre(postgre_engine, df, dataset, table, method='replace')
        # update the metadata for table
        update_metadata(project_id, postgre_engine, df, dataset, table)
        if (lst_ids is not None) and (len(lst_ids) > 0):
            # log updated IDs and Tables
            log_update(log_file, dataset, table, True, lst_ids=lst_ids)
        else:
            # log updated table
            log_update(log_file, dataset, table, True)
    return True


def upload_table_to_gbq_from_json(project_id: str, json_data: list, dataset: str, table: str, schema: list) -> bool:
    """
    Upload json data to GBQ table
    :param project_id:  Project ID
    :param json_data: Json data to upload
    :param dataset: dataset where to upload the json data in GBQ
    :param table: table where to upload the json data in GBQ
    :param schema: Table Schema
    :return: True if successful
    """
    # Upload to GBQ with specific schema
    gbq_client, _ = get_gbq_clients(project_id)
    job_config = bigquery.LoadJobConfig(schema=schema)
    gbq_client.load_table_from_json(json_data, f'{dataset}.{table}', job_config=job_config).result()
    print(f'Table {dataset}.{table} is uploaded/updated to GBQ')
    return True


def lst_subquestions(subquestions) -> str:
    """
    Transform sub questions list into sub questions string
    :param subquestions: list of dictionaries with sub questions data
    :return: string of all sub question ids or 'None'
    """
    # create empty list to store the sub questions ids
    lst_subquestions = []
    # check if the list of dictionaries is not np.nan
    if subquestions is not np.nan:
        if not isinstance(subquestions, list):
            if 'JobTitle' in subquestions.keys():
                lst_subquestions.append(str(subquestions['JobTitle']['id']))
            if 'JobTitle_Other' in subquestions.keys():
                lst_subquestions.append(str(subquestions['JobTitle_Other']['id']))
            # return string with all sub question ids
            return ",".join(lst_subquestions)
        else:
            # for each dictionary in the provided list
            for each in subquestions:
                # get the sub question id and append it to the final list
                lst_subquestions.append(str(each['id']))
            # return string with all sub question ids
            return ",".join(lst_subquestions)
    else:
        # return None, in case the list is empty
        return 'None'


def unpack_options(row: pd.Series) -> pd.DataFrame:
    """
    Unpack options column for single row
    :param row: specific row with options
    :return: dataframe with unpacked options
    """
    result = pd.DataFrame()
    for k, v in row['options'].items():
        row['answer_id'] = k
        row['answer'] = v['answer']
        current_row = [[list(row.index)[i], v['answer']] if i == list(row.index).index('answer') else
                       [list(row.index)[i], list(row.values)[i]] for i in range(len(list(row.values)))]
        single = pd.DataFrame(current_row)
        single.set_index(0, inplace=True)
        single = single.T
        result = result.append(single)
    return result


def unpack_subquestions(row: pd.Series) -> pd.DataFrame:
    """
    Unpack sub questions sub questions column for single row
    :param row: specific row with sub questions
    :return: dataframe with unpacked sub questions
    """
    df_ss = pd.DataFrame()
    for k, v in row['subquestions'].items():
        single = pd.DataFrame(v).T
        single['subquestionID'] = k
        single['responseID'] = row['responseID']
        single['surveyID'] = row['surveyID']
        df_ss = df_ss.append(single)
    return df_ss


def download_surveys_data(team_id: str) -> pd.DataFrame:
    """
    Download surveys data for specific team
    :param team_id: id of the specific team
    :return: dataframe containing surveys downloaded data
    """
    # get list of surveys
    print(f'Download full list of surveys')
    # set url to get survey information
    url = f'{BASE_URL}?api_token={TOKEN}&api_token_secret={API_SECRET}'
    # get survey information from first page
    response_survey = requests.get(url, headers=HEADERS).json()
    # get number of pages
    nb_pages = response_survey['total_pages']
    # create empty dataframe to store all surveys information
    df_surveys = pd.DataFrame()
    # for each page of the response collect the information about surveys
    for page in range(nb_pages):
        # set url for each page
        url = f'{BASE_URL}?api_token={TOKEN}&api_token_secret={API_SECRET}&page={page + 1}'
        # get data for surveys from the corresponding page
        response_survey_page = requests.get(url, headers=HEADERS).json()
        # transform response into dataframe
        df_single_page = pd.DataFrame.from_dict(response_survey_page['data'])
        # filter only surveys from ns media team
        df_single_page = df_single_page[df_single_page['team'] == team_id]
        # append data from single page to final dataframe
        df_surveys = df_surveys.append(df_single_page)
    # drop statistics and links columns from df_surveys
    df_surveys = df_surveys.drop(['statistics', 'links'], axis=1)
    # rename columns
    df_surveys = df_surveys.rename(columns={'id': 'surveyID', '_type': 'type', '_subtype': 'subtype'})
    # cast some columns to integer
    df_surveys['surveyID'] = df_surveys['surveyID'].astype('int32')
    df_surveys['team'] = df_surveys['team'].astype('int32')
    # Set type datetime to date columns because it is string
    df_surveys['created_on'] = pd.to_datetime(df_surveys['created_on'])
    df_surveys['modified_on'] = pd.to_datetime(df_surveys['modified_on'])
    df_surveys = df_surveys[~df_surveys['title'].str.startswith('Test -')]
    df_surveys = df_surveys[~df_surveys['title'].str.startswith('Test-')]
    print(f'Finished download of full list of surveys')
    return df_surveys


def download_surveys_description_data(surveys_lst: list) -> (pd.DataFrame, list):
    """
    Download surveys description data for each survey id in provided list
    :param surveys_lst: list of survey ids
    :return: dataframe with surveys descriptions
    """
    print(f'Download descriptions for surveys')
    # create empty list to store the json, used to upload the table SurveysDescriptions to GBQ
    description_json = []
    # for each survey of the list extract data
    for survey in surveys_lst:
        # set url for each survey
        url = f'{BASE_URL}/{survey}?api_token={TOKEN}&api_token_secret={API_SECRET}'
        # TRY  to get the description for the survey, in case it fails, continue with next id
        try:
            # get response
            response_desc = requests.get(url, headers=HEADERS).json()
            # copy data dictionary from response
            data_dict = response_desc['data'].copy()
            # remove pages section, as it is already processed
            data_dict.pop('pages', None)
            # remove team section, as it contains data only for NS Media Group team
            data_dict.pop('team', None)
            # rename id to surveyID and set it to integer
            data_dict_rename = dict(('surveyID', int(v)) if k == 'id' else (k, v) for k, v in data_dict.items())
            # transform languages to string, as it is a list
            data_dict_transform = dict(
                (k, ",".join(v)) if k == 'languages' else (k, v) for k, v in data_dict_rename.items())
            # TODO: address bug from Alchemer where key in a dictionary is empty
            # remove "" key from statistics dictionary
            if data_dict_transform['statistics'] is not None:
                dict_statistics = dict((k, v) for k, v in data_dict_transform['statistics'].items() if k != "")
                data_dict_transform['statistics'] = dict_statistics
            description_json.append(data_dict_transform)
        except:
            print('Tried unsuccessfully with survey id ', survey)
            continue
    # create a new dataframe from the json data, which will be used
    # to upload the table SurveysDescriptions to Postgre
    df_json_normalized_data = json_normalize(description_json)
    # drop statistics columns
    df_json_normalized_data = df_json_normalized_data.drop('statistics', axis=1)
    # rearrange columns to reflect order in schema
    df_description = df_json_normalized_data[['surveyID', 'type', 'status', 'created_on', 'modified_on',
                                              'forward_only', 'languages', 'title', 'internal_title',
                                              'title_ml.English', 'theme', 'blockby', 'statistics.Partial',
                                              'statistics.Complete', 'statistics.TestData', 'statistics.Deleted',
                                              'statistics.Disqualified', 'overall_quota', 'auto_close',
                                              'links.default', 'links.campaign']]
    # cast dates to datetime type
    df_description['created_on'] = pd.to_datetime(df_description['created_on'])
    df_description['modified_on'] = pd.to_datetime(df_description['modified_on'])
    print(f'Finished download of descriptions for surveys')
    return df_description, description_json


def download_surveys_questions_data(surveys_lst: list) -> pd.DataFrame:
    """
    Download surveys questions data for each survey id in provided list
    :param surveys_lst: list of survey ids
    :return: dataframe with surveys questions
    """
    # create empty dataframe for questions
    df_questions_raw = pd.DataFrame()
    print(f'Download questions for surveys')
    # for each survey collect questions
    for survey in surveys_lst:
        # set url to get number of pages for the corresponding survey
        url = f'{BASE_URL}/{survey}/surveyquestion?api_token={TOKEN}&api_token_secret={API_SECRET}'
        # TRY to get the data for sub object question for the survey, in case it fails continue with next survey
        try:
            response_question = requests.get(url, headers=HEADERS).json()
            # get number of pages
            nb_pages = response_question['total_pages']
            # for each page collect the questions
            for page in range(nb_pages):
                # set url for each page
                url = f'{BASE_URL}/{survey}/surveyquestion?api_token={TOKEN}&api_token_secret={API_SECRET}&page={page + 1}'
                response_question_page = requests.get(url, headers=HEADERS).json()
                # transform response into dataframe
                single_page = pd.DataFrame(response_question_page['data'])
                single_page['surveyID'] = survey
                single_page['page'] = page
                # append single page to the final questions dataframe
                df_questions_raw = df_questions_raw.append(single_page)
        except:
            print('Tried unsuccessfully with survey id ', survey)
            continue
    # normalize title
    df_questions_raw['normalized_title'] = json_normalize(df_questions_raw['title'])
    df_questions_raw['title'] = df_questions_raw['normalized_title'].map(
        lambda x: BeautifulSoup(x, "lxml").get_text(strip=True))
    # drop unnecessary columns
    df_questions = df_questions_raw.drop(['properties', 'options', 'normalized_title'], axis=1)
    # transform sub questions section to a list of sub question ids
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
    df_questions['has_showhide_deps'] = df_questions['has_showhide_deps'].astype('bool')
    print(f'Finished download of questions for surveys')
    return df_questions


def download_surveys_questions_options_data(df_questions: pd.DataFrame) -> pd.DataFrame:
    """
    Download questions options data for each question in provided dataframe
    :param df_questions: dataframe containing questions data
    :return: dataframe with surveys questions options data
    """
    # create empty dataframe for questions options
    print(f'Download questions options for surveys')
    df_options_raw = pd.DataFrame()
    # for each survey and question pair of ids
    for index, row in df_questions.iterrows():
        # set url
        url = f'{BASE_URL}/{row["surveyID"]}/surveyquestion/{row["questionID"]}' \
              f'/surveyoption?api_token={TOKEN}&api_token_secret={API_SECRET}'
        try:
	        response_options = requests.get(url, headers=HEADERS).json()
	        # get number of pages
	        nb_pages = response_options['total_pages']
	        # for eah page get options data
	        for page in range(nb_pages):
	            url = f'{BASE_URL}/{row["surveyID"]}/surveyquestion/{row["questionID"]}' \
	                  f'/surveyoption?api_token={TOKEN}&api_token_secret={API_SECRET}&page={page + 1}'
	            response_options_page = requests.get(url, headers=HEADERS).json()
	            # transform response into dataframe
	            single_page = pd.DataFrame(response_options_page['data'])
	            single_page['surveyID'] = row["surveyID"]
	            single_page['page'] = page
	            single_page['questionID'] = row["questionID"]
	            # append single page to the final OPTIONS dataframe
	            df_options_raw = df_options_raw.append(single_page)
        except:
            print('Tried unsuccessfully with option id: ', row["questionID"])
            continue

    # transform columns
    df_option_properties = df_options_raw.properties.apply(pd.Series)
    df_options = df_options_raw.copy()
    # add disabled column to options dataframe
    df_options['disabled'] = df_option_properties[['disabled']]
    # set type to boolean
    df_options['disabled'] = df_options['disabled'].astype('bool')
    # drop properties column after un nesting it
    df_options = df_options.drop(['properties', 'title'], axis=1)
    if 'image' in df_options.columns:
        df_options = df_options.drop(['image'], axis=1)
    # rename columns
    df_options = df_options.rename(columns={'id': 'optionID'})
    # set type int to df_options id column
    df_options['optionID'] = df_options['optionID'].astype('int32')
    df_options['questionID'] = df_options['questionID'].astype('int32')
    df_options['page'] = df_options['page'].astype('int32')
    print(f'Finished download of questions options for surveys')
    return df_options


def download_survey_responses_data(surveys_lst: list, last_date: datetime = None) -> (pd.DataFrame, pd.DataFrame):
    """
    Download survey responses data for each survey in provided list
    :param last_date: optional parameter for filtering responses description dataframe
    :param surveys_lst: list of survey ids
    :return: dataframe with responses descriptions data and dataframe with answers data
    """
    # create empty dataframe for responses descriptions
    df_responses_desc = pd.DataFrame()
    # create empty dataframe for answers for each response
    df_answers = pd.DataFrame()
    print(f'Download responses for NS Media Group surveys')
    # for each survey collect the responses
    for survey in surveys_lst:
        # set response url petition
        url = f'{BASE_URL}/{survey}/surveyresponse?api_token={TOKEN}&api_token_secret={API_SECRET}'
        # try to get responses for survey, in case it fails continue with next survey id
        try:
            response_resp = requests.get(url, headers=HEADERS).json()
            # get number of pages for each survey  and collect data for each page
            for page in range(response_resp['total_pages']):
                # set url for each page
                url = f'{BASE_URL}/{survey}/surveyresponse?api_token={TOKEN}&api_token_secret={API_SECRET}&page={page + 1}'
                response_resp_page = requests.get(url, headers=HEADERS).json()
                # transform response to dataframe
                single_response_desc = pd.DataFrame.from_dict(response_resp_page['data'])
                # rename id column to responseID
                single_response_desc = single_response_desc.rename(columns={"id": "responseID"})
                # set type datetime to date column
                single_response_desc['date_submitted'] = pd.to_datetime(single_response_desc['date_submitted'])
                single_response_desc['date_started'] = pd.to_datetime(single_response_desc['date_started'])
                if last_date is not None:
                    single_response_desc = single_response_desc[single_response_desc['date_submitted'] > last_date]
                # separate responseID and survey data to process them separately
                answers = single_response_desc[['responseID', 'survey_data']]
                # get the permutive user id
                single_response_desc['permutive_id'] = single_response_desc['url_variables'].apply(
                    lambda x: x if x == [] else (
                        re.findall("{'key': 'permutive_user_id', 'value': '(.*?)', 'type': 'url'}", str(x))))
                single_response_desc['permutive_id'] = single_response_desc['permutive_id'].apply(
                    lambda x: None if x == [] else x[0])
                # drop unnecessary columns
                single_response_desc = single_response_desc.drop(['url_variables', 'survey_data'], axis=1)
                # set surveyID for the response
                single_response_desc['surveyID'] = survey
                # append single response description to final dataframe
                df_responses_desc = df_responses_desc.append(single_response_desc)
                # process each answer (row)
                for index, row in answers.iterrows():
                    # transform the row from dictionary to dataframe
                    single = pd.DataFrame.from_dict(row['survey_data'], orient='index')
                    # set the response ID
                    single['responseID'] = row['responseID']
                    # set the survey ID
                    single['surveyID'] = survey
                    # append answer to final answers dataframe
                    df_answers = df_answers.append(single)
        except:
            print('Tried unsuccessfully with survey id ', survey)
            continue
    print(f'Finished download of responses for NS Media Group surveys')
    if not df_responses_desc.empty:
        # set type int to df_responses_desc id columns
        df_responses_desc['surveyID'] = df_responses_desc['surveyID'].astype('int32')
        df_responses_desc['responseID'] = df_responses_desc['responseID'].astype('int32')
        # set type str to list/dictionary columns
        df_responses_desc['data_quality'] = df_responses_desc['data_quality'].astype('str')
        df_responses_desc['is_test_data'] = df_responses_desc['is_test_data'].astype('int32')
        df_responses_desc = transform_numeric_columns_types(df_responses_desc, 'link_id')
        df_responses_desc = transform_numeric_columns_types(df_responses_desc, 'dma')
        df_responses_desc = transform_numeric_columns_types(df_responses_desc, 'latitude')
        df_responses_desc = transform_numeric_columns_types(df_responses_desc, 'longitude')
        # normalize question column
        df_answers['question'] = df_answers['question'].apply(lambda x: BeautifulSoup(x, "lxml").get_text(strip=True))
        # rename question id column for each answer to questionID
        df_answers = df_answers.rename(columns={"id": "questionID"})
        # set type int to df_answers id columns
        df_answers['surveyID'] = df_answers['surveyID'].astype('int32')
        df_answers['responseID'] = df_answers['responseID'].astype('int32')
        if 'answer' in df_answers.columns:
            # set type str to list/dictionary columns
            df_answers['answer'] = df_answers['answer'].astype('str')
    return df_responses_desc, df_answers


def transform_answers_options(df_answers: pd.DataFrame) -> pd.DataFrame:
    """
    Unpack df_answers options column
    :param df_answers: dataframe containing the answers
    :return: dataframe with unpacked options
    """
    # TRANSFORM ANSWERS OPTIONS
    print(f'Transforming answers options')
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
    return df_answers_transformed


def transform_answers_of_simple_questions(df_answers_transformed: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the answers of questions without subquestions
    :param df_answers_transformed: dataframe with answers
    :return: dataframe with transformed answers
    """
    # get the answers without sub questions
    print(f'Transforming answers of questions without subquestions')
    df_subquestions_nan = df_answers_transformed[df_answers_transformed['subquestions'].isnull()]
    df_subquestions_nan = df_subquestions_nan.drop(['subquestions'], axis=1)
    df_subquestions_nan['questionID'] = df_subquestions_nan['questionID'].astype('int32')
    df_subquestions_nan['responseID'] = df_subquestions_nan['responseID'].astype('int32')
    # this is the answers final df to upload -> Responses
    df_subquestions_nan['surveyID'] = df_subquestions_nan['surveyID'].astype('int32')
    df_subquestions_nan['section_id'] = df_subquestions_nan['section_id'].astype('int32')
    if 'shown' in df_subquestions_nan.columns:
        df_subquestions_nan['shown'] = df_subquestions_nan['shown'].astype('boolean')
    df_subquestions_nan['answer_id'] = df_subquestions_nan['answer_id'].replace(np.nan, None)
    df_subquestions_nan['answer_id'] = df_subquestions_nan['answer_id'].apply(lambda x: re.sub(r"[^0-9]+", "", str(x)))
    df_subquestions_nan['answer_id'] = df_subquestions_nan['answer_id'].apply(lambda x: 0 if x == '' else x)
    df_subquestions_nan['answer_id'] = df_subquestions_nan['answer_id'].astype('int32')
    df_subquestions_nan['answer'] = df_subquestions_nan['answer'].astype(str)
    if 'comments' in df_subquestions_nan.columns:
        # clean comments column
        df_subquestions_nan['comments'] = df_subquestions_nan.comments.astype('str')
        df_subquestions_nan['comments'] = df_subquestions_nan['comments'].apply(
            lambda x: BeautifulSoup(x, "lxml").get_text(strip=True))
    # define de nul char (0x00)
    re_null = re.compile(pattern='\x00')
    # replace NUL characters
    df_subquestions_nan.replace(regex=re_null, value=' ', inplace=True)
    return df_subquestions_nan


def transform_table_questions_answers(df_subquestions_lvl_1: pd.DataFrame) -> pd.DataFrame:
    """
    Transform answers of table questions
    :param df_subquestions_lvl_1: dataframe with unpacked subquestions answers
    :return: dataframe with table questions answers
    """
    print(f'Transforming answers of table questions')
    # extract rows with questions of type TABLE (they have subquestions second level)
    df_tables = df_subquestions_lvl_1[['questionID', 'responseID', 'surveyID', 'subquestions']][
        df_subquestions_lvl_1['type'] == 'TABLE']
    # unpack df_table subquestions
    df_tables['unpacked_subsub'] = df_tables.apply(lambda x: unpack_subquestions(x), axis=1)
    df_table_answers = pd.concat(df_tables['unpacked_subsub'].tolist())
    df_table_answers = df_table_answers.rename(columns={'id': 'optionID'})
    # ready to upload to SubquestionsAnswers
    df_table_answers['optionID'] = df_table_answers['optionID'].astype('int32')
    df_table_answers['parent'] = df_table_answers['parent'].astype('int32')
    df_table_answers['subquestionID'] = df_table_answers['subquestionID'].astype('int32')
    return df_table_answers


def transform_subquestions_and_multitextbox_answers(df_subquestions_lvl_1: pd.DataFrame) -> (
pd.DataFrame, pd.DataFrame):
    """
    Transform subquestions answers and multitextbox answers
    :param df_subquestions_lvl_1: dataframe with unpacked subquestions answers
    :return: dataframe with transformed subquestions answers and dataframe with transformed multitextbox answers
    """
    print(f'Transforming subquestions and multitextbox answers')
    # get rest of subquestions answers
    df_without_tables = df_subquestions_lvl_1[df_subquestions_lvl_1['type'] != 'TABLE']
    df_without_tables = df_without_tables.drop(['subquestions', 'options'], axis=1)
    # extract rows with multi textbox type
    df_multitextbox = df_without_tables[df_without_tables['type'].isnull()]
    # TRANSFORM THEM into a dataframe dropping nan rows
    df_multi_columns = pd.DataFrame()
    for col in df_multitextbox.columns:
        if col.isdigit():
            a = pd.DataFrame(df_multitextbox[[col, 'responseID', 'surveyID']].dropna(subset=[f'{col}']))
            for item in a[[col, 'responseID', 'surveyID']].values:
                single = json_normalize(item[0])
                single['responseID'] = item[1]
                single['surveyID'] = item[2]
                df_multi_columns = df_multi_columns.append(single)
            # drop the numeric column from df_without_tables
            df_without_tables = df_without_tables.drop([col], axis=1)
    # Table MultiTextBoxResponses
    df_multi_columns = df_multi_columns.rename(columns={'id': 'optionID'})
    # Table SubquestionsResponses
    df_without_tables = df_without_tables.dropna()
    df_without_tables['questionID'] = df_without_tables['questionID'].astype('int32')
    df_without_tables['parent'] = df_without_tables['parent'].astype('int32')
    df_without_tables['answer_id'] = df_without_tables['answer_id'].astype('int32')
    df_without_tables['responseID'] = df_without_tables['responseID'].astype('int32')
    df_without_tables['surveyID'] = df_without_tables['surveyID'].astype('int32')
    return df_without_tables, df_multi_columns


def download_surveys_campaigns_data(surveys_lst: list) -> pd.DataFrame:
    """
    Downloads a dataframe with campaigns data for each survey in provided list
    :param surveys_lst: list of survey ids
    :return: dataframe with campaigns data
    """
    # create empty dataframe for campaigns
    df_campaigns = pd.DataFrame()
    print(f'Download campaigns for surveys')
    # collect campaign data for each survey
    for survey in surveys_lst:
        # set url
        url = f'{BASE_URL}/{survey}/surveycampaign?api_token={TOKEN}&api_token_secret={API_SECRET}'
        try:
            response_campaigns = requests.get(url, headers=HEADERS).json()
            # get number of pages for each survey
            nb_pages = response_campaigns['total_pages']
            # for each page collect data
            for page in range(nb_pages):
                # set url for specific page and survey
                url = f'{BASE_URL}/{survey}/surveycampaign?api_token={TOKEN}&api_token_secret={API_SECRET}&page={page + 1}'
                response_campaigns_page = requests.get(url, headers=HEADERS).json()
                # transform response into dataframe
                df_single_page = pd.DataFrame.from_dict(response_campaigns_page['data'])
                # set survey id
                df_single_page['surveyID'] = survey
                # append data to final dataframe
                df_campaigns = df_campaigns.append(df_single_page)
        except:
            print('Tried unsuccessfully with survey id ', survey)
            continue
    # drop theme columns
    if all(elem in df_campaigns.columns for elem in ['primary_theme_options','primary_theme_content']):
        df_campaigns.drop(columns=['primary_theme_options','primary_theme_content'],inplace=True)
    # rename campaign id column for each campaign to campaignID
    df_campaigns = df_campaigns.rename(columns={"id": "campaignID"})
    # set type int to df_campaigns id columns
    df_campaigns['surveyID'] = df_campaigns['surveyID'].astype('int32')
    df_campaigns['campaignID'] = df_campaigns['campaignID'].astype('int32')
    # set type datetime to df_campaigns date columns
    df_campaigns['date_created'] = pd.to_datetime(df_campaigns['date_created'])
    df_campaigns['date_modified'] = pd.to_datetime(df_campaigns['date_modified'])
    df_campaigns['SSL'] = df_campaigns['SSL'].apply(lambda x: True if x == 'True' else False)
    print(f'Finished download of campaigns for surveys')
    return df_campaigns


def download_surveys_reports_data(surveys_lst: list) -> (pd.DataFrame, list):
    """
    Download reports data for each survey in provided list
    :param surveys_lst:  list of survey ids
    :return: dataframe with reports data and a list with survey ids without reports
    """
    # create empty dataframe for reports
    df_reports = pd.DataFrame()
    # create empty list to store surveys without reports
    lst_reports = []
    print(f'Download reports for surveys')
    # for each survey collect report
    for survey in surveys_lst:
        # set url
        url = f'{BASE_URL}/{survey}/surveyreport?api_token={TOKEN}&api_token_secret={API_SECRET}'
        try:
            response_reports = requests.get(url, headers=HEADERS).json()
            # check if there is data
            if response_reports['result_ok']:
                # transform response into dataframe
                df_single_survey = pd.DataFrame.from_dict(response_reports['data'])
                # append report data to final dataframe
                df_reports = df_reports.append(df_single_survey)
            else:
                # append the survey without report to the list
                lst_reports.append(survey)
                print('There is no report for survey with ID: ', survey)
        except:
            print('Tried unsuccessfully with survey id ', survey)
            continue
    # rename campaign id column for each campaign to campaignID
    df_reports = df_reports.rename(columns={"id": "reportID"})
    # set type int to df_reports id columns
    df_reports['surveyID'] = df_reports['surveyID'].astype('int32')
    print(f'Finished download of reports for surveys')
    return df_reports, lst_reports


def transform_numeric_columns_types(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Transform numeric columns type to float
    :param df: dataframe with specific column
    :param column: name of the column to transform
    :return: dataframe with transformed column type
    """
    df[column] = df[column].apply(
        lambda x: np.nan if isinstance(x, type(None)) else x)
    df[column] = df[column].apply(lambda x: np.nan if x == '' else x)
    df[column] = df[column].astype('float32')
    return df


def download_surveys_statistics_data(surveys_lst: list) -> (pd.DataFrame, pd.DataFrame, list):
    """
    Download surveys statistics data for each survey in provided list
    :param surveys_lst: list of survey ids
    :return: dataframe with statistics data and dataframe with breakdown statistics data
    """
    # create empty dataframe for statistics
    df_statistics = pd.DataFrame()
    # create empty list to store surveys without statistics
    lst_statistics = []
    print(f'Download statistics for surveys')
    # for each survey collect statistic data
    for survey in surveys_lst:
        # set url
        url = f'{BASE_URL}/{survey}/surveystatistic?api_token={TOKEN}&api_token_secret={API_SECRET}'
        try:
            response_statistics = requests.get(url, headers=HEADERS).json()
            # check if there is data
            if response_statistics['result_ok']:
                # transform response into dataframe
                df_single_survey = pd.DataFrame.from_dict(response_statistics['data'])
                # set survey id
                df_single_survey['surveyID'] = survey
                # append statistic data to final dataframe
                df_statistics = df_statistics.append(df_single_survey)
            else:
                lst_statistics.append(survey)
                print('There are no statistics for survey with ID: ', survey)
        except:
            print('Tried unsuccessfully with survey id ', survey)
            continue
    print(f'Finished download of statistics for NS Media Group surveys')
    print('Transform statistics data')
    # rename id column to questionID
    df_statistics = df_statistics.rename(columns={'id': 'questionID'})
    # set type int to df_statistics id columns
    df_statistics['surveyID'] = df_statistics['surveyID'].astype('int32')
    df_statistics['questionID'] = df_statistics['questionID'].astype('int32')
    # extract breakdown data into a new dataframe
    df_breakdown_raw = df_statistics[['questionID', 'surveyID', 'breakdown']][~df_statistics['breakdown'].isnull()]
    # for each breakdown row extract labels, append survey and question id and append to final df_breakdown
    df_breakdown = pd.DataFrame()
    for index, row in df_breakdown_raw.iterrows():
        single = pd.DataFrame(row['breakdown'])
        single['questionID'] = row['questionID']
        single['surveyID'] = row['surveyID']
        df_breakdown = df_breakdown.append(single)
    df_breakdown['percentage'] = df_breakdown['percentage'].astype('float32')
    df_breakdown['count'] = df_breakdown['count'].astype('int32')
    df_breakdown['label'] = df_breakdown['label'].astype('str')
    # drop breakdown column from statistics dataframe
    df_statistics = df_statistics.drop('breakdown', axis=1)
    # rename column total responses
    df_statistics = df_statistics.rename(columns={'total responses': 'total_responses'})
    df_statistics = transform_numeric_columns_types(df_statistics, 'total_responses')
    df_statistics = transform_numeric_columns_types(df_statistics, 'sum')
    df_statistics = transform_numeric_columns_types(df_statistics, 'average')
    df_statistics['min'] = df_statistics['min'].apply(lambda x: x.replace(',', '') if isinstance(x, str) else x)
    df_statistics = transform_numeric_columns_types(df_statistics, 'min')
    df_statistics['max'] = df_statistics['max'].apply(lambda x: x.replace(',', '') if isinstance(x, str) else x)
    df_statistics = transform_numeric_columns_types(df_statistics, 'max')
    df_statistics['stdDev'] = df_statistics['stdDev'].apply(lambda x: x.replace(',', '') if isinstance(x, str) else x)
    df_statistics = transform_numeric_columns_types(df_statistics, 'stdDev')
    return df_statistics, df_breakdown, lst_statistics


def get_last_time(project_id, query_string) -> datetime:
    """
    Get max date from table.
    :param project_id: project ID
    :param query_string: SQL select query
    :return: max date from specific table
    """
    df_max = get_gbq_table(project_id, query_string)
    date_time = (df_max.max()).values[0]
    a = pd.to_datetime(date_time)
    b = a.tz_localize('utc')
    return b
