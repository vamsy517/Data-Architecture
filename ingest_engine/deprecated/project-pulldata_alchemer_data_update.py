import datetime
import pytz
from datetime import date, timedelta
import os

import pandas_gbq
import pandas as pd
import scipy.stats as stats
import numpy as np

from google.oauth2 import service_account
from google.cloud import bigquery

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from sqlalchemy import create_engine 

import requests
import json
from pandas.io.json import json_normalize

from bs4 import BeautifulSoup
import re

# schema for SurveysDescriptions
descriptions_schema = [
    {
        "name": "surveyID",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },
    {
        "name": "type",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "status",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "created_on",
        "type": "TIMESTAMP",
        "mode": "NULLABLE"
    },
    {
        "name": "modified_on",
        "type": "TIMESTAMP",
        "mode": "NULLABLE"
    },
    {
        "name": "forward_only",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },
    {
        "name": "languages",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "title",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "internal_title",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "title_ml",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {
                "name": "English",
                "type": "STRING",
                "mode": "NULLABLE"
            }
        ]
    },
    {
        "name": "theme",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "blockby",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "statistics",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {
                "name": "Partial",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "Complete",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "TestData",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "Deleted",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "Disqualified",
                "type": "INTEGER",
                "mode": "NULLABLE"
            }
        ]
    },
    {
        "name": "overall_quota",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "auto_close",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "links",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {
                "name": "default",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "campaign",
                "type": "STRING",
                "mode": "NULLABLE"
            }
            
        ]
    }
    
]




def get_credentials(project_id):
    ''' 
        Create a BigQuery client and authorize the connection to BigQuery
        Input:  
                project_id: project-pulldata
        Output:
                return big query client
    '''
    keyfile= '/home/ingest/credentials/gbq_credentials.json'
    credentials = service_account.Credentials.from_service_account_file(
        keyfile,
    )
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = project_id
    
    client = bigquery.Client.from_service_account_json(keyfile)
    return client


def delete_table(postgre_engine, gbq_client, dataset,table):
    '''
        Delete specific table from specific dataset in Postgre and BigQuery
        Input:
            postgre_engine: connection to postgree
            gbq_client: connection to gbq
            dataset: dataset which contains the table
            table: specific table to be deleted
        Output:
            return True if table is deleted successfully
    '''
    # Deleting table from postgre
    sql = f'drop table {dataset.lower()}."{table}"'
    postgre_engine.execute(sql)
    print(f'Table {table} is deleted from dataset {dataset} from Postgre') 
    # Deleting table from BQ
    table_ref = gbq_client.dataset(dataset).table(table)
    gbq_client.delete_table(table_ref)  
    print(f'Table {table} is deleted from dataset {dataset} from BigQuery')
    return True


def upload_data(postgre_engine, project_id, dataset, table, df, json_data):
    ''' 
        Upload df data to database (postgre/GBQ) 
        Input:
            postgre_engine: connection to Postgre
            project_id: project ID
            dataset: dataset to which we upload the df
            table: specific table which will contain the df
            df: dataframe to upload
            json_data: data for SurveysDescriptions table to be uploaded to GBQ
        Output:
            return True if upload is successfull
    '''
    # Upload to postgre
    df.to_sql(table,schema=dataset.lower(),
                          con=postgre_engine,method='multi', chunksize = 100000, if_exists = 'replace')
    print(f'Table {table} is successfully uploaded in the {dataset} dataset to Postgre')
    if table == 'SurveysDescriptions':
        # Upload to GBQ with specific schema
        gbq_client = get_credentials(project_id)
        job_config = bigquery.LoadJobConfig(schema=descriptions_schema)
        gbq_client.load_table_from_json(json_data, f'{dataset}.{table}', job_config=job_config).result()
    else:
        # Upload to GBQ
        df.to_gbq(destination_table=f'{dataset}.{table}',project_id = project_id, if_exists = 'replace')
    print(f'Table {table} is successfully uploaded in the {dataset} dataset to BigQuery')
    return True


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


def log_upload(dataset, table, process, lst_ids = None):
    '''
        log the tables 
        Input:
            dataset: dataset to which we upload the table
            table: specific table, which is logged
            process: True (in case the table is uploaded successfully) or False (in case there was no data to upload)
            lst_ids: list of survey IDs, for which there is either no report or statistics            
        Output:
            return True if logging is successfull
    '''
    with open ('/home/ingest/logs/update_surveys_processing_results.csv', 'a') as fl:
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # set message to be empty for all tables except Reports and Statistics
        message = ""
        # set message to hold message for missing reports
        if table == 'Reports':
            message = 'There are no reports for the following ids: '
        # set message to hold message for missing statistics
        elif table =='Statistics':
            message = 'There are no statistics for the following ids: '
        # check if the table is uploaded successfully
        if process:
            # check if there is non-empty list of IDs
            if lst_ids is not None:
                # log list of ids
                converted_id_list = [str(element) for element in lst_ids]
                inds_str = ",".join(converted_id_list)
                fl.write(str(curr_time) + '|' + dataset + '.' + table + '|' + message + inds_str +  '. Replaced successfully.\n')
            else:
                # log successful replacement
                fl.write(str(curr_time) + '|' + dataset + '.' + table + '|Replaced successfully.\n')      
        else:
            fl.write(str(curr_time) + '|' + dataset + '.' + table + '|There is no data, skipping replacement.\n')
    print(f'Processing for table {table} in dataset {dataset} logged successfully.')    
    return True


def replace_data(df, dataset, table, postgre_engine, gbq_client, project_id, lst_ids = None, json_data = None):
    ''' 
        Delete specific table from postgre and bq
        Upload the new df to postgre and bq
        Update metadata
        Log results
        Input:
            df: dataframe to upload
            dataset: dataset to which we upload the df
            table: specific table which will contain the df
            postgre_engine: connection to Postgre
            gbq_client: connection to BigQuery
            project_id: project ID 
            lst_ids: list of survey IDs, for which there is either no report or statistics 
            json_data: contains data for table SurveysDescriptions to be uploaded in GBQ
        Output:
            return True if all operations are successfull
    '''
    # check if dataframe is empty
    if df.empty:
        print(f'There is no data for {dataset}.{table}. Skipping replacement.')
        log_upload(dataset, table, False)
    else:
        # check if table exists in Postgre and BigQuery, if not skip deleting
        sql = 'SELECT * FROM '+ dataset+'.INFORMATION_SCHEMA.TABLES' 
        tables_list = pd.read_gbq(sql)['table_name'].tolist()
        if table in tables_list:
            # delete table CompanyListing from postgre and BigQuery
            delete_table(postgre_engine, gbq_client, dataset,table)
        # upload new table to postgre and bq
        upload_data(postgre_engine, project_id, dataset, table, df, json_data=json_data)
        # update the metadata for table
        update_metadata(df, dataset, table, postgre_engine, gbq_client, project_id)
        if (lst_ids is not None) and (len(lst_ids) > 0):
            # log updated IDs and Tables
            log_upload(dataset, table, True, lst_ids=lst_ids)
        else:
            # log updated table
            log_upload(dataset, table, True)
    return True

def lst_subquestions(subquestions):
    ''' 
        Input:
            subquestions: list of dictionaries with subquestions data
            
        Output:
            return list of all subquestion ids or None
    '''
    # check if the list of dictionaries is not np.nan
    if subquestions is not np.nan:
        if not isinstance(subquestions, list):
            # create empty list ot store the subquestions ids
            lst_subquestions=[]
            lst_subquestions.append(str(subquestions['JobTitle']['id']))
            lst_subquestions.append(str(subquestions['JobTitle_Other']['id']))
            # return string with all subquestion ids
            return ",".join(lst_subquestions)
        else:
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
        current_row = [[list(row.index)[i], k] if i == list(row.index).index('answer_id') else [list(row.index)[i], list(row.values)[i]] for i in range(len(list(row.values)))]
        current_row = [[list(row.index)[i], v['answer']] if i == list(row.index).index('answer') else [list(row.index)[i], list(row.values)[i]] for i in range(len(list(row.values)))]
        single = pd.DataFrame(current_row)
        single.set_index(0, inplace= True)
        single = single.T
        result = result.append(single)
    return result

# unpack subquestions which have other subquestions
def unpack_subquestions(row):
    df_ss = pd.DataFrame()
    for k, v in row['subquestions'].items():
        single = pd.DataFrame(v).T
        single['subquestionID'] = k
        single['responseID'] = row['responseID']
        single['surveyID'] = row['surveyID']
        df_ss = df_ss.append(single)
    return df_ss

def update_data():
    '''
        Download full Surveys data from Alchemer API, transform downloaded data, create new Surveys tables,
        delete old Surveys tables, upload new tables and update metadata.
    '''
     # set headers options to avoid detecting the scraper
    headers = {
	    'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0',
	    'ACCEPT' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
	    'ACCEPT-ENCODING' : 'gzip, deflate, br',
	    'ACCEPT-LANGUAGE' : 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
	    'REFERER' : 'https://www.google.com/'
	}
    # set project ID
    project_id = "project-pulldata"
    # create connection to postgree and GBQ
    # NOTE!!! ALSO instantiate clients / engines closer to when needed - connections terminated otherwise
    postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
    gbq_client = get_credentials(project_id)
    # set dataset
    dataset = 'Alchemer'
    # set token and api secret, used in the call to the Alchemer API
    token = '5eba4d051916f33ab54c5e05849fb6f87d8f573c90f87fe1cb'
    api_secret = 'A9oIXVYJxsMWU'
    # get list of surveys
    print(f'Download full list of NS Media Group surveys')
    # set url to get survey information
    url = f'https://api.alchemer.eu/v5/survey?api_token={token}&api_token_secret={api_secret}'
    # get survey information from first page 
    response_survey = requests.get(url).json()
    # get number of pages
    nb_pages = response_survey['total_pages']
    # create empty dataframe to store all surveys information
    df_surveys = pd.DataFrame()
    # for each page of the response collect the information about surveys
    for page in range(nb_pages):
        #set url for each page
        url = f'https://api.alchemer.eu/v5/survey?api_token={token}&api_token_secret={api_secret}&page={page+1}'
        # get data for surveys from the corresponding page
        response_survey_page = requests.get(url).json()
        # transform response into dataframe
        df_single_page = pd.DataFrame.from_dict(response_survey_page['data'])
        # filter only surveys from ns media team
        df_single_page = df_single_page[df_single_page['team'] == '644416'] 
        # append data from signle page to final dataframe
        df_surveys = df_surveys.append(df_single_page)
    # drop statistics and links columns from df_surveys
    df_surveys = df_surveys.drop(['statistics', 'links'], axis = 1)
    # rename columns
    df_surveys = df_surveys.rename(columns={'id':'surveyID','_type':'type','_subtype': 'subtype'})
    # cast some columns to integer
    df_surveys['surveyID'] = df_surveys['surveyID'].astype('int32')
    df_surveys['team'] = df_surveys['team'].astype('int32')
    # Set type datetime to date columns because it is string
    df_surveys['created_on'] = pd.to_datetime(df_surveys['created_on'])
    df_surveys['modified_on'] = pd.to_datetime(df_surveys['modified_on'])
    df_surveys = df_surveys[~df_surveys['title'].str.startswith('Test -')]
    df_surveys = df_surveys[~df_surveys['title'].str.startswith('Test-')]
    print(f'Finished download of full list of NS Media Group surveys')
    # replace table Surveys in Postgre and GBQ
    replace_data(df_surveys, dataset, 'Surveys', postgre_engine, gbq_client, project_id)
    # get a list of all surveys
    surveys_lst = df_surveys['surveyID'].tolist()
    # Remove the id of the Pardot integration survey
    surveys_lst.remove(90299441)
    print(f'Download descriptions for NS Media Group surveys')
    # create empty list to store the json, used to upload the table SurveysDescriptions to GBQ
    json_data = []
    # for each survey of the list extract data
    for survey in surveys_lst:
        # set url for each survey
        url = f'https://api.alchemer.eu/v5/survey/{survey}?api_token={token}&api_token_secret={api_secret}'
        # TRY  to get the description for the survey, in case it fails, continiue with next id
        try:
            # get response
            response_desc = requests.get(url).json()
            # copy data dictionary from response
            data_dict = response_desc['data'].copy()
            # remove pages section, as it is already processed
            data_dict.pop('pages', None)
            # remove team section, as it contains data only for NS Media Group team
            data_dict.pop('team', None)
            # rename id to surveyID and set it to integer
            data_dict_rename = dict(('surveyID', int(v)) if k == 'id' else (k, v) for k, v in data_dict.items())
            # transform languages to string, as it is a list
            data_dict_transform = dict((k, ",".join(v)) if k == 'languages' else (k, v) for k, v in data_dict_rename.items())
            # TODO: address bug from Alchemer where kye in a dictionary is empty
            # remove "" key from statistics dictionary
            if data_dict_transform['statistics'] is not None:
                dict_statistics = dict((k, v) for k,v in data_dict_transform['statistics'].items() if k!="")
                data_dict_transform['statistics'] = dict_statistics
            json_data.append(data_dict_transform)
        except:
            print('Tried unsuccessfully with survey id ', survey)
            continue
    # create a new dataframe from the json data, which will be used
    # to upload the table SurveysDescriptions to Postgre
    df_json_normalized_data = json_normalize(json_data)
    # drop statistics columns
    df_json_normalized_data = df_json_normalized_data.drop('statistics',axis=1)
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
    print(f'Finished download of descriptions for NS Media Group surveys')
    # replace table SurveysDescriptions in Postgre and GBQ
    replace_data(df_description, dataset, 'SurveysDescriptions', postgre_engine, gbq_client, project_id, json_data=json_data)
    # GET DATA FOR SUB OBJECT QUESTIONS
    # create empty dataframe for questions
    df_questions_raw = pd.DataFrame()
    print(f'Download questions for NS Media Group surveys')
    # for each survey collect questions
    for survey in surveys_lst:
        # set url to get number of pages for the corresponding survey
        url = f'https://api.alchemer.eu/v5/survey/{survey}/surveyquestion?api_token={token}&api_token_secret={api_secret}'
        # TRY to get the data for sub object question for the survey, in case it fails continue with next survey
        try:
            response_question = requests.get(url).json()
            # get number of pages
            nb_pages = response_question['total_pages']
            # for each page collect the questions
            for page in range(nb_pages):
                # set url for each page
                url = f'https://api.alchemer.eu/v5/survey/{survey}/surveyquestion?api_token={token}&api_token_secret={api_secret}&page={page+1}'
                response_question_page = requests.get(url).json()
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
    df_questions['has_showhide_deps'] = df_questions['has_showhide_deps'].astype('bool')
    print(f'Finished download of questions for NS Media Group surveys')
    # replace table Questions in Postgre and GBQ
    replace_data(df_questions, dataset, 'Questions', postgre_engine, gbq_client, project_id)
    # GET DATA FOR SUB OBJECT QUESTION OPTIONS
    # create empty dataframe for questions options
    print(f'Download questions options for NS Media Group surveys')
    df_options_raw = pd.DataFrame()
    # for each survey and question pair of ids 
    for index, row in df_questions.iterrows():
        # set url
        url = f'https://api.alchemer.eu/v5/survey/{row["surveyID"]}/surveyquestion/{row["questionID"]}/surveyoption?api_token={token}&api_token_secret={api_secret}'
        response_options = requests.get(url).json()
        # get number of pages
        nb_pages = response_options['total_pages']
        # for eah page get options data
        for page in range(nb_pages):
            url = f'https://api.alchemer.eu/v5/survey/{row["surveyID"]}/surveyquestion/{row["questionID"]}/surveyoption?api_token={token}&api_token_secret={api_secret}&page={page+1}'
            response_options_page = requests.get(url).json()
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
    df_options_raw = df_options_raw.drop(['properties','title','image'], axis = 1)
    # rename columns
    df_options_raw = df_options_raw.rename(columns={'id':'optionID'})
    # set type int to df_options id column
    df_options_raw['optionID'] = df_options_raw['optionID'].astype('int32')
    df_options_raw['questionID'] = df_options_raw['questionID'].astype('int32')
    df_options_raw['page'] = df_options_raw['page'].astype('int32')
    print(f'Finished download of questions options for NS Media Group surveys')
    # replace table QuestionsOptions in Postgre and GBQ
    replace_data(df_options_raw, dataset, 'QuestionsOptions', postgre_engine, gbq_client, project_id)
    # GET RESPONSES DATA
    # create empty dataframe for responses descriptions
    df_responses_desc = pd.DataFrame()
    # create empty dataframe for answers for each response
    df_answers = pd.DataFrame()
    print(f'Download responses for NS Media Group surveys')
    # for each survey collect the responses
    for survey in surveys_lst:
        # set response url petition
        url = f'https://api.alchemer.eu/v5/survey/{survey}/surveyresponse?api_token={token}&api_token_secret={api_secret}'
        # try to get responses for survey, in case it fails continie with next survey id
        try:
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
                # separate responseID and survey data to process them separately
                answers = single_response_desc[['responseID','survey_data']]
                # get the permutive user id
                single_response_desc['permutive_id'] = single_response_desc['url_variables'].apply(lambda x: x if x == [] else (re.findall("{'key': 'permutive_user_id', 'value': '(.*?)', 'type': 'url'}", str(x))))
                single_response_desc['permutive_id'] = single_response_desc['permutive_id'].apply(lambda x: None if x == [] else x[0])
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
        except:
            print('Tried unsuccessfully with survey id ', survey)
            continue
    print(f'Finished download of responses for NS Media Group surveys')
    # set type int to df_responses_desc id columns
    df_responses_desc['surveyID'] = df_responses_desc['surveyID'].astype('int32')
    df_responses_desc['responseID'] = df_responses_desc['responseID'].astype('int32')
    # set type datetime to date column
    df_responses_desc['date_submitted'] = pd.to_datetime(df_responses_desc['date_submitted'])
    df_responses_desc['date_started'] = pd.to_datetime(df_responses_desc['date_started'])
    # set type str to list/dictionary columns
    df_responses_desc['data_quality'] = df_responses_desc['data_quality'].astype('str')
    df_responses_desc['is_test_data'] = df_responses_desc['is_test_data'].astype('int32')
    df_responses_desc['link_id'] = df_responses_desc['link_id'].replace(np.nan, None)
    df_responses_desc['link_id'] = df_responses_desc['link_id'].astype('int32')
    df_responses_desc['dma'] = df_responses_desc['dma'].apply(lambda x: np.nan if isinstance(x, type(None)) else x)
    df_responses_desc['dma'] = df_responses_desc['dma'].apply(lambda x: np.nan if x == '' else x)
    df_responses_desc['dma'] = df_responses_desc['dma'].astype('float32')
    df_responses_desc['latitude'] = df_responses_desc['latitude'].apply(lambda x: np.nan if isinstance(x, type(None)) else x)
    df_responses_desc['latitude'] = df_responses_desc['latitude'].apply(lambda x: np.nan if x == '' else x)
    df_responses_desc['latitude'] = df_responses_desc['latitude'].astype('float32')
    df_responses_desc['longitude'] = df_responses_desc['longitude'].apply(lambda x: np.nan if isinstance(x, type(None)) else x)
    df_responses_desc['longitude'] = df_responses_desc['longitude'].apply(lambda x: np.nan if x == '' else x)
    df_responses_desc['longitude'] = df_responses_desc['longitude'].astype('float32')
    # replace table ResponsesDescriptions in Postgre and GBQ
    replace_data(df_responses_desc, dataset, 'ResponsesDescriptions', postgre_engine, gbq_client, project_id)
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
    df_answers['answer'] = df_answers['answer'].astype('str')
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
    # TRANSFORM ANSWERS SUBQUESTINONS
    # get the answers without subquestions
    print(f'Transforming subquestions answers')
    df_subquestions_nan = df_answers_transformed[df_answers_transformed['subquestions'].isnull()]
    df_subquestions_nan = df_subquestions_nan.drop(['subquestions'], axis = 1)
    df_subquestions_nan['questionID'] = df_subquestions_nan['questionID'].astype('int32')
    #df_subquestions_nan['section_id'] = df_subquestions_nan['section_id'].astype('int32')
    df_subquestions_nan['responseID'] = df_subquestions_nan['responseID'].astype('int32')
    # this is the answers final df to upload -> Responses
    df_subquestions_nan['surveyID'] = df_subquestions_nan['surveyID'].astype('int32')
    df_subquestions_nan['section_id'] = df_subquestions_nan['section_id'].astype('int32')
    df_subquestions_nan['shown'] = df_subquestions_nan['shown'].astype('boolean')
    df_subquestions_nan['answer_id'] = df_subquestions_nan['answer_id'].replace(np.nan,None)
    df_subquestions_nan['answer_id'] = df_subquestions_nan['answer_id'].apply(lambda x: re.sub(r"[^0-9]+", "",str(x)))
    df_subquestions_nan['answer_id'] = df_subquestions_nan['answer_id'].apply(lambda x: 0 if x == '' else x)
    df_subquestions_nan['answer_id'] = df_subquestions_nan['answer_id'].astype('int32')
    # clean comments column
    df_subquestions_nan['comments'] = df_subquestions_nan.comments.astype('str')
    df_subquestions_nan['comments'] = df_subquestions_nan['comments'].apply(lambda x: BeautifulSoup(x, "lxml").get_text(strip = True))
    # define de nul char (0x00)
    re_null = re.compile(pattern='\x00')
    # replace NUL characters
    df_subquestions_nan.replace(regex=re_null,value=' ', inplace=True)
    # replace table Responses in Postgre and GBQ
    replace_data(df_subquestions_nan, dataset, 'Responses', postgre_engine, gbq_client, project_id)
    # get rows with subquestions
    df_subquestions_raw = df_answers_transformed[['responseID','surveyID','subquestions']][~df_answers_transformed['subquestions'].isnull()]
    # unpack first level subquestions
    df1 = (pd.concat({i: json_normalize(x.values(),max_level=0) for i, x in df_subquestions_raw.pop('subquestions').items()})
             .reset_index(level=1, drop=True)
             .join(df_subquestions_raw)
             .reset_index(drop=True))
    df1 = df1.rename(columns={'id': 'questionID'})
    # extract rows with questions of type TABLE (they have subquestions second level)
    df_tables = df1[['questionID','responseID','surveyID','subquestions']][df1['type'] == 'TABLE']
    # unpack df_table subquestions
    df_tables['unpacked_subsub'] = df_tables.apply(lambda x: unpack_subquestions(x), axis = 1)
    df_unpacked_sub = pd.concat(df_tables['unpacked_subsub'].tolist())
    df_unpacked_sub = df_unpacked_sub.rename(columns={'id':'optionID'})
    # ready to upload to SubquestionsAnswers
    df_unpacked_sub['optionID'] = df_unpacked_sub['optionID'].astype('int32')
    df_unpacked_sub['parent'] = df_unpacked_sub['parent'].astype('int32')
    df_unpacked_sub['subquestionID'] = df_unpacked_sub['subquestionID'].astype('int32')
    # replace table TableResponses in Postgre and GBQ
    replace_data(df_unpacked_sub, dataset, 'TableResponses', postgre_engine, gbq_client, project_id)
    print(f'Transforming MultiTextBox answers')
    # get rest of subquestion answers
    df_without_tables = df1[df1['type'] != 'TABLE']
    df_without_tables = df_without_tables.drop(['subquestions','options'], axis = 1)
    # extract rows with multitextbox type
    df_multitextbox = df_without_tables[df_without_tables['type'].isnull()]
    # TRANSFORM THEM into a dataframe dropping nan rows
    df_multi_columns = pd.DataFrame()
    for col in df_multitextbox.columns:
        if col.isdigit():
            a = pd.DataFrame(df_multitextbox[[col,'responseID','surveyID']].dropna(subset=[f'{col}']))
            for item in a[[col,'responseID','surveyID']].values:
                single = json_normalize(item[0])
                single['responseID'] = item[1] 
                single['surveyID'] = item[2]
                df_multi_columns = df_multi_columns.append(single)
            # drop the numeric column from df_without_tables
            df_without_tables = df_without_tables.drop([col], axis = 1)
    # Table MultiTextBoxResponses
    df_multi_columns = df_multi_columns.rename(columns={'id':'optionID'})
    # Table SubquestionsResponses
    df_without_tables = df_without_tables.dropna()
    df_without_tables['questionID'] = df_without_tables['questionID'].astype('int32')
    df_without_tables['parent'] = df_without_tables['parent'].astype('int32')
    df_without_tables['answer_id'] = df_without_tables['answer_id'].astype('int32')
    df_without_tables['responseID'] = df_without_tables['responseID'].astype('int32')
    df_without_tables['surveyID'] = df_without_tables['surveyID'].astype('int32')
    # replace table SubquestionsResponses in Postgre and GBQ
    replace_data(df_without_tables, dataset, 'SubquestionsResponses', postgre_engine, gbq_client, project_id)
    # replace table MultiTextBoxResponses in Postgre and GBQ
    replace_data(df_multi_columns, dataset, 'MultiTextBoxResponses', postgre_engine, gbq_client, project_id)
    # GET CAMPAIGN DATA
    # create empty dataframe for campaigns
    df_campaigns = pd.DataFrame()
    print(f'Download campaigns for NS Media Group surveys')
    # collect campaign data for each survey
    for survey in surveys_lst:
        # set url
        url = f'https://api.alchemer.eu/v5/survey/{survey}/surveycampaign?api_token={token}&api_token_secret={api_secret}'
        try:
            response_campaigns = requests.get(url).json()
            # get number of pages for each survey
            nb_pages = response_campaigns['total_pages']
            # for each page collect data
            for page in range(nb_pages):
                # set url for specific page and survey
                url = f'https://api.alchemer.eu/v5/survey/{survey}/surveycampaign?api_token={token}&api_token_secret={api_secret}&page={page+1}'
                response_campaigns_page = requests.get(url).json()
                # transform response into dataframe
                df_single_page = pd.DataFrame.from_dict(response_campaigns_page['data'])
                # set survey id
                df_single_page['surveyID'] = survey
                # append data to final dataframe
                df_campaigns = df_campaigns.append(df_single_page)
        except:
            print('Tried unsuccessfully with survey id ', survey)
            continue
    # rename campaign id column for each campaign to campaignID
    df_campaigns = df_campaigns.rename(columns={"id": "campaignID"})
    # set type int to df_campaigns id columns
    df_campaigns['surveyID'] = df_campaigns['surveyID'].astype('int32')
    df_campaigns['campaignID'] = df_campaigns['campaignID'].astype('int32')
    # set type datetime to df_campaigns date columns
    df_campaigns['date_created'] = pd.to_datetime(df_campaigns['date_created'])
    df_campaigns['date_modified'] = pd.to_datetime(df_campaigns['date_modified'])
    df_campaigns['SSL'] = df_campaigns['SSL'].apply(lambda x:  True if x == 'True' else False)
    print(f'Finished download of campaigns for NS Media Group surveys')
    # replace table Campaigns in Postgre and GBQ
    replace_data(df_campaigns, dataset, 'Campaigns', postgre_engine, gbq_client, project_id)
    # GET REPORTS DATA
    # create empty dataframe for reports
    df_reports = pd.DataFrame()
    # create empty list to store surveys without reports
    lst_reports = []
    print(f'Download reports for NS Media Group surveys')
    # for each survey collect report
    for survey in surveys_lst:
        # set url
        url = f'https://api.alchemer.eu/v5/survey/{survey}/surveyreport?api_token={token}&api_token_secret={api_secret}'
        try:
            response_reports = requests.get(url).json()
            # check if there is data
            if response_reports['result_ok'] == True:
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
    print(f'Finished download of reports for NS Media Group surveys')
    # replace table Reports in Postgre and GBQ
    replace_data(df_reports, dataset, 'Reports', postgre_engine, gbq_client, project_id,lst_ids = lst_reports)
    # GET STATISTIC DATA
    # create empty dataframe for statistics
    df_statistics = pd.DataFrame()
    # create empty list to store surveys without statistics
    lst_statistics = []
    print(f'Download statistics for NS Media Group surveys')
    # for each survey collect statistic data
    for survey in surveys_lst:
        # set url
        url = f'https://api.alchemer.eu/v5/survey/{survey}/surveystatistic?api_token={token}&api_token_secret={api_secret}'
        try:
            response_statistics = requests.get(url).json()
            # check if there is data 
            if response_statistics['result_ok'] == True:
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
    # rename id column to questionID
    df_statistics = df_statistics.rename(columns={'id': 'questionID'})
    # set type int to df_statistics id columns
    df_statistics['surveyID'] = df_statistics['surveyID'].astype('int32')
    df_statistics['questionID'] = df_statistics['questionID'].astype('int32')
    # extract breakdown data into a new dataframe
    df_breakdown_raw = df_statistics[['questionID','surveyID','breakdown']][~df_statistics['breakdown'].isnull()]
    # for each breakdown row extract labels, append survey and question id and append to final df_breakdown
    df_breakdown = pd.DataFrame()
    for index, row in df_breakdown_raw.iterrows():
        single = pd.DataFrame(row['breakdown'])
        single['questionID'] = row['questionID']
        single['surveyID'] = row['surveyID']
        df_breakdown = df_breakdown.append(single)
    df_breakdown['percentage'] = df_breakdown['percentage'].astype('float32') 
    df_breakdown['count'] = df_breakdown['count'].astype('int32') 
    # drop breakdown column from statistics dataframe
    df_statistics = df_statistics.drop('breakdown', axis = 1)
    # rename column total responses
    df_statistics = df_statistics.rename(columns={'total responses':'total_responses'})
    df_statistics['total_responses'] = df_statistics['total_responses'].apply(lambda x: np.nan if isinstance(x, type(None)) else x)
    df_statistics['total_responses'] = df_statistics['total_responses'].apply(lambda x: np.nan if x == '' else x)
    df_statistics['total_responses'] = df_statistics['total_responses'].astype('float32')
    df_statistics['sum'] = df_statistics['sum'].apply(lambda x: np.nan if isinstance(x, type(None)) else x)
    df_statistics['sum'] = df_statistics['sum'].apply(lambda x: np.nan if x == '' else x)
    df_statistics['sum'] = df_statistics['sum'].astype('float32')
    df_statistics['average'] = df_statistics['average'].apply(lambda x: np.nan if isinstance(x, type(None)) else x)
    df_statistics['average'] = df_statistics['average'].apply(lambda x: np.nan if x == '' else x)
    df_statistics['average'] = df_statistics['average'].astype('float32')
    df_statistics['min'] = df_statistics['min'].apply(lambda x: x.replace(',','') if isinstance(x,str) else x)
    df_statistics['min'] = df_statistics['min'].apply(lambda x: np.nan if isinstance(x, type(None)) else x)
    df_statistics['min'] = df_statistics['min'].apply(lambda x: np.nan if x == '' else x)
    df_statistics['min'] = df_statistics['min'].astype('float32')
    df_statistics['max'] = df_statistics['max'].apply(lambda x: x.replace(',','') if isinstance(x,str) else x)
    df_statistics['max'] = df_statistics['max'].apply(lambda x: np.nan if isinstance(x, type(None)) else x)
    df_statistics['max'] = df_statistics['max'].apply(lambda x: np.nan if x == '' else x)
    df_statistics['max'] = df_statistics['max'].astype('float32')
    df_statistics['stdDev'] = df_statistics['stdDev'].apply(lambda x: x.replace(',','') if isinstance(x,str) else x)
    df_statistics['stdDev'] = df_statistics['stdDev'].apply(lambda x: np.nan if isinstance(x, type(None)) else x)
    df_statistics['stdDev'] = df_statistics['stdDev'].apply(lambda x: np.nan if x == '' else x)
    df_statistics['stdDev'] = df_statistics['stdDev'].astype('float32')
    # replace table Statistics in Postgre and GBQ
    replace_data(df_statistics, dataset, 'Statistics', postgre_engine, gbq_client, project_id,lst_ids = lst_statistics)
    # replace table BreakdownStatistics in Postgre and GBQ
    replace_data(df_breakdown, dataset, 'BreakdownStatistics', postgre_engine, gbq_client, project_id,lst_ids = lst_statistics)  
    return True

default_args = {
    'owner': 'project-pulldata_alchemer_data_update',
    'start_date': datetime.datetime(2021, 1, 17, 20, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_alchemer_data_update',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 22 * * *',
     ) as dag:
    
    
    alchemer_update = PythonOperator(task_id='update_data',
                               python_callable=update_data)

alchemer_update