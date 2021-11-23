import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pandas.io.json import json_normalize
# from alchemer_data_update.utils import download_surveys_data, replace_data, download_surveys_description_data,\
#     download_surveys_questions_data, download_surveys_questions_options_data, download_survey_responses_data,\
#     transform_answers_options, transform_answers_of_simple_questions, transform_table_questions_answers,\
#     transform_subquestions_and_multitextbox_answers, download_surveys_campaigns_data, download_surveys_reports_data,\
#     download_surveys_statistics_data
from alchemer_data_update.utils import *
from alchemer_data_update.constants import GD_TEAM_ID, GD_DATASET, SURVEYS_DESCRIPTIONS_SCHEMA
from ingest_utils.constants import PULLDATA_PROJECT_ID, POSTGRE_AUTH, LOGS_FOLDER, UPDATE_SURVEYS_GD_PROCESSING_RESULTS


def download_and_replace_gd_alchemer_data() -> bool:
    """
    Download full Surveys data from Alchemer API, transform downloaded data, create new Surveys tables,
        delete old Surveys tables, upload new tables and update metadata.
    :return: return True if operations were successful
    """
    log_file = f'{LOGS_FOLDER}{UPDATE_SURVEYS_GD_PROCESSING_RESULTS}'
    # download surveys dataframe
    df_surveys = download_surveys_data(GD_TEAM_ID)
    # replace table Surveys in Postgre and GBQ
    replace_data(PULLDATA_PROJECT_ID, POSTGRE_AUTH, df_surveys, GD_DATASET, 'Surveys', log_file)
    # get a list of all surveys
    surveys_lst = df_surveys['surveyID'].tolist()
    # download surveys description dataframe
    df_description, description_json = download_surveys_description_data(surveys_lst)
    # replace table SurveysDescriptions in Postgre and GBQ
    replace_data(PULLDATA_PROJECT_ID, POSTGRE_AUTH, df_description, GD_DATASET, 'SurveysDescriptions',
                 log_file, schema=SURVEYS_DESCRIPTIONS_SCHEMA, json_data=description_json)
    # download surveys question dataframe
    df_questions = download_surveys_questions_data(surveys_lst)
    # replace table Questions in Postgre and GBQ
    replace_data(PULLDATA_PROJECT_ID, POSTGRE_AUTH, df_questions, GD_DATASET, 'Questions', log_file)
    # download surveys questions options data
    df_options = download_surveys_questions_options_data(df_questions)
    # replace table QuestionsOptions in Postgre and GBQ
    replace_data(PULLDATA_PROJECT_ID, POSTGRE_AUTH, df_options, GD_DATASET, 'QuestionsOptions', log_file)
    # download surveys responses data
    df_responses_desc, df_answers = download_survey_responses_data(surveys_lst)
    # replace table ResponsesDescriptions in Postgre and GBQ
    replace_data(PULLDATA_PROJECT_ID, POSTGRE_AUTH, df_responses_desc, GD_DATASET, 'ResponsesDescriptions', log_file)
    # unpack options
    df_answers_transformed = transform_answers_options(df_answers)
    # transform answers of simple questions (without subquestions)
    df_subquestions_nan = transform_answers_of_simple_questions(df_answers_transformed)
    # replace table Responses in Postgre and GBQ
    replace_data(PULLDATA_PROJECT_ID, POSTGRE_AUTH, df_subquestions_nan, GD_DATASET, 'Responses', log_file)
    # get rows with subquestions
    df_subquestions_raw = df_answers_transformed[['responseID', 'surveyID',
                                                  'subquestions']][~df_answers_transformed['subquestions'].isnull()]
    # unpack first level subquestions
    df_subquestions_lvl_1 = (pd.concat({i: json_normalize(x.values(), max_level=0)
                                        for i, x in df_subquestions_raw.pop('subquestions').items()})
                             .reset_index(level=1, drop=True)
                             .join(df_subquestions_raw)
                             .reset_index(drop=True))
    df_subquestions_lvl_1 = df_subquestions_lvl_1.rename(columns={'id': 'questionID'})
    print(f'Transforming MultiTextBox answers')
    df_multi_columns = pd.DataFrame()
    for col in df_subquestions_lvl_1.columns:
        if col.isdigit():
            a = pd.DataFrame(df_subquestions_lvl_1[[col, 'responseID', 'surveyID']].dropna(subset=[f'{col}']))
            for item in a[[col, 'responseID', 'surveyID']].values:
                single = json_normalize(item[0])
                single['responseID'] = item[1]
                single['surveyID'] = item[2]
                df_multi_columns = df_multi_columns.append(single)
    df_multi_columns = df_multi_columns.rename(columns={'id': 'optionID'})
    replace_data(PULLDATA_PROJECT_ID, POSTGRE_AUTH, df_multi_columns, GD_DATASET, 'MultiTextBoxResponses', log_file)
    # GET CAMPAIGN DATA
    df_campaigns = download_surveys_campaigns_data(surveys_lst)
    # replace table Campaigns in Postgre and GBQ
    replace_data(PULLDATA_PROJECT_ID, POSTGRE_AUTH, df_campaigns, GD_DATASET, 'Campaigns', log_file)
    # GET REPORTS DATA
    df_reports, lst_reports = download_surveys_reports_data(surveys_lst)
    # replace table Reports in Postgre and GBQ
    replace_data(PULLDATA_PROJECT_ID, POSTGRE_AUTH, df_reports, GD_DATASET, 'Reports', log_file, lst_ids=lst_reports)
    # GET STATISTIC DATA
    df_statistics, df_breakdown, lst_statistics = download_surveys_statistics_data(surveys_lst)
    # replace table Statistics in Postgre and GBQ
    replace_data(PULLDATA_PROJECT_ID, POSTGRE_AUTH, df_statistics, GD_DATASET, 'Statistics', log_file,
                 lst_ids=lst_statistics)
    # replace table BreakdownStatistics in Postgre and GBQ
    replace_data(PULLDATA_PROJECT_ID, POSTGRE_AUTH, df_breakdown, GD_DATASET, 'BreakdownStatistics', log_file,
                 lst_ids=lst_statistics)
    return True


default_args = {
    'owner': 'project-pulldata_alchemer_gd_data_update',
    'start_date': datetime.datetime(2021, 1, 17, 20, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_alchemer_gd_data_update',
         catchup=False,
         default_args=default_args,
         schedule_interval='0 5 * * *',
         ) as dag:
    alchemer_gd_update = PythonOperator(task_id='download_and_replace_gd_alchemer_data',
                                        python_callable=download_and_replace_gd_alchemer_data)

alchemer_gd_update
