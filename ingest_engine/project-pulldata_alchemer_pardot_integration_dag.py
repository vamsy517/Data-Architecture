from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
from pardot_utils.utils import integration_log, get_api_key
from pardot_alchemer_integration.utils import get_integration_data, get_email_df, get_list_data_api, get_info_user, \
    get_all_ids, get_info, check_if_mail_exist, update_pardot, update_all_lists, add_pardot
from pardot_utils.constants import GET_BATCH_URL, LISTS_API, B2B_EMAIL, B2B_PASSWORD, B2B_USER_KEY
from ingest_utils.database_gbq import upload_table_to_gbq
from ingest_utils.database_postgre import upload_table_to_postgre
from ingest_utils.constants import PULLDATA_PROJECT_ID, POSTGRE_AUTH
from ingest_utils.metadata import update_metadata


def update_create_prospects() -> bool:
    """
    main function for update or add new prospect users from Alchemer survey
    :return: True if operations are successful
    """
    dataset = 'AlchemerPardotIntegration'
    # get all tables from Alchemer survey
    data_survey = get_integration_data([90271405])
    if data_survey:
        df_questions, df_options_raw, df_responses_desc, df_final_sub_answers, df_answers_transformed = data_survey
        # get dataframe with all provided emails, we exclude respondents without emails
        email_df = get_email_df(df_final_sub_answers)
        user_key = B2B_USER_KEY
        # get new api key
        api_key = get_api_key(B2B_EMAIL, B2B_PASSWORD, user_key)
        # dataframe with all Lists for 2020
        url = GET_BATCH_URL.format(pardot_api=LISTS_API)
        data_list = get_list_data_api(url)
        # iterate for every row of email dataframe
        count_new_users = 0
        count_updates = 0
        for index, row in email_df.iterrows():
            # get the information from the survey by respondent id
            forename, surname, job_title, company, country, list_checkbox = get_info_user(row.responseID,
                                                                                          df_responses_desc,
                                                                                          df_answers_transformed,
                                                                                          df_final_sub_answers)
            # get all IDS for subscriptions that respondent is selected to subscribe from Pardot data with all lists
            # we need this ID to create and update subscriptions with Pardot API
            # the data is in dict format
            dict_ids, all_ids_list = get_all_ids(list_checkbox, data_list)
            print(all_ids_list)
            # get info for job_title, company, country to dict format
            dict_info = get_info(job_title, company, country)
            # check if email exist if not return false if exist return the id of prospect user form Pardot system
            update_id = check_if_mail_exist(row.answer, api_key, user_key)
            print(dict_ids)
            print(dict_info)
            if update_id:
                print('update')
                count_updates += 1
                # update the user
                update_pardot(api_key, user_key, update_id, forename, surname, dict_info, dict_ids)
                update_all_lists(api_key, user_key, update_id, all_ids_list)
            else:
                print('create')
                count_new_users += 1
                # create new user
                add_pardot(api_key, user_key, row.answer, forename, surname, dict_info, dict_ids)
                update_id = check_if_mail_exist(row.answer, api_key, user_key)
                if update_id:
                    update_all_lists(api_key, user_key, update_id, all_ids_list)
                else:
                    print('ERROR the User is not created')
        # record log file
        integration_log(count_new_users, count_updates)
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
            upload_table_to_gbq(PULLDATA_PROJECT_ID, dataframe, dataset, tablename, method=mode)
            upload_table_to_postgre(POSTGRE_AUTH, dataframe, dataset, tablename, method=mode)
            update_metadata(PULLDATA_PROJECT_ID, POSTGRE_AUTH, dataframe, dataset, tablename)
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

