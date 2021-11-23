import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
import time
import pytz
from permutive_pardot_match.utils import fix_log_error, get_prospect_data
from permutive_pardot_match.gbq_queries import QUERY_EMAILS
from pardot_utils.constants import B2B_DATASET, B2B_EMAIL, B2B_PASSWORD, B2B_USER_KEY
from ingest_utils.database_gbq import get_gbq_table, upload_table_to_gbq
from ingest_utils.constants import PULLDATA_PROJECT_ID, PERMUTIVE_PARDOT_MATCH_LOG_B2B
from pardot_utils.utils import get_api_key

# download emails data
print('Downloading data for current day...')
emails = get_gbq_table(PULLDATA_PROJECT_ID, QUERY_EMAILS)


def collect_all_emails_ids(emails_df) -> bool:
    """
    Download all the matching data between Pardot and Permutive.
    Upload the matches to the corresponding table in GBQ.
    :params emails: dataframe with emails
    """

    dataset = B2B_DATASET
    table = 'PermutivePardotMatch'
    email = B2B_EMAIL
    password = B2B_PASSWORD
    user_key = B2B_USER_KEY

    # create a empty dataframe to fill with matching data
    mapping_table = pd.DataFrame()
    # set api_key
    api_key = get_api_key(email, password, user_key)

    # remove problematic emails
    emails_df = emails_df[emails_df['value'] != 'ravi.tangirala@progressivemediagroup.in']
    emails_df = emails_df[emails_df['value'] != 'ravikumar.tangirala@globaldata.com']
    emails_df = emails_df[emails_df['value'] != 'sam.hall@globaldata.com']
    # if the emails dataframe is empty print a message
    if emails_df.empty:
        print('There are no new emails to process.')
    # if emails dataframe is not empty, process the emails
    else:
        print('Processing matches between Pardot and Permutive...')
        # for each row in emails
        for item in emails_df.values:
            # create a df for a single emails data
            single_df = pd.DataFrame()
            # try to get the prospect id for the current email
            try:
                response = get_prospect_data(item[0], api_key, user_key)

            except:
                print('Trying again...')
                # if error, wait 60 sec and try again
                time.sleep(60)
                try:
                    # try to get the prospect id
                    response = get_prospect_data(item[0], api_key, user_key)
                except:
                    # if error persists, continue for next row
                    continue
            # if the response from get_prospect_data is 'ivalid key' ,
            # set a new api key and repeat the process to obtain the id
            if response == 'invalid key':
                api_key = get_api_key(email, password, user_key)
                try:
                    response = get_prospect_data(item[0], api_key, user_key)
                except:
                    print('Trying again...')
                    time.sleep(60)
                    try:
                        response = get_prospect_data(item[0], api_key, user_key)
                    except:
                        continue
            # if the response from get_prospect_data is different from 'invalid email', get the necessary data
            if response != 'invalid email':
                prospect_id = response
                # set the prospect_id as integer
                single_df['prospect_id'] = [int(prospect_id)]
                # set the permutive id , which comes from original emails data
                single_df['permutive_id'] = [item[1]]
                # set the created date as current timestamp
                single_df['created'] = [datetime.datetime.now(pytz.utc)]
                # append the matching data to the final fataframe
                mapping_table = mapping_table.append(single_df)
        # if the mapping_Table is not empty, upload to GBQ table
        if not mapping_table.empty:
            print('Uploading dataframe with ids to GBQ.')
            upload_table_to_gbq(PULLDATA_PROJECT_ID, mapping_table, dataset, table)
        # if it is empty return a message
        else:
            print('No mathing entires found.')
    return True


def update_error_log(context):
    instance = context['task_instance']
    fix_log_error(emails, PERMUTIVE_PARDOT_MATCH_LOG_B2B)


default_args = {
    'owner': 'project-pulldata_permutive_pardot_match_update_b2b',
    'start_date': datetime.datetime(2021, 2, 9, 21, 1, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_permutive_pardot_match_update_b2b',
         catchup=False,
         default_args=default_args,
         schedule_interval='15 1 * * *',
         ) as dag:
    update_ids = PythonOperator(task_id='update_ids', python_callable=collect_all_emails_ids,
                                op_kwargs={"emails_df": emails},
                                on_failure_callback=update_error_log)

update_ids
