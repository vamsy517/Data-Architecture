from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import datetime
from ingest_utils.database_gbq import get_gbq_table
from ingest_utils.constants import PULLDATA_PROJECT_ID, LOGS_FOLDER
from ingest_utils.class_datahub_updater import Updater
from pardot_utils.constants import B2B_DATASET, EMAIL_API, EMAIL_CLICKS_TABLE, EMAILS_TABLE, B2B_EMAIL, \
    B2B_PASSWORD, B2B_USER_KEY, B2B_EMAIL_LOG, EMAILS_DATA_NAME, GET_BATCH_URL
from pardot_utils.gbq_queries import QUERY_MAX_MIN_EMAIL
from pardot_utils.utils import get_new_created_updated_data, log_update, split, get_email_metrics


def upload_data() -> bool:
    """
    Download Pardot Emails/Emails Metrics new data and update the table in GBQ and Postgre
    :return: True if Pardot Emails/Emails Metrics data is updated
    """
    project_id = PULLDATA_PROJECT_ID
    dataset = B2B_DATASET
    pardot_api = EMAIL_API
    clicks_table = EMAIL_CLICKS_TABLE
    emails_table = EMAILS_TABLE
    email = B2B_EMAIL
    password = B2B_PASSWORD
    user_key = B2B_USER_KEY
    log_file = f'{LOGS_FOLDER}{B2B_EMAIL_LOG}'
    data_name = EMAILS_DATA_NAME
    url = GET_BATCH_URL.format(pardot_api=pardot_api)
    # get max created and updated values to start download form that values
    max_created_updated = get_gbq_table(project_id, QUERY_MAX_MIN_EMAIL.format(project_id=project_id,
                                                                               dataset=dataset,
                                                                               table=clicks_table))
    # get new created data
    new_created_df = get_new_created_updated_data(url, max_created_updated.max_created_at.max(),
                                                  str(int(max_created_updated.maxid) - 1), 'created',
                                                  email, password, user_key, data_name)
    if not new_created_df.empty:
        # fix data types and column names
        new_created_df = new_created_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        new_created_df['email_template_id'] = new_created_df['email_template_id'].astype(pd.Int64Dtype())
        date_cols = ['created_at']
        new_created_df[date_cols] = new_created_df[date_cols].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S',
                                                                    errors='ignore', utc=True)
        new_created_df['list_email_id'] = new_created_df['list_email_id'].astype(pd.Int64Dtype())
        ids_clicks = list(new_created_df.id)
        # split final data in chunks and run updater class on every chunk
        # we split the data because is to big and can't be uploaded at onc
        chunks = split(new_created_df, 30000)
        for c in chunks:
            ids_clicks_split = list(c.id)
            update_clicks = Updater(dataset, clicks_table, c, 'id', ['id'], ids_clicks_split)
            update_clicks.update_data()

        # get the data for email metric details and fix the data types
        email_df = get_email_metrics(email, password, user_key, new_created_df)
        filter_col = [col for col in email_df if col.endswith('rate') or col.endswith('ratio')]
        for col in filter_col:
            email_df[col] = email_df[col].str.rstrip('%').astype('float') / 100.0
        email_df = email_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        email_df.id = email_df.id.astype('int')
        ids_email_metrics = list(email_df.id)
        chunks = split(email_df, 30000)
        for c in chunks:
            ids_email_split = list(c.id)
            update_email_metrics = Updater(dataset, emails_table, c, 'id', ['id'], ids_email_split)
            update_email_metrics.update_data()
    else:
        ids_clicks = []
        ids_email_metrics = []
        # record log file
    log_update(log_file, ids_clicks, clicks_table)
    log_update(log_file, ids_email_metrics, emails_table)
    return True


default_args = {
    'owner': 'project-pulldata_Pardot_Emails_data_update',
    'start_date': datetime.datetime(2020, 11, 24, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_Pardot_Emails_data_update',
         catchup=False,
         default_args=default_args,
         schedule_interval='0 20 * * *',
         ) as dag:
        
    update_email = PythonOperator(task_id='upload_data', python_callable=upload_data)

update_email
