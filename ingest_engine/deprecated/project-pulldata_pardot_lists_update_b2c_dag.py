from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import datetime
from ingest_utils.utils import fix_dataframe_columns
from ingest_utils.database_gbq import get_gbq_table
from ingest_utils.constants import PULLDATA_PROJECT_ID, LOGS_FOLDER
from ingest_utils.class_datahub_updater import Updater
from pardot_utils.constants import B2C_DATASET, LISTS_API, LISTS_TABLE, B2C_EMAIL, \
    B2C_PASSWORD, B2C_USER_KEY, B2C_LISTS_LOG, LISTS_DATA_NAME, GET_BATCH_URL
from pardot_utils.gbq_queries import QUERY_MAX_MIN
from pardot_utils.utils import get_new_created_updated_data, log_update


def upload_data() -> bool:
    """
    Download Pardot Lists new data and update the table in GBQ and Postgre
    :return: True if Pardot Lists data is updated
    """
    project_id = PULLDATA_PROJECT_ID
    dataset = B2C_DATASET
    pardot_api = LISTS_API
    table = LISTS_TABLE
    email = B2C_EMAIL
    password = B2C_PASSWORD
    user_key = B2C_USER_KEY
    log_file = f'{LOGS_FOLDER}{B2C_LISTS_LOG}'
    data_name = LISTS_DATA_NAME
    url = GET_BATCH_URL.format(pardot_api=pardot_api)
    # get max created and updated values to start download form that values
    max_created_updated = get_gbq_table(project_id, QUERY_MAX_MIN.format(project_id=project_id,
                                                                         dataset=dataset,
                                                                         table=table))
    # get new created data
    new_created_df = get_new_created_updated_data(url, max_created_updated.max_created_at.max(),
                                                  str(int(max_created_updated.maxid) - 1), 'created',
                                                  email, password, user_key, data_name)
    # get new updated data
    new_updated_df = get_new_created_updated_data(url, max_created_updated.max_updated_at.max(),
                                                  str(int(max_created_updated.minid) - 1), 'updated',
                                                  email, password, user_key, data_name)
    # concat update and new data
    final_df = pd.concat([new_created_df, new_updated_df], axis=0, ignore_index=True, sort=False)
    final_df = final_df.drop_duplicates(subset='id', keep="last")
    # check if DataFrame is empty
    if not final_df.empty:
        # fix columns names
        final_df = fix_dataframe_columns(final_df)
        # fix all numeric values
        final_df = final_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        # all date columns
        date_cols = ['created_at', 'updated_at']
        final_df[date_cols] = final_df[date_cols].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S', errors='ignore',
                                                        utc=True)
        # fix data types
        final_df[['title', 'description']] = final_df[['title', 'description']].astype(str)
        d = {'false': False, 'true': True}
        final_df['is_public'] = final_df['is_public'].map(d)
        final_df['is_dynamic'] = final_df['is_dynamic'].map(d)
        final_df['is_crm_visible'] = final_df['is_crm_visible'].map(d)

        # get new IDs
        ids = list(final_df.id)
        update_list_table = Updater(dataset, table, final_df, 'id', ['id'], ids)
        update_list_table.update_data()
    else:
        ids = []

    # record log file
    log_update(log_file, ids, table)
    return True


default_args = {
    'owner': 'project-pulldata_Pardot_Lists_data_update_B2C',
    'start_date': datetime.datetime(2020, 11, 24, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_Pardot_Lists_data_update_B2C',
         catchup=False,
         default_args=default_args,
         schedule_interval='0 19 * * *',
         ) as dag:
    update_list = PythonOperator(task_id='upload_data', python_callable=upload_data)

update_list
