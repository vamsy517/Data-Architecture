from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import datetime
from ingest_utils.database_gbq import get_gbq_table
from ingest_utils.constants import PULLDATA_PROJECT_ID, LOGS_FOLDER
from ingest_utils.class_datahub_updater import Updater
from pardot_utils.constants import B2B_DATASET, LIST_MEMBERSHIP_API, LIST_MEMBERSHIP_TABLE, B2B_EMAIL, \
    B2B_PASSWORD, B2B_USER_KEY, B2B_LIST_MEMBERSHIP_LOG, LIST_MEMBERSHIP_DATA_NAME, GET_BATCH_URL
from pardot_utils.gbq_queries import QUERY_MAX_MIN
from pardot_utils.utils import get_new_created_updated_data, split, log_update


def upload_data() -> bool:
    """
    Download Pardot List Membership new data and update the table in GBQ and Postgre
    :return: True if Pardot List Membership data is updated
    """
    project_id = PULLDATA_PROJECT_ID
    dataset = B2B_DATASET
    pardot_api = LIST_MEMBERSHIP_API
    table = LIST_MEMBERSHIP_TABLE
    email = B2B_EMAIL
    password = B2B_PASSWORD
    user_key = B2B_USER_KEY
    log_file = f'{LOGS_FOLDER}{B2B_LIST_MEMBERSHIP_LOG}'
    data_name = LIST_MEMBERSHIP_DATA_NAME
    url = GET_BATCH_URL.format(pardot_api=pardot_api)
    # get max created and updated values to start download form that values
    max_created_updated = get_gbq_table(project_id, QUERY_MAX_MIN.format(project_id=project_id,
                                                                         dataset=dataset,
                                                                         table=table))
    # get new created data
    new_created_df = get_new_created_updated_data(url, max_created_updated.max_created_at.max(),
                                                  str(int(max_created_updated.maxid) - 1), 'created',
                                                  email, password, user_key, data_name, 3)

    # get new updated data
    new_updated_df = get_new_created_updated_data(url, max_created_updated.max_updated_at.max(),
                                                  str(int(max_created_updated.minid) - 1), 'updated',
                                                  email, password, user_key, data_name, 3)
    # concat update and new data
    final_df = pd.concat([new_created_df, new_updated_df], axis=0, ignore_index=True, sort=False)
    # drop duplicates rows
    final_df = final_df.drop_duplicates(subset='id', keep="last")
    # check if DataFrame is empty
    if not final_df.empty:
        # fix data types
        final_df = final_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        d = {'false': False, 'true': True}
        final_df['opted_out'] = final_df['opted_out'].map(d)
        date_cols = ['created_at', 'updated_at']
        final_df[date_cols] = final_df[date_cols].apply(pd.to_datetime,
                                                        format='%Y-%m-%d %H:%M:%S', errors='ignore', utc=True)

        # record IDs
        ids = list(final_df.id)
        # split final data in chunks and run updater class on every chunk
        # we split the data because is to big and can't be uploaded at onc
        chunks = split(final_df, 20000)
        for c in chunks:
            ids_details = list(c.id)
            update_list = Updater(dataset, table, c, 'id', ['id'], ids_details)
            update_list.update_data()
    else:
        ids = []
    # record log file
    log_update(log_file, ids, table)
    return True


default_args = {
    'owner': 'project-pulldata_Pardot_List_Memberships_data_update',
    'start_date': datetime.datetime(2020, 11, 24, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_Pardot_List_Memberships_data_update',
         catchup=False,
         default_args=default_args,
         schedule_interval='0 23 * * *',
         ) as dag:
    
    update_list_memberships = PythonOperator(task_id='upload_data',
                                             python_callable=upload_data)
update_list_memberships
