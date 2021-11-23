from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import datetime
from ingest_utils.utils import fix_dataframe_columns
from ingest_utils.database_gbq import get_gbq_table, upload_table_to_gbq
from ingest_utils.database_postgre import upload_table_to_postgre
from ingest_utils.metadata import update_metadata
from ingest_utils.constants import PULLDATA_PROJECT_ID, LOGS_FOLDER, POSTGRE_AUTH
from sql_queries.gbq_queries import QUERY_WHOLE_TABLE
from pardot_utils.constants import B2B_DATASET, PROSPECTS_API, PROSPECTS_TABLE, B2B_EMAIL, \
    B2B_PASSWORD, B2B_USER_KEY, B2B_PROSPECTS_LOG, PROSPECTS_DATA_NAME, GET_BATCH_URL
from pardot_utils.utils import get_new_created_updated_data, log_update, split


def upload_data() -> bool:
    """
    Download Pardot Prospects new data and update the table in GBQ and Postgre
    :return: True if Pardot Prospects data is updated
    """
    project_id = PULLDATA_PROJECT_ID
    dataset = B2B_DATASET
    pardot_api = PROSPECTS_API
    table = PROSPECTS_TABLE
    email = B2B_EMAIL
    password = B2B_PASSWORD
    user_key = B2B_USER_KEY
    log_file = f'{LOGS_FOLDER}{B2B_PROSPECTS_LOG}'
    data_name = PROSPECTS_DATA_NAME
    url = GET_BATCH_URL.format(pardot_api=pardot_api)
    # get old table from GBQ
    old_df = get_gbq_table(project_id, QUERY_WHOLE_TABLE.format(project_id=project_id, dataset=dataset, table=table))
    # get new created data
    new_created_df = get_new_created_updated_data(url, old_df.created_at.max(),
                                                  str(int(old_df.id.max()) - 1), 'created',
                                                  email, password, user_key, data_name)

    # get new updated data
    new_updated_df = get_new_created_updated_data(url, old_df.updated_at.max(),
                                                  str(int(old_df.id.min()) - 1), 'updated',
                                                  email, password, user_key, data_name)
    # concat update and new data
    final_df = pd.concat([new_created_df, new_updated_df], axis=0, ignore_index=True, sort=False)

    # check if DataFrame is empty
    if not final_df.empty:
        final_df = fix_dataframe_columns(final_df)
        # drop sensitive data
        final_df = final_df.drop(
            ['salutation', 'first_name', 'last_name', 'email', 'password', 'phone', 'fax', 'address_one',
             'address_two'], axis=1)
        try:
            final_df = final_df.drop(['mobile_phone'], axis=1)
        except:
            pass
        final_df = pd.concat([old_df, final_df], axis=0, ignore_index=True, sort=False)
        # drop duplicates rows
        final_df = final_df.drop_duplicates(subset='id', keep="last")
        final_df = final_df.loc[:, ~final_df.columns.duplicated()]
        # fix data types
        final_df = final_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        # cols with date data for convert to datetime
        date_cols = ['created_at', 'updated_at', 'crm_last_sync',
                     'last_activity_at',
                     'last_activityvisitor_activitycreated_at',
                     'last_activityvisitor_activityupdated_at']
        # convert to datetime
        final_df[date_cols] = final_df[date_cols].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S', errors='ignore',
                                                        utc=True)
        # get list of prospect ID for log
        ids = list(final_df.id)

        # upload data to gbq
        final_df.to_gbq(destination_table=f'{dataset}.{table}', project_id=project_id, if_exists='replace')
        # upload data to postgre
        upload_table_to_postgre(POSTGRE_AUTH, final_df, dataset, table, method='replace')
        chunks = split(final_df, 20000)
        for c in chunks:
            # update metadata
            update_metadata(project_id, POSTGRE_AUTH, c, dataset, table)
    else:
        ids = []
    # record log file
    log_update(log_file, ids, table)
    return True


default_args = {
    'owner': 'project-pulldata_Pardot_Prospects_data_update',
    'start_date': datetime.datetime(2020, 11, 8, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_Pardot_Prospects_data_update',
         catchup=False,
         default_args=default_args,
         schedule_interval='0 18 * * *',
         ) as dag:
    update_prospects = PythonOperator(task_id='upload_data', python_callable=upload_data)

update_prospects
