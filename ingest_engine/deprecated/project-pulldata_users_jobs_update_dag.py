from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
from ingest_utils.constants import PULLDATA_PROJECT_ID, POSTGRE_AUTH
from ingest_utils.database_gbq import upload_table_to_gbq, get_gbq_table
from ingest_utils.metadata import update_metadata
from users_jobs_update.gbq_queries import QUERY_GET_ALL_JOBS, GET_PREV_DATA, DELETE_ROWS_USER_JOBS
from users_jobs_update.utils import update_row
import users_jobs_update.jobs_processing as jp


def update_users_jobs() -> bool:
    project_id = PULLDATA_PROJECT_ID
    dataset = 'UsersJobs'
    table = 'users_jobs'
    # get old data from GBQ table:
    user_jobs_df_old = get_gbq_table(project_id, GET_PREV_DATA)
    # get New data
    user_jobs_df_new = get_gbq_table(project_id, QUERY_GET_ALL_JOBS)
    # sort the data
    user_jobs_all = user_jobs_df_new.sort_values(by=['updated_at', 'created_at', 'job_title'],
                                                 ascending=[True, True, False])
    # remove some of the invalid values
    list_symbols = ['', ' ', '.', '-', ',', '..', '...', '--', '/', '?', '???', 'TEST', 'TEst', 'Test', 'test',
                    'x', 'xx', 'xxx', 'xx', 'NULL', 'none', 'None', 'NONE', 'xxxx']
    final_df = user_jobs_all[~user_jobs_all.job_title.isin(list_symbols)]
    # drop duplicate values and keep last that is most updated
    final_df = final_df.drop_duplicates(subset='permutive_id', keep='last')
    # get first created values
    created_df = user_jobs_all.groupby('permutive_id', sort=True)['created_at'].apply(min).to_frame().reset_index()
    # get last updated/created values
    updated_df = user_jobs_all.groupby('permutive_id', sort=True)['updated_at'].apply(max).to_frame().reset_index()
    # drop old updated/created and add the new one
    final_df = final_df.drop(['created_at', 'updated_at'], axis=1)
    final_df = final_df.merge(created_df, on='permutive_id')
    final_df = final_df.merge(updated_df, on='permutive_id')
    # get all old IDS
    old_ids = list(user_jobs_df_old.permutive_id.unique())
    # filter only new created data
    final_df_new = final_df[~final_df.permutive_id.isin(old_ids)]
    # set history to be the new created
    final_df_new['history'] = final_df_new.job_title
    # get new data that has ID in the old data
    final_df_old = final_df[final_df.permutive_id.isin(old_ids)]
    # rename columns
    final_df_old = final_df_old.rename(columns={'job_title': 'job_title_2', 'created_at': 'created_at_2',
                                                'updated_at': 'updated_at_2', 'source_table': 'source_table_2'})
    # merge the new and old data
    merged_df = final_df_old.merge(user_jobs_df_old, on='permutive_id')
    # get list with updated IDs
    list_with_updated_ids = list(merged_df.query('job_title_2 != job_title')['permutive_id'])
    # check for updates
    # if there is update we update the data
    # else keep it as it is
    merged_df = merged_df[merged_df['permutive_id'].isin(list_with_updated_ids)]
    merged_df_final = merged_df.apply(update_row, axis=1)
    # drop aux columns
    merged_df_final.drop(['job_title_2', 'created_at_2', 'updated_at_2', 'source_table_2'], axis=1, inplace=True)
    # merge updated old data with the new one
    final_df_to_upload = merged_df_final.append(final_df_new)
    # delete rows for update before append the new
    # we use get_gbq_table to delete the rows
    if list_with_updated_ids:
        get_gbq_table(project_id, DELETE_ROWS_USER_JOBS.format(dataset=dataset, list_of_ids='"'+'","'.join(list_with_updated_ids)+'"'))
    # upload the data to GBQ
    upload_table_to_gbq(project_id, final_df_to_upload, dataset, table, method='append')
    # Update metadata
    update_metadata(project_id, POSTGRE_AUTH, final_df_to_upload, dataset, table)
    print('New record: ' + str(final_df_new.shape[0]))
    print('Update record: ' + str(len(list_with_updated_ids)))
    processed_job_df = jp.run_jobs_processing(final_df_to_upload[['permutive_id', 'job_title']])
    # delete rows for update before append the new
    # we use get_gbq_table to delete the rows
    if list_with_updated_ids:
        get_gbq_table(project_id, DELETE_ROWS_USER_JOBS.format(dataset='Segmentation_v2',
                                                               list_of_ids='"'+'","'.join(list_with_updated_ids)+'"'))
    # upload the data to GBQ
    upload_table_to_gbq(project_id, processed_job_df, 'Segmentation_v2', table, method='append')
    print('Update processed jobs')
    return True


default_args = {
    'owner': 'project-pulldata',
    'start_date': datetime.datetime(2021, 5, 26, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_update_users_jobs',
         catchup=False,
         default_args=default_args,
         schedule_interval='0 5 * * *',
         ) as dag:
    update_users_jobs = PythonOperator(task_id='update_users_jobs', python_callable=update_users_jobs)

update_users_jobs
