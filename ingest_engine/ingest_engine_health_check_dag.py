import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ingest_utils.logging import log_last_run_date
from ingest_utils.constants import LOGS_FOLDER, HEALTH_CHECK_LOG, PULLDATA_PROJECT_ID, POSTGRE_AUTH
from ingest_utils.database_gbq import get_gbq_table, upload_table_to_gbq
from ingest_utils.database_postgre import upload_table_to_postgre
from ingest_utils.metadata import delete_rows_from_meta_table, METADATA_DATASET
from ingest_engine_health_check.utils import generate_list_of_tables, generate_health_check_table
from ingest_engine_health_check.constants import NR_SAMPLES, NR_SAMPLES_Z_SCORE, HEALTH_CHECK_TABLE
from sql_queries.gbq_queries import QUERY_INFORMATION_SCHEMA, QUERY_DATASET_INFORMATION_SCHEMA


def perform_health_check() -> bool:
    # generate list of tables, on which to perform health check
    lst_tables = generate_list_of_tables()
    # TODO: DO NOT READ FROM LOGS
    if lst_tables.size != 0:
        # iterate over each table
        for item in lst_tables:
            # strip spaces before and after item
            item = item.strip()
            dataset, table = item.split('.')
            df_schema = get_gbq_table(PULLDATA_PROJECT_ID, QUERY_INFORMATION_SCHEMA)
            dataset_list = df_schema['schema_name'].tolist()
            if dataset in dataset_list:
                df_dataset_schema = get_gbq_table(PULLDATA_PROJECT_ID,
                                                  QUERY_DATASET_INFORMATION_SCHEMA.format(dataset=dataset))
                tables_list = df_dataset_schema['table_name'].tolist()
                if table in tables_list:
                    print(f'Start Health Check on {dataset}.{table}')
                    # perform health check for the specific table
                    df_health_check = generate_health_check_table(PULLDATA_PROJECT_ID, dataset, table,
                                                                  NR_SAMPLES, NR_SAMPLES_Z_SCORE)
                    # check if the results are empty, due to emtpy table in BigQuery
                    if df_health_check.shape[0] > 0:
                        df_health_check['Column_Type'] = df_health_check['Column_Type'].astype(str)
                        df_health_check['Coverage'] = df_health_check['Coverage'].astype(str)

                        print(f'End Health Check on {dataset}.{table}') 
                        # delete old data from health check table that is updated
                        print(f'Delete data for {dataset}.{table} from HealthCheck table')
                        delete_rows_from_meta_table(PULLDATA_PROJECT_ID, POSTGRE_AUTH,
                                                    HEALTH_CHECK_TABLE, dataset, table)
                        # upload and append the data to gbq HealthCheck table
                        print(f'Upload data for {dataset}.{table} to GBQ HealthCheck table')
                        upload_table_to_gbq(PULLDATA_PROJECT_ID, df_health_check, METADATA_DATASET, HEALTH_CHECK_TABLE)
                        # upload and append the data to Postgre HealthCheck table
                        print(f'Upload data for {dataset}.{table} to Postgre HealthCheck table')
                        upload_table_to_postgre(POSTGRE_AUTH, df_health_check, METADATA_DATASET, HEALTH_CHECK_TABLE)
                    else:
                        print(f'Table {dataset}.{table} is empty. Skipping Health Check.') 
                else:
                    print(f'There is no table {table} in dataset {dataset} in GBQ')
            else:
                print(f'There is no dataset {dataset} in GBQ')
                
        return True
    print('There are no new tables.')
    return True


default_args = {
    'owner': 'ingest_engine_input_data_health_check',
    'start_date': datetime.datetime(2020, 8, 30, 5, 1, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('Input_data_health_check',
         catchup=False,
         default_args=default_args,
         schedule_interval='0 5 * * *',
         ) as dag:

    health_check = PythonOperator(task_id='perform_health_check',
                                  python_callable=perform_health_check)

    record_log = PythonOperator(task_id='log_last_run_date',
                                python_callable=log_last_run_date,
                                op_kwargs={"log_file": f'{LOGS_FOLDER}{HEALTH_CHECK_LOG}'})


health_check >> record_log
