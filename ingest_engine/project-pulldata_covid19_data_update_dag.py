#imports
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
from covid19_data_update.constants import DATA_SOURCE
import pandas as pd
from covid19_data_update.constants import APPLE_LAST_DATE, COVID19_DATASET
from covid19_data_update.gbq_queries import QUERY_APPLE_TABLE
from ingest_utils.database_gbq import get_gbq_table, upload_table_to_gbq
from ingest_utils.database_postgre import upload_table_to_postgre
from ingest_utils.logging import write_to_log
from ingest_utils.utils import fix_dataframe_columns
from ingest_utils.constants import PULLDATA_PROJECT_ID, LOGS_FOLDER, UPDATE_COVID19_PROCESSING_RESULTS, POSTGRE_AUTH,\
    SUCC_REPLACE_LOG_MSG, NO_DATA_SKIP_UPLOAD
from ingest_utils.metadata import update_metadata


def download_and_update_data() -> bool:
    """
    Get Covid19 data from DATA_SOURCE links and upload it to Postgre and GBQ
    :return: True if the data is updated successfully
    """

    # loop through data source dict
    for table, source in DATA_SOURCE.items():
        # check if there is new data for apple_covid19_mobility_data
        if table == 'apple_covid19_mobility_data':
            apple_gbq_date = get_gbq_table(PULLDATA_PROJECT_ID, QUERY_APPLE_TABLE).values[0][0].replace("_", "-")[1:]
            if APPLE_LAST_DATE == apple_gbq_date:
                print('There is no new data for apple_covid19_mobility_data')
                write_to_log(f'{LOGS_FOLDER}{UPDATE_COVID19_PROCESSING_RESULTS}',
                             COVID19_DATASET, table, NO_DATA_SKIP_UPLOAD)
                continue

        # download in dataframe every csv form source url
        df = pd.read_csv(source, low_memory=False)
        df = fix_dataframe_columns(df)
        # fix all columns that are with name 'date' to be datetime type
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(
                df['date'].astype(str).str.replace('-', ''),
                format='%Y%m%d')
        # upload the data to GBQ and Postgre
        upload_table_to_gbq(PULLDATA_PROJECT_ID, df, COVID19_DATASET, table, method='replace')
        upload_table_to_postgre(POSTGRE_AUTH, df, COVID19_DATASET, table, method='replace')
        # Update metadata
        update_metadata(PULLDATA_PROJECT_ID, POSTGRE_AUTH, df, COVID19_DATASET, table)
        write_to_log(f'{LOGS_FOLDER}{UPDATE_COVID19_PROCESSING_RESULTS}', COVID19_DATASET, table, SUCC_REPLACE_LOG_MSG)
    return True


default_args = {
    'owner': 'project-pulldata_covid19_data_update',
    'start_date': datetime.datetime(2020, 10, 15, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_covid19_data_update',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 6 * * *',
     ) as dag:
    
    
    update_covid_data = PythonOperator(task_id='download_and_update_data',
                               python_callable=download_and_update_data)

update_covid_data
