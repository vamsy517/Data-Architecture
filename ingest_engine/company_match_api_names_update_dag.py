from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import datetime

from ingest_utils.storage_gcp import upload_file_to_storage
from ingest_utils.constants import PULLDATA_PROJECT_ID
from company_names_update.constants import API_KEY

from company_names_update.queries import GET_COMPANY_NAMES
from company_names_update.utils import build_pickle, api_refresh


def update_company_names() -> bool:
    """
    Update company match API with new clearbit company names daily.
    :return: True if successful
    """
    # build pickles with new companies names
    download_dir = '/home/ingest/company_match_old_files/'
    upload_dir = '/home/ingest/company_match_files/'
    bucket = 'nsmg-company-match'
    clearbit_pickle_name, fixednames_pickle_name = 'clearbit_all.p', 'fixednames.p'
    build_pickle(PULLDATA_PROJECT_ID, GET_COMPANY_NAMES, download_dir,
                 upload_dir, bucket, clearbit_pickle_name, fixednames_pickle_name)
    # upload the pickles
    upload_file_to_storage(PULLDATA_PROJECT_ID, bucket,
                           f'{upload_dir}{clearbit_pickle_name}',
                           clearbit_pickle_name)
    upload_file_to_storage(PULLDATA_PROJECT_ID, bucket,
                           f'{upload_dir}{fixednames_pickle_name}',
                           fixednames_pickle_name)

    # finally, also refresh the cloud run API via its own 'init' method
    url = f'https://nsmg.nsmgendpoints.co.uk/companymatchapi/init?api_key={API_KEY}'
    api_refresh(url)
    return True


default_args = {
    'owner': 'dscoe_team',
    'start_date': datetime.datetime(2021, 4, 10, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}


with DAG('company_match_api_names_update',
         catchup=False,
         default_args=default_args,
         schedule_interval='0 3 * * *',
         ) as dag:
    upload_company_names = PythonOperator(task_id='update_company_names',
                                          python_callable=update_company_names)

upload_company_names
