from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
from global_data_utils.utils import check_gd_api_token, get_mnc_entity_listing_or_details, fix_date_type, update_data
from global_data_utils.constants import MNC_API_CREDENTIALS, MNC_API_URL, MNC_API, MNC_DATASET
from ingest_utils.constants import CREDENTIALS_FOLDER, UPDATE_MNC_PROCESSING_RESULTS
from ingest_utils.utils import iter_by_group


def process_data() -> bool:
    """
    Download full MNC data from GD API, transform nested columns,
    update tables and metadata in both Postgre and GBQ via class Updater.
    @return: True if success
    """
    # Download company data from API
    # set the token id
    token = check_gd_api_token(f'{CREDENTIALS_FOLDER}{MNC_API_CREDENTIALS}', MNC_API_URL, MNC_API)
    log_file = f'{UPDATE_MNC_PROCESSING_RESULTS}'
    # get MNC Companies Listing
    print(f'Start to download full data for MNC Companies Listing')
    df_listing_companies = get_mnc_entity_listing_or_details('Companies', 'Listing', token)
    print(f'Finished download of full data for MNC Companies Listing')
    # fix as on date column type
    df_listing_companies = fix_date_type(df_listing_companies, ['As_On_Date'])
    # update data for Companies Listing
    update_data(df_listing_companies, MNC_DATASET, 'MNCCompaniesListing', 'CompanyID', ['CompanyID'], log_file)
    # get MNC Segments listing
    print(f'Start to download full data for MNC Segments')
    df_listing_segments = get_mnc_entity_listing_or_details('Segments', 'Listing', token)
    print(f'Finished download of full data for MNC Segments')
    # update data for Segments Listing
    update_data(df_listing_segments, MNC_DATASET, 'MNCSegmentsListing', 'CompanyID', ['CompanyID'], log_file)
    # get MNC Segments details
    print(f'Start to download full data for MNC Segments details')
    df_details_segments = get_mnc_entity_listing_or_details('Segments', 'Details', token)
    print(f'Finished download of full data for MNC Segments details')
    # update data for Segments Details
    update_data(df_details_segments, MNC_DATASET, 'MNCSegmentsDetails', 'CompanyID',
                ['CompanyID', 'SegmentType', 'SegmentTitle', 'Year', 'Value_Millions'], log_file)
    
    # get MNC Subsidiaries listing
    print(f'Start to download full data for MNC Subsidiaries')
    df_listing_subsidiaries = get_mnc_entity_listing_or_details('Subsidiaries', 'Listing', token)
    print(f'Finished download of full data for MNC Subsidiaries')
    # update data for Subsidiaries Listing
    update_data(df_listing_subsidiaries, MNC_DATASET, 'MNCSubsidiariesListing', 'CompanyID', ['CompanyID'], log_file)
    # get MNC Subsidiaries details
    print(f'Start to download full data for MNC Subsidiaries details')
    df_details_subsidiaries = get_mnc_entity_listing_or_details('Subsidiaries', 'Details', token)
    print(f'Finished download of full data for MNC Subsidiaries details')
    # split final data in chunks and run updater class on every chunk
    # we split the data because is to big and can't be uploaded at once
    for i, c in enumerate(iter_by_group(df_details_subsidiaries, 'CompanyID', 30000)):
        print('Processing chunk: ', i)
        update_data(c, MNC_DATASET, 'MNCSubsidiariesDetails', 'CompanyID', ['CompanyID', 'URN'], log_file)
    return True


default_args = {
    'owner': 'project-pulldata_mnc_data_update',
    'start_date': datetime.datetime(2021, 1, 10, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_mnc_data_update',
         catchup=False,
         default_args=default_args,
         schedule_interval='0 21 * * *',
         ) as dag:
    mnc_update = PythonOperator(task_id='process_data', python_callable=process_data)

mnc_update
