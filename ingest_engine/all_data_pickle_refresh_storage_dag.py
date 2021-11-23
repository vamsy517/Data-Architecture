from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ingest_utils.constants import PULLDATA_PROJECT_ID,FRANKLIN_API_PICKLE_REFRESH_URL
import pandas as pd
import datetime
import segmentation.app_utils.app_utils as app_utils
import ingest_utils.storage_gcp as st
import pickle
import requests 
import time
import os

def refresh_data_pickles_on_storage() -> bool:
    """
    Refresh the pickles on cloud storage which are used to power the franklin api    
    The api would download the pickle files and refresh the dashboard
    :return: True in case all operations are successful
    """

    bucket="franklin-service"
    retry_threshold=3
    sleep_time=30

    all_data_file_name="all_data_all_domains.pickle"
    all_dropdown_file_name="dropdowns_all_domains.pickle"
    # Fetch latest data and dump to pickle files
    print('Fetching user aggregated data')
    all_data_df = app_utils.get_data()    
    drop_down_data_df = app_utils.get_dropdowns(all_data_df)
    pickle.dump(all_data_df, open(all_data_file_name, "wb"))
    pickle.dump(drop_down_data_df, open(all_dropdown_file_name, "wb"))
    print('Fetched user aggregated data and dumped to local pickle files')

    # Upload pickle files to gcp bucket 
    print('Uploading local pickle files to bucket '+bucket)
    st.upload_file_to_storage(PULLDATA_PROJECT_ID, bucket,all_data_file_name,all_data_file_name)
    st.upload_file_to_storage(PULLDATA_PROJECT_ID, bucket,all_dropdown_file_name,all_dropdown_file_name)
    print('Uploaded local pickle files to bucket '+bucket)

    if os.path.exists(all_data_file_name):
        os.remove(all_data_file_name)
    if os.path.exists(all_dropdown_file_name):
        os.remove(all_dropdown_file_name)

    #Refresh Franklin API with the new pickle files 

    for retry in range(0,retry_threshold): 
        response = requests.get(FRANKLIN_API_PICKLE_REFRESH_URL)
        if(response.ok != True):
            if(retry ==2):
                raise ValueError('Franklin API pickle refresh NOT successful!')
            else:
                print("API call failed, sleeping for "+str(sleep_time)+" seconds before retrying attempt:" + str(retry+1))
                time.sleep(sleep_time)
        else:
            break

    return True

default_args = {
    'owner': 'project-pulldata',
    'start_date': datetime.datetime(2021, 9, 28, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_data_refresh_pickles',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 * * * *',
     ) as dag:
    
    
    refresh_data_pickles = PythonOperator(task_id='refresh_data_pickles_on_storage',
                               python_callable=refresh_data_pickles_on_storage)

refresh_data_pickles