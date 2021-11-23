import datetime
from ingest_utils import class_uploader
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ingest_utils.constants import PULLDATA_PROJECT_ID, LOGS_FOLDER
from ingest_files_engine.constants import INPUT_FOLDER, PROCESSED, IN_PROGRESS, NEW_FOUND_FILES, REDIRECTED_FILES


def detect_input_files() -> bool:
    """
    Check if there are new csv files and upload them to GBQ and Postgre
    :return: True if successful
    """
    # this is the input folder; all input files MUST be placed here
    list_folder = os.listdir(INPUT_FOLDER)
    
    # get the list of already processed files
    with open(f'{LOGS_FOLDER}{PROCESSED}', 'r') as f:
        processed_list = f.readlines()
    # create a list of processed files and remove newline character from the end of the name of the file
    processed_list = [item[:-1] for item in processed_list]
    
    # get the list of files, which are currently being processed
    with open(f'{LOGS_FOLDER}{IN_PROGRESS}', 'r') as f:
        in_progress_list = f.readlines()
        
    # create a list of currently being processed files and remove newline character from the end of the name of the file
    in_progress_list = [item[:-1] for item in in_progress_list]
    
    # TODO: fix in_progress logic
    
    # get new uploaded files by subtracting the list of current files in the input folder
    # and the list of already processed files
    files_to_upload = set(list_folder) - set(processed_list) # - set(in_progress_list)
    
    # check if there are any new files
    if len(files_to_upload) > 0:
        # create a file, containing the time and the list of new files
        with open(f'{LOGS_FOLDER}{NEW_FOUND_FILES}', 'a') as f:
            curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(str(curr_time) + '|' + str(files_to_upload) + '\n')

        # get the first of the new files
        file_to_upload = list(files_to_upload)[0]
        
        # get the filepath of the new file
        filepath_to_upload = INPUT_FOLDER + str(file_to_upload)
        # set the project_id to which the file will be uploaded
        # instantiate the GBQ_Uploader Class
        uploader = class_uploader.Uploader(PULLDATA_PROJECT_ID)
        
        # log the event that the file was sent for processing
        with open(f'{LOGS_FOLDER}{REDIRECTED_FILES}', 'a') as f:
            curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(str(curr_time) + '|' + str(file_to_upload) + '\n')
      
        # process the new file (more details can be found in upload_to_gbq.py)
        uploader.process_file(filepath_to_upload)
        print('Success')
    else:
        print('No new files at this time')
    return True


default_args = {
    'owner': 'ingest_engine_pulldata_sandbox',
    'start_date': datetime.datetime(2020, 7, 2, 8, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_ingest',
     catchup=False,
     default_args=default_args,
     schedule_interval='*/5 * * * *',
     ) as dag:

    detect = PythonOperator(task_id='detect_input_files', python_callable=detect_input_files)

detect
