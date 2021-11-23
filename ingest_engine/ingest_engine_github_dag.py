from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import datetime
import time
import subprocess
from ingest_engine_github.constants import EXCLUDED_EXTENSIONS, EXCLUDED_ENTITIES, DAGS_FOLDER,\
    GITHUB_POSTGRE_API_FOLDER, AIRFLOW_POSTGRE_API_FOLDER, GITHUB_PATH
from ingest_utils.constants import MAIN_FOLDER
from ingest_engine_github.utils import replace_files_and_folders


def check_repo_for_updates()->bool:
    """
    Check Github Repo for updates
    In case any updates are present, replace all files and folders (except excluded ones) in the corresponding folder
    :return: True if there are no updates or the files are replaced successfully
    """
    # change current directory to GITHUB_PATH
    os.chdir(GITHUB_PATH)
    # fetch the origin to see if any changes have happened
    command = 'sudo git fetch'
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    # get the states - local repo vs master
    command = 'git status -uno'
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    # inspect the second line of the response
    output_check = output.decode("utf-8").split('\n')[1]
    # if branch is up-to-date with master, pass with a print
    if output_check == "Your branch is up-to-date with 'origin/master'.":
        print('Nothing to pull from master repo on GitHub.')
    else:
        # else pull the origin
        print('Pulling from GitHub repo, Master branch..')
        command = 'sudo git pull'
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        # once local repo is updated, get a list of all python scripts in the repo's ingest folder
        ingest_engine_file_list = os.listdir(MAIN_FOLDER)
        ingest_engine_file_list = [list_item for list_item in ingest_engine_file_list if list_item[-3:]
                                   not in EXCLUDED_EXTENSIONS]
        replace_files_and_folders(ingest_engine_file_list, EXCLUDED_ENTITIES, MAIN_FOLDER, DAGS_FOLDER)
        # TO DO: Create logic for more complex file structures
        # check if the folder of the API is changed in the last 300 seconds (= frequency of the DAG)
        most_recent_change = max([os.path.getmtime(GITHUB_POSTGRE_API_FOLDER + item)
                              for item in os.listdir(GITHUB_POSTGRE_API_FOLDER)])
        time_since_change = time.time() - most_recent_change
        if time_since_change < 300:
            # once local repo is updated, get a list of all files and folders in the repo's APIs folder
            apis_file_list = os.listdir(GITHUB_POSTGRE_API_FOLDER)
            replace_files_and_folders(apis_file_list, EXCLUDED_ENTITIES,
                                      GITHUB_POSTGRE_API_FOLDER, AIRFLOW_POSTGRE_API_FOLDER)
            print("Restarting Flask API service")
            # restart the service for API:
            command = 'sudo systemctl stop postgre-flaskrestplus.service'
            process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
            output, error = process.communicate()
            command = 'sudo systemctl daemon-reload'
            process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
            output, error = process.communicate()
            command = 'sudo systemctl start postgre-flaskrestplus.service'
            process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
            output, error = process.communicate()
        else:
            print('Skipping copy of API files')
    return True


default_args = {
    'owner': 'ingest_engine_pulldata_sandbox',
    'start_date': datetime.datetime(2020, 7, 20, 13, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_ingest_github',
     catchup=False,
     default_args=default_args,
     schedule_interval='*/5 * * * *',
     ) as dag:
    
    
    github_ingest = PythonOperator(task_id='check_repo_for_updates',python_callable=check_repo_for_updates)

github_ingest
