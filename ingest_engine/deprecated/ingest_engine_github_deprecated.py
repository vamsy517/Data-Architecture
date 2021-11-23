from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import os
import datetime
import time
import subprocess
import shutil

def check_update_repo():
    # list of file extensions which we want to skip copying
    EXCLUDED_EXTENSIONS = ['.md']
    # list of files/folders which we dont need to copy to dags folder
    EXCLUDED_ENTITIES = ['gd_apis_notebooks', 'deprecated']
    # set the source path to the repo folder
    path = '/home/NSMG-DataArchitecture/'
    os.chdir(path)

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
        ingest_engine_file_list = os.listdir('ingest_engine')
        ingest_engine_file_list = [list_item for list_item in ingest_engine_file_list if list_item[-3:] not in EXCLUDED_EXTENSIONS]
        # for each item in the list
        for entity in ingest_engine_file_list:
        	# check if entity is in excluded entities list
        	if entity not in EXCLUDED_ENTITIES:
	            src = 'ingest_engine/' + str(entity)
	            dst = '/home/airflow_gcp/airflow/dags/' + str(entity)
	            if os.path.isdir(src):
	                # check if folder already exists
	                if os.path.exists(dst):
	                    # remove folder
	                    shutil.rmtree(dst)
	                shutil.copytree(src, dst)
	                print(f'Finished copying {entity} folder to {dst}.')
	            else:
	                shutil.copyfile(src, dst)
	                print(f'Finished copying {entity} file to {dst}.')

        # TO DO: Create logic for more complex file structures
        # check if the folder of the API is changed in the last 300 seconds (= freaquancy of the DAG)
        most_recent_change = max([os.path.getmtime('APIs/flaskrestplus_postgre_api/' + item)
                              for item in os.listdir('APIs/flaskrestplus_postgre_api/')])
        time_since_change = time.time() - most_recent_change
        if time_since_change < 300:
            # once local repo is updated, get a list of all files and folders in the repo's APIs folder
            apis_file_list = os.listdir('APIs/flaskrestplus_postgre_api/')
            # for each file or folder
            for item in apis_file_list:
                # create source and destination items (files or folders)
                src = 'APIs/flaskrestplus_postgre_api/' + str(item)
                dst = '/home/data_hub_api/Postgre_API/' + str(item)
                # if the item is folder (directory)
                if os.path.isdir(src):
                    # check if path exisst and remove the folder
                    if os.path.exists(dst):
                        shutil.rmtree(dst)
                    # copy folder from local repo to destination folder
                    shutil.copytree(src, dst)
                else:
                    # replace file from local repo to destination 
                    shutil.copyfile(src, dst)
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
    
    
    github_ingest = PythonOperator(task_id='check_update_repo',
                               python_callable=check_update_repo)

github_ingest