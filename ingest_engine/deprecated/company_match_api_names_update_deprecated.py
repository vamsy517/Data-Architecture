from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from google.cloud import storage

import numpy as np
import requests
import datetime
import pickle
import re
from cleanco import cleanco

from ingest_utils.get_gbq_clients import get_clients
from sql_queries.company_match_names import company_match_names_query



def get_names(query):

    # initialize connection to GBQ
    bqclient, bqstorageclient= get_clients('project-pulldata')
    # get companies names from clearbit event
    dataframe = (
        bqclient.query(query)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )
    return dataframe


def download_file(filename):
    # create client and download file from big bucket
    client = storage.Client.from_service_account_json('/home/ingest/credentials/pulldata-bucket.json')
    bucket = 'nsmg-company-match'
    bucket = client.bucket(bucket)
    blob = bucket.get_blob(filename)
    blob.download_to_filename('/home/ingest/company_match_old_files/'+filename)
    names_list = pickle.load(open('/home/ingest/company_match_old_files/'+filename, "rb"))
    return names_list


def clean_name(name):
    """
        We set name to lower case, replace all & with and strip and remove group
        Use regeg to clean punctuation
        Use cleanco library to clean company names
        You can check what cleanco do in official libery documentation:
        https://pypi.org/project/cleanco/
        Input: company name
        Output: clean name
    """

    pattern = re.compile(r"[[:punct:]]+")
    name = str(name).lower().replace('&', 'and').replace('group', '').strip()
    name = cleanco(name).clean_name()
    name = pattern.sub("", name)
    return name


def build_pickle():
    """
    Download all companies names from clearbit and old companies names from the bucket.
    Compare the old list with the new list and get the difference (new names).
    Clean the new names and add them to the existing list and create new pickles.
    """
    names_df = get_names(company_match_names_query)
    name_list = list(names_df.name)
    name_list_old = download_file('clearbit_all.p')
    name_list_old_fixed = download_file('fixednames.p')
    new_list = np.setdiff1d(name_list, name_list_old)
    print('Number of new companies')
    print(len(new_list))
    name_list_old.extend(new_list)
    new_list_clean = list(map(clean_name, new_list))
    name_list_old_fixed.extend(new_list_clean)
    pickle.dump(name_list_old, open( "/home/ingest/company_match_files/clearbit_all.p", "wb" ))
    pickle.dump(name_list_old_fixed, open( "/home/ingest/company_match_files/fixednames.p", "wb" ))
    return True


def upload_to_storage(project_id, bucket, source_file, target_file):
    # upload the pickles to bucket
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket)
    blob = bucket.blob(target_file)
    with open(source_file, 'rb') as my_file:
        blob.upload_from_file(my_file)
    return True

def upload_pickles():
    # build pickles with new companie names
    build_pickle()
    # upload the pickles
    upload_to_storage('project-pulldata', 'nsmg-company-match',
                      '/home/ingest/company_match_files/clearbit_all.p',
                      'clearbit_all.p')
    upload_to_storage('project-pulldata', 'nsmg-company-match',
                      '/home/ingest/company_match_files/fixednames.p',
                      'fixednames.p')
    
    # finally, also refresh the cloudrun API via its own 'init' method
    url = f'https://nsmg.nsmgendpoints.co.uk/companymatchapi/init?api_key=GRJufHnAf4Dv4Tzu07jvaKfK0tA'
    response = requests.post(url).json()
    if response == 'done':
        print('Cloud Run API refresh successful!')
    else:
        print('Cloud Run API refresh NOT successful!')
        raise ValueError('Cloud Run API refresh NOT successful!')
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
    
    
    upload_company_names = PythonOperator(task_id='upload_pickles',
                               python_callable=upload_pickles)

upload_company_names