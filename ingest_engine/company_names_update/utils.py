import re
from cleanco import cleanco
import pickle

import requests
from ingest_utils.database_gbq import get_gbq_table
from ingest_utils.storage_gcp import download_file_from_storage
import numpy as np


def clean_company_name(name: str) -> str:
    """
        We set name to lower case, replace all & with and strip and remove group
        Use regeg to clean punctuation
        Use cleanco library to clean company names
        You can check what cleanco do in official libery documentation:
        https://pypi.org/project/cleanco/
        :param name: company name
        :return: clean name
    """

    pattern = re.compile(r"[[:punct:]]+")
    name = str(name).lower().replace('&', 'and').replace('group', '').strip()
    name = cleanco(name).clean_name()
    name = pattern.sub("", name)
    return name


def build_pickle(project_id: str, company_names_query: str, download_dir: str, upload_dir: str, bucket: str,
                 clearbit_pickle_name: str, fixednames_pickle_name: str) -> bool:
    """
    Download all companies names from clearbit and old companies names from the bucket.
    Compare the old list with the new list and get the difference (new names).
    Clean the new names and add them to the existing list and create new pickles.
    :param project_id: Project ID
    :param company_names_query: SQL for getting all company names from Clierbit
    :param download_dir: directory to download the pickles
    :param upload_dir: directory for files to upload
    :param bucket: bucket name
    :param clearbit_pickle_name: name for pickle with clearbit names
    :param fixednames_pickle_name: name for pickle with cleaned/fixed names
    :return: True if the pickles are build successfully
    """

    names_df = get_gbq_table(project_id, company_names_query)
    name_list = list(names_df.name)
    download_file_from_storage(project_id, clearbit_pickle_name, bucket, download_dir)
    name_list_old = pickle.load(open(f'{download_dir}{clearbit_pickle_name}', "rb"))
    download_file_from_storage(project_id, fixednames_pickle_name, bucket, download_dir)
    name_list_old_fixed = pickle.load(open(f'{download_dir}{fixednames_pickle_name}', "rb"))
    new_list = np.setdiff1d(name_list, name_list_old)
    print('Number of new companies')
    print(len(new_list))
    name_list_old.extend(new_list)
    new_list_clean = list(map(clean_company_name, new_list))
    name_list_old_fixed.extend(new_list_clean)
    pickle.dump(name_list_old, open(f"{upload_dir}{clearbit_pickle_name}", "wb"))
    pickle.dump(name_list_old_fixed, open(f"{upload_dir}{fixednames_pickle_name}", "wb"))
    return True


def api_refresh(url: str) -> bool:
    """
    :param url: URL for the request that trigger the API refresh
    :return: True if the API is refreshed with the new lists of names
    """
    response = requests.post(url).json()
    if response == 'done':
        print('Cloud Run API refresh successful!')
    else:
        print('Cloud Run API refresh NOT successful!')
        raise ValueError('Cloud Run API refresh NOT successful!')
    return True
