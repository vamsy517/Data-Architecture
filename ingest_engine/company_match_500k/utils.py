import re
from cleanco import cleanco
import pathlib
import pickle
import numpy as np
from Levenshtein import distance as levenshtein_distance

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


def download_file(filename, client, test_download=False):
    """
    Download needed stored files
    :param filename: file name
    :param client: big bucket client
    :param test_download: True or False if we use function in unit tests
    :return: List of company names
    """
    # create file path
    local_file = pathlib.Path('/home/ingest/company_match_500k_materials/'+filename)
    # check if file already exist and if test is True we download to check this functionality
    if local_file.is_file() and not test_download:
        print(f"Using already downloaded file {local_file}.")
    else:
        # download file (pickle) from bucket and load it
        print(f"Downloading file {local_file}.")
        bucket = 'nsmg-company-match'
        bucket = client.bucket(bucket)
        blob = bucket.get_blob(filename)
        blob.download_to_filename(local_file)
    names_list = pickle.load(open(local_file, "rb"))
    return names_list


def get_closest_names(company_name, clearbit_all, fixednames, n_closest, threshold):
    """
        Input:
            company_name: single company name
            clearbit_all: list with all companies that we will use to match
            n_closest: numer of returned matches
            threshold: prefferred threshold for Levenshtein distance
        Return:
            closest_names: return list with closest names
    """
    # levenshtein distances list with levenshtein distance score for that specific company name
    levenshtein_distances_list = [
        levenshtein_distance(company_name, clearbit_company) for clearbit_company in fixednames]
    # list with n closest indexes from list of all companies
    closest_strings_indices = np.argsort(levenshtein_distances_list)[:n_closest]
    # dict with n closest indexes from list of all companies as keys and corresponding distance score
    key_value_dict = {index: levenshtein_distances_list[index] for index in closest_strings_indices}
    # if threshold is None get list with closest names indexes
    # else get list with closest names indexes and add None for indexes bigger then threshold
    if threshold == 'None':
        closest_names = [clearbit_all[index] for index in closest_strings_indices.tolist()]
        closest_score = [key_value_dict[index] for index in closest_strings_indices.tolist()]
    else:
        closest_names = [clearbit_all[index] if key_value_dict[index] <= int(threshold) else 'None' for index in
                         closest_strings_indices.tolist()]
    return list(zip(closest_names, closest_score))


