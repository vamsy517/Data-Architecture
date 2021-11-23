import itertools
import numpy as np
import hashlib


def get_param_combinations(dict_of_parameters=None):
    combinations_of_parameters = list(
        itertools.product(*list(dict_of_parameters.values()))
    )
    combinations_of_parameters = [
        dict(zip(list(dict_of_parameters.keys()), i))
        for i in combinations_of_parameters
    ]
    return combinations_of_parameters


def overall_combinations(params=None):
    overall_combinations = get_param_combinations(params)

    # multiply by the number of fine-tuning reruns to be done (if not False - at least 1 rerun, meaning multiply
    # by 2)
    n_combinations = len(overall_combinations)

    if params["fine_tune"]:
        n_combinations = n_combinations * (
            len(params["fine_tune"]) + len(params["cols_to_keep"])
        )

    return n_combinations


def clean_domains(text=None):
    if text[:4] == "www.":
        text = text[4:]
    return text


def calculate_cosine_similarity(vector_1, vector_2):
    similarity = np.dot(vector_1, vector_2) / (
        np.linalg.norm(vector_1) * np.linalg.norm(vector_2)
    )
    return similarity


def get_hash(list_of_strings):
    # make sure list is sorted alphabetically and utf-8 encoded after join
    joined_str = ",".join(sorted(list_of_strings)).encode("utf-8")
    # generate the hash and get the 8 last digits
    hash_int = int(hashlib.sha1(joined_str).hexdigest(), 16) % (10 ** 8)
    return hash_int


def transform_function(x):
    return x ** 2
