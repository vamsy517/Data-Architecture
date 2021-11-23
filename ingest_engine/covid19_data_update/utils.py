import requests


def get_apple_last_date_json() -> str:
    """
    :return: last date from apple website json file
    """
    return requests.get('https://covid19-static.cdn-apple.com/covid19-mobility-data/current/v3/index.json')\
        .json()['mobilityDataVersion'].split(":", 1)[1]





