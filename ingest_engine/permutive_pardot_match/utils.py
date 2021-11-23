import re
import pandas as pd
from pardot_alchemer_integration.utils import get_mails
from ingest_utils.constants import LOGS_FOLDER


def get_prospect_data(email: str, api_key: str, user_key: str) -> str:
    """
    :param email: specific email
    :param api_key: pardot api key
    :param user_key: pardot user key
    :return: prospect_id in case the email matches,in other case return error message
    """
    # get request to return the data
    response = get_mails(email, api_key, user_key)
    # check api key, if it is not correct return message
    if 'err code="1"' in response.text:
        print('Invalid api key.')
        return 'invalid key'
    # check if email exists in pardot data, if it doesnt exist return a message
    if 'err code="4"' in response.text or '' == response.text:
        print('The email does not exist in Pardot.')
        return 'invalid email'
    # if the email is correct and exists in pardot, get the prospect_id associated with it
    prospect_id = re.findall("<id>(.*?)</id>", response.text)[0]
    return prospect_id


def fix_log_error(emails: pd.DataFrame, log_file: str) -> bool:
    """
    If the dag fails save the max and min date from the downloaded dataframe
    from permutive to avoid loosing data from one day with fails.
    :param emails: DataFrame with emails from today
    :param log_file: file to log results
    :return: True if log successfully
    """
    max_date = emails.time.max()
    min_date = emails.time.min()
    with open(f'{LOGS_FOLDER}{log_file}', 'a') as fl:
        fl.write(f'[{str(max_date)} , {str(min_date)}]')
    return True

