from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq

from ..logger import logpr

def gbq_upload(df, params):
    '''
    Function to upload data from a pandas dataframe to GBQ.
    Parameters:
        df - a pandas dataframe
        params - dictionary of any parameters that apply to df.to_gbq() function

        params={'destination_table':'ComTrade.commodities_monthly', 'project_id':'project-pulldata', 'if_exists':'replace'}

    Version = 1.0
    '''
    # destination_table=f'{dataset}.{table}',project_id = project_id, if_exists = 'replace'
    try:
        df.to_gbq(**params)
    except Exception as e:
        logpr(e)