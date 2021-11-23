from google.oauth2 import service_account
from google.cloud import bigquery, bigquery_storage
import pandas_gbq

from ..logger import logpr

def gbq_credentials(filename='gbq_credentials.json', project_id='project-pulldata'):
    '''
    Function to retrieve credentials for GBQ
    Parameters:
        filename - path to the file with service account information
        project_id - project id inside GBQ


    Version = 1.2
    '''

    logpr('Getting GBQ credentials')
    try:
        credentials = service_account.Credentials.from_service_account_file(filename)
        pandas_gbq.context.credentials = credentials
        pandas_gbq.context.project = project_id
        client = bigquery.Client.from_service_account_json(filename)

        asd = {'bqclient' :  bigquery.Client(credentials=credentials, project=project_id),
        'bqstorageclient' : bigquery_storage.BigQueryReadClient(credentials=credentials)}
        
        return asd
    except Exception as e:
        logpr(e)