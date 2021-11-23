from requests import get
import sys

from ..logger import logpr

def get_request(params):#url='', params='', headers='', auth=''):
    '''
    General function to make a GET request to an API.
    Parameters:
        url     - string;   url of the API
        params  - dict;     API specific parameters that the API takes 
        headers - dict;     API specific headers
    Output:
        JSON response

    Version = 1.2
    '''

    try:
        response = get(**params)
        logpr('GET status: {}; {}'.format(response.status_code, response.url))
        return response
    except Exception as e:
        logpr(e)
        sys.exit('Stopping process..')


    