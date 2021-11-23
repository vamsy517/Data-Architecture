from requests import get
import sys

from ..logger import logpr

def get_request(url='', params='', headers=''):
    '''
    General function to make a GET request to an API.
    Parameters:
        url     - string;   url of the API
        params  - dict;     API specific parameters that the API takes 
        headers - dict;     API specific headers
    Output:
        JSON response

    Version = 1.1
    '''
    #logpr('GET request for: {}'.format(str(url)))

    try:
        response = get(url=url, params=params, headers=headers)
        logpr('GET status: {}; {}'.format(response.status_code,url))
        return response
    except Exception as e:
        logpr(e)
        sys.exit('Stopping process..')


    