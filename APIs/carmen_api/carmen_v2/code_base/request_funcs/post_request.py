from requests import post
import sys

from ..logger import logpr

def post_request(params):#auth='', url='', data={}, headers={}):
    '''
    General function to make a POST request to an API.
    Parameters:
        url     - string;   url of the API
        data    - dict; a dictionary with the necessary data to fulfill the POST request
    Output:
        JSON response

    Version = 1.2
    '''
    #logpr('GET request for: {}'.format(str(url)))

    try:
        response = post(**params)
        logpr('POST status: {}; {}'.format(response.status_code,response.url))
        return response
    except Exception as e:
        logpr(e)
        sys.exit('Stopping process..')