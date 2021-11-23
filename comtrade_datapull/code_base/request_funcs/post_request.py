from requests import post
import sys

from ..logger import logpr

def post_request(auth, url='', data={}, headers={}):
    '''
    General function to make a POST request to an API.
    Parameters:
        url     - string;   url of the API
        data    - dict; a dictionary with the necessary data to fulfill the POST request
    Output:
        JSON response

    Version = 1.1
    '''
    #logpr('GET request for: {}'.format(str(url)))

    try:
        response = post(url=url, data=data, auth=auth)
        logpr('GET status: {}; {}'.format(response.status_code,url))
        return response
    except Exception as e:
        logpr(e)
        sys.exit('Stopping process..')