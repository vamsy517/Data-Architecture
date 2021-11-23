'''
    Folder for general use code that can be applied to multiple projects
    Version = 1.0
'''

from .logger import logpr
from .request_funcs.get_request import get_request
from .request_funcs.post_request import post_request
from .gbq_funcs.gbq_creds import gbq_credentials
from .gbq_funcs.gbq_upload import gbq_upload
