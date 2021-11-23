import logging
from datetime import datetime

'''
    General logger function.
    Version = 1.0
'''

def logpr(message):
    logging.info(str(datetime.now()) + ': ' + str(message))
    logging.debug(str(datetime.now()) + ': ' + str(message))
    print(str(datetime.now()) + ': ' + str(message))
