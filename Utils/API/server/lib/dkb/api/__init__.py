"""
DKB API server module.
"""

import logging

from exceptions import DkbApiException
import methods

CONFIG_DIR = '%%CFG_DIR%%'


__version__ = '0.2.dev20200219'


STATUS_CODES = {
    200: 'OK',
    250: 'Some Information Missed',
    251: 'Method Description Missed',
    400: 'Bad Request',
    460: 'Category Failure',
    461: 'Category Not Found',
    462: 'Invalid Category Name',
    470: 'Method Failure',
    471: 'Method Not Found',
    472: 'Method Already Exists',
    473: 'Missed Argument',
    474: 'Invalid Argument',
    500: 'Internal Server Error',
    501: 'Not Implemented',
    550: 'Storage Failure',
    551: 'Storage Client Failure',
    560: 'Storage Query Failure',
    561: 'Storage Query Not Found',
    562: 'Storage Query Parameter Missed',
    590: 'Configuration Error',
    591: 'Configuration Not Found'
}


def configure():
    """ Configure API server methods.

    :return: True if configuration succeed, else False
    :rtype: bool
    """
    retval = True
    try:
        methods.configure()
    except DkbApiException, err:
        logging.fatal("Server configuration failed: %s." % err)
        trace = traceback.format_exception(*sys.exc_info())
        for line in trace:
            logging.debug(trace)
        retval = False
    return retval
