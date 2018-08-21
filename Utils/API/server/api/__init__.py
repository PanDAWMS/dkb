"""
DKB API server module.
"""

import logging

ADDR = '%%ADDR%%'

from exceptions import DkbApiException
import methods


STATUS_CODES = {
    200: 'OK',
    250: 'Some Information Missed',
    251: 'Method Description Missed',
    400: 'Bad Request',
    461: 'Category Not Found',
    462: 'Method Not Found',
    471: 'Missed Argument',
    472: 'Invalid Argument',
    500: 'Internal Server Error',
    501: 'Not Implemented',
    570: 'Method Failed'
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
