#!/bin/env python

'''
'''

from datetime import datetime
import os
import sys


def path_join(a, b):
    ''' Wrapper around os.path.join.
    This wrapper is required to account for possible different
    separators in paths.

    :param a: first path to join
    :type a: str
    :param b: second path to join
    :type b: str

    :return: resulting path
    :rtype: str
    '''
    return os.path.join(a, b).replace("\\", "/")


def log(msg, prefix='DEBUG'):
    ''' Add prefix and current time to message and write it to stderr.

    :param msg: message
    :type msg: str
    :param prefix: log level indicating the type/importance of the message.
    :type prefix: str
    '''
    prefix = '(%s)' % (prefix)
    # 11 = len("(CRITICAL) "), where CRITICAL is the longest log level name.
    prefix = prefix.ljust(11)
    sys.stderr.write('%s%s %s\n' % (prefix, datetime.now().isoformat(), msg))
