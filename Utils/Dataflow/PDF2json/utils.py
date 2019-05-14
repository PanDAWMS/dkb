#!/bin/env python

'''
'''

from datetime import datetime
import os
import sys


def path_join(a, b):
    """ Wrapper around os.path.join.
    This wrapper is required to account for possible different
    separators in paths.
    """
    return os.path.join(a, b).replace("\\", "/")


def log(msg, prefix='DEBUG'):
    ''' Add prefix and current time to message and write it to stderr. '''
    prefix = '(%s)' % (prefix)
    # 11 = len("(CRITICAL) "), where CRITICAL is the longest log level name.
    prefix = prefix.ljust(11)
    sys.stderr.write('%s%s %s\n' % (prefix, datetime.now().isoformat(), msg))
