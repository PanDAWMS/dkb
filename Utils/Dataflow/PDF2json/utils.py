#!/bin/env python

'''
'''

import os


def path_join(a, b):
    """ Wrapper around os.path.join.
    This wrapper is required to account for possible different
    separators in paths.
    """
    return os.path.join(a, b).replace("\\", "/")
