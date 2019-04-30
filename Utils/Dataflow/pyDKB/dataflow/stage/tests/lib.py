#!/usr/bin/env python

""" Common functions for testing ProcessorStage. """

import cStringIO
import sys


def isolate_function_error(f, *args):
    """ Silence and retrieve the function's error message.

    The function is expected to throw a SystemExit when run with
    specific arguments. Error stream is redirected into a string during the
    function's execution, and the resulting messages can be analyzed.

    :param f: function to execute
    :type f: function
    :param args: arguments to execute function with
    :type args: list

    :return: list with two members, first one is the error message,
             second one is the function's return
    :rtype: list
    """
    buf = cStringIO.StringIO()
    temp_err = sys.stderr
    sys.stderr = buf
    try:
        result = f(*args)
    except SystemExit:
        result = None
    sys.stderr = temp_err
    buf.seek(0)
    msg = buf.read()
    buf.close()
    return [msg, result]
