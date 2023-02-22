"""
pyDKB.common.misc

Miscellanious utility functions.
"""

import sys
import inspect
from datetime import datetime
import time

from .types import logLevel

# Datetime format for log messages
DTFORMAT = '%Y-%m-%d %H:%M:%S'


def log(message, level=logLevel.INFO, *args):
    """ Output log message with given log level.

    In case of multiline messages or list of messages only first line (message)
    is prepended with provided prefixes and timestamp; in all the next lines
    (messages) they are replaced with special prefix '(==)', representing that
    these lines belong to the same log record.

    Empty lines and lines containing only whitespace symbols are ignored.

    :param message: message to output (string, list of strings or
                    any other object)
    :type message: object
    :param level: log level of the message
    :type level: ``pyDKB.common.types.logLevel`` member
    :param *args: additional prefixes (will be output between log
                  level prefix and message body)
    :type *args: str
    """
    if not logLevel.hasMember(level):
        log("Unknown log level: %s" % level, logLevel.WARN)
        level = logLevel.INFO
    if not isinstance(message, list):
        message = [message]
    lines = []
    for m in message:
        lines += [line for line in str(m).splitlines() if line.strip()]
    if args:
        prefix = ' ' + ' '.join(['(%s)' % p for p in args])
    else:
        frm = inspect.stack()[1]
        mod = inspect.getmodule(frm[0])
        modname = getattr(mod, '__name__', 'main')
        prefix = ' (%s)' % modname
    if lines:
        dtime = datetime.now().strftime(DTFORMAT)
        out_message = "%s (%s)%s %s" % (dtime, logLevel.memberName(level),
                                        prefix, lines[0])
        for line in lines[1:]:
            out_message += "\n(==) %s" % line
        out_message += "\n"
        sys.stderr.write(out_message)


def ensure_argparse_arg_name(args, kwargs):
    """ Ensure cmgline argument name for `argparse.add_argument()`.

    Returns the expected argument name and updates ``kwargs['dest']``
    value.

    When :python:func:`argparse.add_argument()` is called, `argparse`
    decides how to name the new argument basing on the passed parameters
    (to use this name in `argparse.Namespace` object).
    This function returns the name, that (supposedly) will be used in
    `argparse`; but to make sure, ``kwargs['dest']`` is (re)set to the same
    value (if applicable).

    For parameters description please refer to `argparse` documentation:
    https://docs.python.org/2.7/library/argparse.html#the-add-argument-method

    :param args: list of `argparse.add_argument()` positional parameters
    :type args: list
    :param kwargs: hash of `argparse.add_argument()` keyword parameters
    :type kwargs: dict

    :return: argument name
    :rtype: string
    """
    arg_name = None
    positional = False
    if 'dest' in kwargs:
        # Use the user-specified argument name
        arg_name = kwargs['dest']
    else:
        # Get the name from `args` (a list of synonymous options
        # or a single positional argument name)
        for arg in args:
            if not arg.startswith('-'):
                # `arg` is a positional argument name
                arg_name = arg
                positional = True
                break
            if arg.startswith('--'):
                # first long option will be used as argument name
                arg_name = arg[2:]
                break
            if not arg_name and arg.startswith('-'):
                # if no long options specified, the first short one will
                # be used
                arg_name = arg[1:]

    # Set 'dest' (arg name used within the parser) to the value
    # we're going to use in code, if applicable (positional argument
    # with explicit `dest` will produce a error)
    if not positional:
        kwargs['dest'] = arg_name

    return arg_name


def execute_with_retry(f, args=[], kwargs={}, retry_on=(Exception,),
                       max_tries=3, sleep=5):
    """ Try to call function `f` and retry in case of error.

    :param f: function to call
    :type f: callable
    :param args: positional arguments
    :type args: list
    :param kwargs: keyword arguments
    :type kwargs: dict

    :param retry_on: which exceptions should be retried
    :type retry_on: set
    :param max_tries: max number of retries
    :type max_tries: int
    :param sleep: pause between retries (sec)
    :type sleep: int

    :return: `f` execution result
    :rtype: object
    """
    attempt = 0
    result = None
    while attempt < max_tries:
        attempt += 1
        try:
            result = f(*args, **kwargs)
            break
        except retry_on as e:
            if attempt >= max_tries:
                raise e
            log("Function call failed ('%s': %i/%i).\n"
                "Reason: %s.\n"
                "Wait for %i sec before retry..."
                % (f.__name__, attempt, max_tries, str(e), sleep))
            time.sleep(sleep)
    return result
