"""
pyDKB.common.misc

Miscellanious utility functions.
"""

import sys
import inspect
from datetime import datetime
import importlib

from types import logLevel

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
    if type(message) != list:
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
        for l in lines[1:]:
            out_message += "\n(==) %s" % l
        out_message += "\n"
        sys.stderr.write(out_message)


def try_to_import(modname, attrname=None):
    """ Try to import specified module or attribute from a module.

    If module/attribute can not be imported, catch the exception and output log
    message.

    :param modname: module name
    :type modname: str
    :param attrname: attribute name (optional)
    :type attrname: str

    :return: imported module, attribute (or submodule);
             `False` in case of failure.
    :rtype: object
    """
    if attrname:
        err_msg = "Failed to import '%s' from '%s'.\nDetails: " \
                  % (attrname, modname)
    else:
        err_msg = "Failed to import module '%s'.\nDetails: " % (modname)

    try:
        result = importlib.import_module(modname)
        if attrname:
            result = getattr(result, attrname)
    except Exception, err:
        log(err_msg + str(err), logLevel.ERROR)
        result = False

    return result
