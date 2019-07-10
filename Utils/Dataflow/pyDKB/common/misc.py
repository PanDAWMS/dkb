"""
pyDKB.common.misc

Miscellanious utility functions.
"""

import sys

from types import logLevel


def log(message, level=logLevel.INFO, *args):
    """ Output log message with given log level.

    :param message: message to output
    :type message: str
    :param level: log level of the message
    :type level: ``pyDKB.common.types.logLevel`` member
    :param *args: additional prefixes (will be output between log
                  level prefix and message body)
    :type *args: str
    """
    if not logLevel.hasMember(level):
        self.log("Unknown log level: %s" % level, logLevel.WARN)
        level = logLevel.INFO
    if type(message) == list:
        lines = message
    else:
        lines = message.splitlines()
    if args:
        prefix = ' ' + ' '.join(['(%s)' % p for p in args])
    else:
        prefix = ''
    if lines:
        out_message = "(%s)%s %s" % (logLevel.memberName(level),
                                     prefix, lines[0])
        for l in lines[1:]:
            out_message += "\n(==) %s" % l
        out_message += "\n"
        sys.stderr.write(out_message)
