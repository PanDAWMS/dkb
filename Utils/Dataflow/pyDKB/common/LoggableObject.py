"""
pyDKB.common.LoggableObject
"""

import sys

from types import logLevel


class LoggableObject(object):
    """ Common ancestor for all classes that need 'log' method. """

    @classmethod
    def log(cls, message, level=logLevel.INFO):
        """ Output log message with given log level.

        :param message: message to output
        :type message: str
        :param level: log level of the message
        :type level: ``pyDKB.common.types.logLevel`` member
        """
        if not logLevel.hasMember(level):
            self.log("Unknown log level: %s" % level, logLevel.WARN)
            level = logLevel.INFO
        if type(message) == list:
            lines = message
        else:
            lines = message.splitlines()
        if lines:
            out_message = "(%s) (%s) %s" % (logLevel.memberName(level),
                                            cls.__name__, lines[0])
            for l in lines[1:]:
                out_message += "\n(==) %s" % l
            out_message += "\n"
            sys.stderr.write(out_message)
