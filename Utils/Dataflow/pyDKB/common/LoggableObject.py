"""
pyDKB.common.LoggableObject
"""

from types import logLevel
from misc import log


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
        log(message, level, cls.__name__)
