"""
pyDKB.dataflow.commuication.Stream
"""

from . import messageType
from . import logLevel
from messages import Message

import sys


class Stream(object):
    """ Abstract class for input/output streams. """

    message_type = None
    fd = None

    def __init__(self, fd, config={}):
        """ Initialization of Stream object. """
        self.reset(fd)
        self.configure(config)

    def log(self, message, level=logLevel.INFO):
        """ Output log message with given log level. """
        if not logLevel.hasMember(level):
            self.log("Unknown log level: %s" % level, logLevel.WARN)
            level = logLevel.INFO
        if type(message) == list:
            lines = message
        else:
            lines = message.splitlines()
        out_message = "(%s) (%s) %s" % (logLevel.memberName(level),
                                        self.__class__.__name__,
                                        lines[0])
        for l in lines[1:]:
            out_message += "\n(==) %s" % l
        out_message += "\n"
        sys.stderr.write(out_message)

    def configure(self, config):
        """ Stream configuration. """
        if not isinstance(config, dict):
            raise TypeError("Stream.configure() expects parameter of type"
                            " 'dict' (got '%s')" % config.__class__.__name__)
        self.EOM = config['eom']

    def set_message_type(self, msg_type):
        """ Set type of the messages in the stream. """
        if not messageType.hasMember(msg_type):
            raise ValueError("Unknown message type: %s" % msg_type)
        self.message_type = msg_type

    def message_type(self):
        """ Get type of the messages in the stream. """
        return self.message_type

    def reset(self, fd, close=True):
        """ Reset file descriptor in operation.

        :param fd: open file descriptor
                   TODO: IOBase objects
        """
        if not isinstance(fd, file):
            raise TypeError("Stream.reset() expects first parameter of type"
                            " 'file' (got '%s')" % fd.__class__.__name__)
        if close and self.fd != fd:
            self.close()
        self.fd = fd

    def close(self):
        """ Close open file descriptors etc. """
        if self.fd and not self.fd.closed:
            self.fd.close()
