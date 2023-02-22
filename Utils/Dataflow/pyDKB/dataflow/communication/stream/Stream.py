"""
pyDKB.dataflow.commuication.stream.Stream
"""

import sys
from io import IOBase

from pyDKB.common import LoggableObject
from . import messageType
from .exceptions import StreamException


class Stream(LoggableObject):
    """ Abstract class for input/output streams. """

    message_type = None
    EOM = None
    _fd = None

    def __init__(self, fd=None, config={}):
        """ Initialization of Stream object. """
        self.reset(fd)
        self.configure(config)

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
        :return: previous file descriptor (or None)
        """
        if not isinstance(fd, (IOBase, None.__class__)):
            raise TypeError("Stream.reset() expects first parameter of type"
                            " 'file' (got '%s')" % fd.__class__.__name__)
        old_fd = self._fd
        if close and old_fd != fd:
            self.close()
        self._fd = fd
        return old_fd

    def get_fd(self):
        """ Return open file descriptor or raise exception. """
        if not self._fd:
            raise StreamException("File descriptor is not configured")
        return self._fd

    def close(self):
        """ Close open file descriptors etc. """
        if self._fd and not self._fd.closed:
            self._fd.close()

    def __del__(self):
        """ Destructor. """
        self.close()
