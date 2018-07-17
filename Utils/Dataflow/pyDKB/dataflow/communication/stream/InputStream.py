"""
pyDKB.dataflow.communication.stream.InputStream
"""

from Stream import Stream
from . import messageType
from . import logLevel
from . import Message
from pyDKB.common import custom_readline

import os
import sys


class InputStream(Stream):
    """ Implementation of the input stream. """

    __iterator = None

    def __iter__(self):
        """ Initialize iteration. """
        self._reset_iterator()
        return self

    def _reset_iterator(self):
        """ Reset inner iterator on a new file descriptor. """
        fd = self.get_fd()
        if self.EOM == '\n':
            self.__iterator = iter(fd.readline, "")
            self.is_empty = self._is_fd_empty
        elif self.EOM == '':
            self.__iterator = iter(fd.read, "")
            self.is_empty = self._is_fd_empty
        else:
            self.__iterator = custom_readline(fd, self.EOM)
            self.is_empty = self._is_generator_empty

    def reset(self, fd, close=True, force=False):
        """ Reset current stream with new file descriptor.

        Overrides parent method to reset __iterator property.
        """
        super(InputStream, self).reset(fd, close)
        # We do not want to reset iterator if `reset()` was called
        # with the same `fd` as before.
        if force or fd != self.get_fd():
            self._reset_iterator()

    def _is_unknown_empty(self):
        """ Implementation of `is_empty()` for not initialized iterator. """
        return None

    is_empty = _is_unknown_empty

    def _is_fd_empty(self):
        """ Implement `is_empty()` method for read/readline iterator. """
        fd = self._fd
        if not fd or getattr(fd, 'closed', True):
            return False
        if fd.fileno() == sys.stdin.fileno():
            return False
        stat = os.fstat(fd.fileno())
        return fd.tell() == stat.st_size

    def _is_generator_empty(self):
        """ Implement `is_empty()` method for generator. """
        return self.__iterator.send(True)

    def parse_message(self, message):
        """ Verify and parse input message.

        Retrun value:
            Message object
            False (failed to parse)
        """
        messageClass = Message(self.message_type)

        try:
            msg = messageClass(message)
            msg.decode()
            return msg
        except (ValueError, TypeError), err:
            self.log("Failed to read input message as %s.\n"
                     "Cause: %s\n"
                     "Original message: '%s'"
                     % (messageClass.typeName(), err, message),
                     logLevel.WARN)
            return False

    def get_message(self):
        """ Get next message from the input stream.

        Return values:
            Message object
            False (failed to parse message)
            None  (no messages left)
        """
        try:
            result = self.next()
        except StopIteration:
            result = None
        return result

    def next(self):
        """ Get next message from the input stream. """
        if not self.__iterator:
            self._reset_iterator()
        msg = self.__iterator.next()
        return self.parse_message(msg)
