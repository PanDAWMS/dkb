"""
pyDKB.dataflow.communication.stream.InputStream
"""

from Stream import Stream
from pyDKB.common.types import logLevel
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
            self.is_readable = self._fd_is_readable
        elif self.EOM == '':
            self.__iterator = iter(fd.read, "")
            self.is_readable = self._fd_is_readable
        else:
            self.__iterator = custom_readline(fd, self.EOM)
            self.is_readable = self._gi_is_readable

    def reset(self, fd, close=True, force=False):
        """ Reset current stream with new file descriptor.

        Overrides parent method to reset __iterator property.

        :param fd: new file descriptor
        :type fd: file
        :param close: if True, close the old file descriptor
        :type close: bool
        :param force: if True, force the reset of iterator
                      (normally, iterator is not reset if the new
                      file descriptor is the same as the old one)
        :type force: bool

        :returns: old file descriptor
        :rtype: file
        """
        old_fd = super(InputStream, self).reset(fd, close)
        # We do not want to reset iterator if `reset()` was called
        # with the same `fd` as before.
        if force or (old_fd and fd != old_fd):
            self._reset_iterator()
        return old_fd

    def is_readable(self):
        """ Check if current input stream is readable.

        :returns: None  -- not initialized,
                  False -- empty,
                  True  -- not empty
        :rtype: bool, NoneType
        """
        return self._unknown_is_readable()

    def _unknown_is_readable(self):
        """ Placeholder: readability test for not initialized stream.

        This function is needed in case that we need to reset `is_readable`
        and the whole Stream object back to the "undefined" state.

        :returns: None
        :rtype: NoneType
        """
        return None

    def _fd_is_readable(self):
        """ Check if bound file descriptor is readable.

        :returns: None  -- not initialized,
                  False -- empty,
                  True  -- not empty
        :rtype: bool, NoneType
        """
        fd = self.get_fd()
        if not fd:
            result = None
        elif getattr(fd, 'closed', True):
            result = False
        elif fd.fileno() == sys.stdin.fileno():
            result = True
        else:
            stat = os.fstat(fd.fileno())
            result = fd.tell() != stat.st_size
        return result

    def _gi_is_readable(self):
        """ Check if the generator iterator can return value on `next()` call.

        :returns: False -- empty,
                  True  -- not empty
        :rtype: bool, NoneType
        """
        try:
            return self.__iterator.send(True)
        except StopIteration:
            return False
        except TypeError:
            # If method 'next()' was never called yet,
            # sending anything but None raises TypeError
            return self._fd_is_readable()

    def parse_message(self, message):
        """ Verify and parse input message.

        :param message: message to parse
        :type message: pyDKB.dataflow.communication.messages.AbstractMessage

        :returns: decoded message or False if parsing failed
        :rtype: pyDKB.dataflow.communication.messages.AbstractMessage, bool
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

    def get_raw_item(self):
        """ Get next raw item from the input stream.

        Raw item is a sequence of symbols that corresponds
        to the supervisor/worker communication protocol object.
        Known objects:
        * message (``<raw_data>+<EOM>``).

        :returns: next raw item
        :rtype: str
        """
        if not self.__iterator:
            self._reset_iterator()
        return self.__iterator.next()

    def get_item(self):
        """ Get next stream item (constructed of raw items).

        Stream item is an object representing some of the supervisor/worker
        communication protocol objects (e.g. ``Message``).

        :returns: parsed next item,
                  False -- parsing failed,
                  None -- no messages left
        :rtype: pyDKB.dataflow.communication.messages.AbstractMessage,
                bool, NoneType
        """
        try:
            msg = self.get_raw_item()
        except StopIteration:
            return None
        if not msg.endswith(self.EOM):
            log_msg = msg[:10] + '<...>' * (len(msg) > 20)
            log_msg += msg[-min(len(msg) - 10, 10):]
            log_msg = log_msg.replace('\n', r'\n')
            self.log("Unexpected end of stream, skipping rest of input:\n"
                     "'%s'" % log_msg, logLevel.WARN)
            return False
        else:
            if self.EOM != '':
                msg = msg[:-len(self.EOM)]
            result = self.parse_message(msg)
        return result

    def next(self):
        """ Get next item (message) from the input stream.

        :raises: StopIteration: no items left
        :returns: parsed next item,
                  False -- parsing failed or unexpected end of stream occurred
        :rtype: pyDKB.dataflow.communication.messages.AbstractMessage, bool
        """
        result = self.get_item()
        if result is None:
            raise StopIteration
        else:
            return result
