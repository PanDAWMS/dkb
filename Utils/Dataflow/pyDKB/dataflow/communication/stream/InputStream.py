"""
pyDKB.dataflow.communication.stream.InputStream
"""

from Stream import Stream
from . import messageType
from . import logLevel
from . import Message
from pyDKB.common import custom_readline


class InputStream(Stream):
    """ Implementation of the input stream. """

    __iterator = None

    def __iter__(self):
        """ Initialize iteration. """
        self._reset_iterator()
        return self

    def _reset_iterator(self):
        """ Reset inner iterator on a new file descriptor. """
        if self.EOM == '\n':
            self.__iterator = iter(self.fd.readline, "")
        elif self.EOM == '':
            self.__iterator = iter([self.fd.read()])
        else:
            self.__iterator = custom_readline(self.fd, self.EOM)

    def reset(self, fd, close=True):
        """ Reset current stream with new file descriptor.

        Overrides parent method to reset __iterator property.
        """
        super(InputStream, self).reset(fd, close)
        # Not _reset_iterator(), as we are not sure someone
        # will ask for new messages -- then why read the whole file
        # in advance (if EOM appears to be '')?
        self.__iterator = None

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
