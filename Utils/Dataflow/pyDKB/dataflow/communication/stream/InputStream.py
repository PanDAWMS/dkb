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
        if self.EOM == '\n':
            self.__iterator = iter(self.fd.readline, "")
        elif self.EOM == '':
            self.__iterator = iter([self.fd.read()])
        else:
            self.__iterator = custom_readline(self.fd, self.EOM)
        return self

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
            return None

    def next(self):
        """ Get next message from the input stream. """
        if not self.__iterator:
            self.__iter__()
        msg = self.__iterator.next()
        return self.parse_message(msg)
