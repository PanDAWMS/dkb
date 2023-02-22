"""
pyDKB.dataflow.communication.stream.OutputStream
"""

from .Stream import Stream
from . import Message


class OutputStream(Stream):
    """ Implementation of the output stream. """

    msg_buffer = []

    def configure(self, config={}):
        """ Configure instance. """
        super(OutputStream, self).configure(config)
        self.EOP = config.get('eop', '')

    def write(self, message):
        """ Add message to the buffer. """
        messageClass = Message(self.message_type)
        if isinstance(message, messageClass):
            self.msg_buffer.append(message)
        elif isinstance(message, list):
            for m in message:
                self.write(m)
        else:
            raise TypeError("OutputStream.write() expects parameter to be of"
                            " type '%s' or 'list' (got '%s')"
                            % (messageClass.__name__, type(message).__name__))

    def flush(self):
        """ Flush buffer to the output stream. """
        for msg in self.msg_buffer:
            self.get_fd().write(msg.encode())
            self.get_fd().write(self.EOM)
        self.drop()

    def eop(self):
        """ Signalize Supervisor about end of process. """
        self.get_fd().write(self.EOP)

    def drop(self):
        """ Drop buffer without sending messages anywhere. """
        self.msg_buffer = []
