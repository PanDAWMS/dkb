"""
pyDKB.dataflow.communication.producer.Producer
"""

import sys

from . import messageType
from . import logLevel
from . import DataflowException

from .. import Message
from ..stream import StreamBuilder


class ProducerException(DataflowException):
    """ Dataflow Producer exception. """
    pass


class Producer(object):
    """ Data producer implementation. """

    config = None

    message_type = None

    _stream = None

    def __init__(self, config={}):
        """ Initialize Producer instance. """
        self.config = config

    def log(self, message, level=logLevel.INFO):
        """ Output log message with given log level. """
        if not logLevel.hasMember(level):
            self.log("Unknown log level: %s" % level, logLevel.WARN)
            level = logLevel.INFO
        if type(message) == list:
            lines = message
        else:
            lines = message.splitlines()
        if lines:
            out_message = "(%s) (%s) %s" % (logLevel.memberName(level),
                                            self.__class__.__name__, lines[0])
            for l in lines[1:]:
                out_message += "\n(==) %s" % l
            out_message += "\n"
            sys.stderr.write(out_message)

    def reconfigure(self, config={}):
        """ (Re)initialize producer with stage config arguments. """
        if config:
            self.config = config
        self.init_stream()

    def init_stream(self):
        """ Init output stream. """
        dest = self.get_dest()
        if dest:
            self._stream = \
                StreamBuilder(dest, self.config) \
                .setType(self.message_type) \
                .build()

    def get_stream(self):
        """ Get input stream linked to the current dest.

        Return value:
            OutputStream
            None (no dests left to read from)
        """
        if self.reset_stream():
            result = self._stream
        else:
            raise ProducerException("Failed to configure output stream.")
        return result

    def reset_stream(self):
        """ Reset input stream to the current dest. """
        dest = self.get_dest()
        if dest:
            if not self._stream:
                self.init_stream()
            else:
                self._stream.reset(dest)
        return dest

    def set_message_type(self, Type):
        """ Set input message type. """
        self.message_type = Type
        try:
            stream = self.get_stream()
            stream.set_message_type(Type)
        except ProducerException:
            # Stream is not configured yet
            pass

    def message_class(self):
        """ Return message class. """
        if self.message_type:
            result = Message(self.message_type)
        else:
            result = None
        return result

    def get_dest_info(self):
        """ Return current dest info. """
        raise NotImplementedError

    def write(self, msg):
        """ Put new message to the current dest (buffer). """
        self.get_stream().write(msg)

    def eop(self):
        """ Write EOP marker to the current dest. """
        self.get_stream().eop()

    def flush(self):
        """ Flush buffered messages to the current dest. """
        self.get_stream().flush()

    def drop(self):
        """ Drop buffered messages. """
        self.get_stream().drop()

    def close(self):
        """ Close opened data stream and data dest. """
        s = self._stream
        if s and not getattr(s, 'closed', True):
            s.close()
