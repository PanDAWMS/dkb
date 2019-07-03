"""
pyDKB.dataflow.communication.producer.Producer
"""

import sys

from pyDKB.common.types import logLevel
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
        self.reconfigure()

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
        """ Init output stream (without real destination). """
        self._stream = \
            StreamBuilder(None, self.config) \
            .setStream('output') \
            .setType(self.message_type) \
            .build()

    def get_stream(self, actualize=True):
        """ Get output stream linked to the current dest.

        If $actualize parameter set to True, will try to reset current stream
        destination; else will use last known destination or None.
        """
        if actualize:
            self.reset_stream()
        elif not self._stream:
            self.init_stream()
        result = self._stream
        if not result:
            raise ProducerException("Failed to configure output stream.")
        return result

    def reset_stream(self):
        """ Reset input stream to the current dest. """
        dest = self.get_dest()
        if dest:
            if not self._stream:
                self.init_stream()
            self._stream.reset(dest)
        return dest

    def set_message_type(self, Type):
        """ Set input message type. """
        self.message_type = Type
        try:
            stream = self.get_stream(False)
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

    def get_dest(self):
        """ Return current destination. """
        raise NotImplementedError

    def get_dest_info(self):
        """ Return current dest info. """
        raise NotImplementedError

    def write(self, msg):
        """ Put new message to the current dest (buffer). """
        self.get_stream(False).write(msg)

    def eop(self):
        """ Write EOP marker to the current dest. """
        self.get_stream().eop()

    def flush(self):
        """ Flush buffered messages to the current dest. """
        self.get_stream().flush()

    def drop(self):
        """ Drop buffered messages. """
        self.get_stream(False).drop()

    def close(self):
        """ Close opened data stream and data dest. """
        s = self._stream
        if s and not getattr(s, 'closed', True):
            s.close()
