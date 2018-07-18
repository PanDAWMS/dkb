"""
pyDKB.dataflow.communication.consumer.Consumer
"""

import sys
from collections import defaultdict

from . import messageType
from . import DataflowException

from .. import Message
from ..stream import StreamBuilder

from pyDKB.common import logging


class ConsumerException(DataflowException):
    """ Dataflow Consumer exception. """
    pass


class Consumer(object):
    """ Data consumer implementation. """

    logger = logging.getLogger(__name__)

    config = None

    message_type = None

    _stream = None

    def __init__(self, config={}):
        """ Initialize Consumer instance. """
        self.config = config
        self.reconfigure()

    def log(self, message, level=logging.INFO):
        """ Output log message with given log level. """
        self.logger.log(level, message)

    def __iter__(self):
        """ Initialize iteration. """
        return self

    def reconfigure(self, config={}):
        """ (Re)initialize consumer with stage config arguments. """
        if config:
            self.config = config

    def init_stream(self):
        """ Init input stream. """
        src = self.get_source()
        if src:
            self._stream = \
                StreamBuilder(src, self.config) \
                .setStream('input') \
                .setType(self.message_type) \
                .build()

    def stream_is_empty(self):
        """ Check if current stream is empty.

        Return value:
            True  (empty)
            False (not empty)
            None  (_stream is not defined or not initialized)
        """
        if not self._stream:
            return None
        return self._stream.is_empty()

    def get_stream(self):
        """ Get input stream linked to the current source.

        Return value:
            InputStream
            None (no sources left to read from)
        """
        if not self.stream_is_empty():
            result = self._stream
        elif self.reset_stream():
            result = self._stream
        else:
            result = None
        return result

    def reset_stream(self):
        """ Reset input stream to the current source. """
        src = self.get_source()
        if src:
            if not self._stream:
                self.init_stream()
            else:
                self._stream.reset(src)
        return src

    def set_message_type(self, Type):
        """ Set input message type. """
        self.message_type = Type
        stream = self.get_stream()
        if stream:
            stream.set_message_type(Type)

    def message_class(self):
        """ Return message class. """
        return Message(self.message_type)

    def get_source_info(self):
        """ Return current source info. """
        raise NotImplementedError

    def get_message(self):
        """ Get new message from current source.

        Return values:
            Message object
            False (failed to parse message)
            None  (all input sources are empty)
        """
        s = self.get_stream()
        if not s:
            msg = None
        else:
            msg = s.get_message()
        return msg

    def next(self):
        """ Return new Message, read from input stream. """
        msg = self.get_message()
        if msg is None:
            raise StopIteration
        return msg

    def close(self):
        """ Close opened data stream and data source. """
        for s in (self.get_stream(), self.get_source()):
            if s and not getattr(s, 'closed', True):
                s.close()
