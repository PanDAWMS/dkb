"""
pyDKB.dataflow.communication.consumer.Consumer
"""

import sys

from pyDKB.common import LoggableObject
from . import DataflowException

from .. import Message
from ..stream import StreamBuilder


class ConsumerException(DataflowException):
    """ Dataflow Consumer exception. """
    pass


class Consumer(LoggableObject):
    """ Data consumer implementation. """

    config = None

    message_type = None

    _stream = None

    def __init__(self, config={}):
        """ Initialize Consumer instance. """
        self.config = config
        self.reconfigure()

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

    def stream_is_readable(self):
        """ Check if input data stream is readable.

        :returns: True  -- stream is initialized and not empty,
                  False -- stream is empty,
                  None  -- stream is not initialized
        :rtype: bool, NoneType
        """
        if not self._stream:
            return None
        return self._stream.is_readable()

    def get_stream(self):
        """ Get input stream linked to the current source.

        Return value:
            InputStream
            None (no sources left to read from)
        """
        if self.reset_stream():
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

    def get_raw_item(self):
        """ Get new raw (stream) item from current source.

        Raw (stream) item is an object representing some of
        the supervisor/worker communication protocol objects.

        Return values:
            Message object
            False (failed to parse message)
            None  (all input sources are empty)
        """
        s = self.get_stream()
        if not s:
            msg = None
        else:
            msg = next(s)
        return msg

    def get_item(self):
        """ Get next processing item (constructed of raw items).

        Processing item is the smallest data unit for stage processing loop
        (e.g. ``ProcessorStage``).

        :returns: parsed next item,
                  False -- parsing failed,
                  None -- no messages left
        :rtype: pyDKB.dataflow.communication.messages.AbstractMessage,
                bool, NoneType
        """
        return self.get_raw_item()

    def __next__(self):
        """ Get next processing item (Message) from current source. """
        msg = self.get_item()
        if msg is None:
            raise StopIteration
        return msg

    def close(self):
        """ Close opened data stream and data source. """
        for s in (self.get_stream(), self.get_source()):
            if s and not getattr(s, 'closed', True):
                s.close()
