"""
pyDKB.dataflow.communication
"""

from .. import messageType
from .. import codeType
from .. import logLevel
from .. import DataflowException
from messages import Message
from Stream import Stream
from InputStream import InputStream
from OutputStream import OutputStream

import consumer

__all__ = ['Message', 'StreamBuilder', 'Stream', 'InputStream', 'OutputStream']


class StreamBuilder(object):
    """ Constructor for Stream object.  """

    message_type = None
    streamClass = None

    def __init__(self, fd, config={}):
        """ Initialize Stream builder.

        :param fd: open file descriptor
                   TODO: IOBase objects
        """
        self.fd = fd
        self.config = config
        if fd.mode == 'r':
            self.streamClass = InputStream
        elif fd.mode == 'w':
            self.streamClass = OutputStream
        else:
            raise ValueError("Unknown file mode for the Stream: '%s'" % mode)

    def setType(self, Type):
        """ Set message type for the Stream. """
        if not (Type is None or messageType.hasMember(Type)):
            raise ValueError("Unknown message type: %s" % Type)
        self.message_type = Type
        return self

    def build(self, config={}):
        """ Create instance of Stream. """
        if not config:
            config = self.config
        instance = self.streamClass(self.fd, config)
        if self.message_type:
            instance.set_message_type(self.message_type)
        return instance
