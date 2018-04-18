"""
pyDKB.dataflow.communication.StreamBuilder
"""

from .. import messageType
from Stream import StreamException
from InputStream import InputStream
from OutputStream import OutputStream


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
            self.setStream('input')
        elif fd.mode == 'w':
            self.setStream('output')

    def setStream(self, stream):
        """ Set stream type: 'input' or 'output'. """
        streams = {
            'input': InputStream,
            'output': OutputStream
        }
        if stream not in streams:
            raise ValueError("setStream(): unknown stream type '%s'"
                             " (expected one of: %s)"
                             % (stream, ', '.join(streams)))
        self.streamClass = streams[stream]
        return self

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
        if not self.streamClass:
            raise StreamException("Stream class is not configured.")
        instance = self.streamClass(self.fd, config)
        if self.message_type:
            instance.set_message_type(self.message_type)
        return instance
