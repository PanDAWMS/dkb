"""
pyDKB.dataflow.communication.stream
"""

from .. import messageType
from .. import codeType
from .. import logLevel
from .. import DataflowException
from .. import Message
from InputStream import InputStream

from Stream import Stream
from InputStream import InputStream

__all__ = ['StreamBuilder', 'Stream', 'InputStream']


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

    def build(self, config={}):
        """ Create instance of Stream. """
        if not config:
            config = self.config
        instance = self.streamClass(self.fd, config)
        return instance
