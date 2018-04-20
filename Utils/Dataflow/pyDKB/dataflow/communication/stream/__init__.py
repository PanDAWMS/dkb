"""
pyDKB.dataflow.communication.stream
"""

from .. import messageType
from .. import codeType
from .. import logLevel
from .. import DataflowException
from .. import Message
from InputStream import InputStream

__all__ = ['Stream']


def Stream(fd, config={}):
    """ Constructor for Stream object.

        :param fd: open file descriptor
                   TODO: IOBase objects
    """
    if fd.mode == 'r':
        cls = InputStream
    else:
        raise ValueError("Unknown file mode for the Stream: '%s'" % mode)
    return cls(fd, config)
