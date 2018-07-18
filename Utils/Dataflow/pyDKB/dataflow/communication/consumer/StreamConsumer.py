"""
pyDKB.dataflow.communication.consumer.StreamConsumer

Data consumer implementation for a single stream.

TODO: think about multiple streams (like a number of named
      pipes, etc). Prehaps, even merge this class with FileConsumer.
"""

import sys
import os

import Consumer
from . import DataflowException

from pyDKB.common import logging


class StreamConsumer(Consumer.Consumer):
    """ Data consumer implementation for Stream data source. """

    logger = logging.getLogger(__name__)

    fd = None

    # Override
    def reconfigure(self, config={}):
        """ (Re)configure Stream consumer. """
        self.fd = sys.stdin
        super(StreamConsumer, self).reconfigure(config)

    def get_source_info(self):
        """ Return current source info. """
        return {'fd': self.fd}

    def get_source(self):
        """ Get Stream file descriptor. """
        return self.fd

    def next_source(self):
        """ Return None.

        As currenty we believe that there is only one input stream
        """
        return None