"""
pyDKB.dataflow.communication.producer.StreamProducer

Data producer implementation for a single stream.

TODO: think about multiple streams (like a number of named
      pipes, etc). Prehaps, even merge this class with FileProducer.
"""

import sys

from .Producer import Producer


class StreamProducer(Producer):
    """ Data producer implementation for Stream data dest. """

    fd = None

    # Override
    def reconfigure(self, config={}):
        """ (Re)configure Stream producer. """
        self.fd = sys.stdout
        super(StreamProducer, self).reconfigure(config)

    def get_dest_info(self):
        """ Return current dest info. """
        return {'fd': self.fd}

    def get_dest(self):
        """ Get Stream file descriptor. """
        return self.fd
