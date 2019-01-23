"""
Consumer submodule init file.
"""

from .. import messageType
from .. import logLevel
from .. import DataflowException

from Consumer import Consumer
from FileConsumer import FileConsumer
from HDFSConsumer import HDFSConsumer
from StreamConsumer import StreamConsumer

from Consumer import ConsumerException

__all__ = ['ConsumerBuilder']


class ConsumerBuilder(object):
    """ Constructor for Consumer instance. """

    consumerClass = None

    def __init__(self, config={}):
        """ Constructor initialization. """
        if not isinstance(config, dict):
            raise TypeError("ConsumerBuilder expects argument of type 'dict'"
                            " (got '%s')" % config.__class__.__name__)
        self.config = config

        if config.get('hdfs'):
            self.setSource('h')
        elif config.get('mode') in ('s', 'm'):
            self.setSource('s')
        else:
            self.setSource(config.get('source'))

    def setSource(self, source):
        """ Set data source for the consumer. """
        sources = {
            'h': HDFSConsumer,
            's': StreamConsumer,
            'f': FileConsumer
        }
        if source not in sources:
            raise ValueError("ConsumerBuilder.setSource() expects one of the"
                             " following values: %s (got '%s')"
                             % (sources.keys(), source))

        self.consumerClass = sources[source]
        return self

    def setType(self, Type):
        """ Set message type for the consumer. """
        if Type is not None and not messageType.hasMember(Type):
            raise ValueError("Unknown message type: %s" % Type)
        self.message_type = Type
        return self

    def build(self, config={}):
        """ Return constructed consumer. """
        if not config:
            config = self.config
        instance = self.consumerClass(config)
        if self.message_type:
            instance.set_message_type(self.message_type)
        return instance
