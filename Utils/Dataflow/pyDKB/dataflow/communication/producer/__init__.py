"""
Producer submodule init file.
"""

from .. import messageType
from .. import DataflowException

from Producer import Producer
from FileProducer import FileProducer
from HDFSProducer import HDFSProducer
from StreamProducer import StreamProducer

from Producer import ProducerException

__all__ = ['ProducerBuilder']


class ProducerBuilder(object):
    """ Constructor for Producer instance. """

    producerClass = None
    message_type = None
    src_info = None

    def __init__(self, config={}):
        """ Constructor initialization. """
        if not isinstance(config, dict):
            raise TypeError("ProducerBuilder expects argument of type 'dict'"
                            " (got '%s')" % config.__class__.__name__)
        self.config = config

        if config.get('hdfs'):
            self.setDest('h')
        elif config.get('dest'):
            self.setDest(config.get('dest'))
        elif config.get('mode') in ('s', 'm'):
            self.setDest('s')
        else:
            self.setDest('f')

    def setDest(self, dest):
        """ Set data destination for the producer. """
        dests = {
            'h': HDFSProducer,
            's': StreamProducer,
            'f': FileProducer
        }
        if dest not in dests:
            raise ValueError("ProducerBuilder.setDest() expects one of the"
                             " following values: %s (got '%s')"
                             % (dests.keys(), dest))

        self.producerClass = dests[dest]
        self.config['dest'] = dest
        return self

    def setType(self, Type):
        """ Set message type for the producer. """
        if not (Type is None or messageType.hasMember(Type)):
            raise ValueError("Unknown message type: %s" % Type)
        self.message_type = Type
        return self

    def setSourceInfoMethod(self, src_info):
        """ Set method to get current source info. """
        if not callable(src_info):
            raise TypeError("setSourceInfoMethod() expects callable object.")
        self.src_info = src_info
        return self

    def build(self, config={}):
        """ Return constructed producer. """
        if not config:
            config = self.config
        instance = self.producerClass(config)
        if self.message_type:
            instance.set_message_type(self.message_type)
        if self.src_info and getattr(instance, 'get_source_info', False):
            instance._source_info = self.src_info
        return instance
