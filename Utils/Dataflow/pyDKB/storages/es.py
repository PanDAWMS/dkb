"""
pyDKB.storages.es
"""

from Storage import Storage
from . import storageType
from exceptions import (StorageException,
                        NotFound,
                        InvalidRequest)


try:
    import elasticsearch
    from elasticsearch.exceptions import (NotFoundError,
                                          RequestError)
except ImportError:
    pass


DEFAULT_CFG = {
    'host': '127.0.0.1',
    'port': '9200'
}


class ES(Storage):
    """ Representation of Elasticsearch storage. """

    # Default index
    index = None

    type = storageType.ES

    def __init__(self, name):
        """ Check if this class can be used and initialize object. """
        try:
            elasticsearch
        except NameError:
            raise StorageException("Required Python module not found:"
                                   " 'elasticsearch'")
        super(ES, self).__init__(name)

    def configure(self, cfg):
        """ Configure ES client.

        Configuration parameters:
          host   (str: '127.0.0.1')
          port   (str: '9200')
          index  (str)
          user   (str)
          passwd (str)

        :param cfg: configuration parameters
        :type cfg: dict
        """
        hosts = [{'host': cfg.get('host', DEFAULT_CFG['host']),
                 'port': cfg.get('port', DEFAULT_CFG['port'])}]
        kwargs = {}
        if cfg.get('user'):
            kwargs['http_auth'] = '%(user)s:%(passwd)s' % cfg
        if cfg.get('index'):
            self.index = cfg['index']
        self.c = elasticsearch.Elasticsearch(hosts, **kwargs)
