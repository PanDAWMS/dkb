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

    def get(self, id, fields=None, index=None, doc_type='_all', parent=None):
        """ Get record from ES.

        Raise:
         * ``NotFound`` exception if record is not found
         * ``InvalidRequest`` if request can not be executed
         * ``StorageNotConfigured`` if called before `configure()`

        Supported document types for ATLAS: 'task', 'output_dataset'.

        :param fields: specific set of fields to get (if not specified, all
                       fields will be returned)
        :type fields: list, NoneType
        :param doc_type: document type
        :type doc_type: str
        :param parent: parent document ID (required for child documents)
        :type parent: str

        :return: ES record with specified or full set of fields
        :rtype: dict
        """
        c = self.client()
        if not index:
            index = self.index
        kwargs = {'index': index, 'doc_type': doc_type, 'id': id}
        if not kwargs['index']:
            raise InvalidRequest("Index not specified.")
        if fields is not None:
            kwargs['_source'] = fields
        if parent:
            kwargs['parent'] = parent
        try:
            r = c.get(**kwargs)
        except NotFoundError, err:
            raise NotFound(self.name, id=id, index=index)
        except RequestError, err:
            if doc_type == 'output_dataset' \
                    and err.args[1] == 'routing_missing_exception':
                self.log('WARN', 'Parent info missed.')
                raise NotFound(self.name, id=id, index=index)
            raise InvalidRequest(err)
        return r.get('_source', {})
