"""
pyDKB.storages.es
"""

import json

from Storage import Storage
from . import storageType
from exceptions import (StorageException,
                        NotFound,
                        InvalidRequest,
                        MissedParameter)


try:
    import elasticsearch
    from elasticsearch.exceptions import (NotFoundError,
                                          RequestError)
except ImportError:
    pass


DEFAULT_CFG = {
    'host': '127.0.0.1',
    'port': '9200',
    'timeout_retry': 3
}


class ES(Storage):
    """ Representation of Elasticsearch storage. """

    # Default index
    index = None

    # Default datetime format
    datetime_fmt = '%d-%m-%Y %H:%M:%S'

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
          host          (str: '127.0.0.1')
          port          (str: '9200')
          index         (str)
          user          (str)
          __passwd      (str)
          timeout_retry (int: 3)

        :param cfg: configuration parameters
        :type cfg: dict
        """
        self.log_cfg(cfg, DEFAULT_CFG)
        hosts = [{'host': cfg.get('host', DEFAULT_CFG['host']),
                 'port': cfg.get('port', DEFAULT_CFG['port'])}]
        kwargs = {}
        if cfg.get('user'):
            kwargs['http_auth'] = '%(user)s:%(__passwd)s' % cfg
        if cfg.get('index'):
            self.index = cfg['index']
        kwargs['retry_on_timeout'] = True
        kwargs['max_retries'] = cfg.get('timeout_retry',
                                        DEFAULT_CFG['timeout_retry'])
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

    def read_query(self, fname, qname=None):
        """ Read query from file and save it.

        Raise ``QueryNotFound`` in case of failure.

        :param fname: file name
        :type fname: str
        :param qname: query name (for futher usage)
        :type qname: str
        """
        raw = False
        try:
            with open(fname, 'r') as f:
                query = f.read()
            query = json.loads(query)
        except IOError:
            raise QueryNotFound(qname, fname)
        except ValueError:
            # Query with parameters may fail when try to parse as JSON
            # In this case we just store it as "raw" version
            raw = True
        self.save_query(query, qname, raw)

    def query_is_raw(self, query):
        """ Check if given query is not compiled ("raw").

        :param query: query body
        :type query: str, dict

        :return: True/False
        :rtype: bool
        """
        return not isinstance(query, dict)

    def exec_query(self, qname=None, **kwargs):
        """ Execute stored query with given parameters.

        :param qname: query name (if None, last used/read
                      one will be used)
        :type qname: str, NoneType
        :param kwargs: query parameters (applied with old-style
                       string formatting operator '%'). Parameter
                       name, started with '_', is treated as special
                       one:
                        * _size  -- for ES request "size";
                        * _type  -- for ES request "doc_type";
                        * _index -- for ES index to use.
        :type kwargs: dict

        :return: storage response
        :rtype: object
        """
        query = self.get_query(qname)
        raw = self.query_is_raw(query)
        params = {}
        for key in kwargs:
            if key.startswith('_'):
                continue
            try:
                params[key] = json.dumps(kwargs[key])
            except TypeError, err:
                if 'datetime' in str(err):
                    val = json.dumps(kwargs[key].strftime(self.datetime_fmt))
                    params[key] = val
                else:
                    raise
        q = {}
        q['index'] = kwargs.get('_index', self.index)
        q['size'] = kwargs.get('_size')
        q['doc_type'] = kwargs.get('_type')
        if params:
            try:
                if not raw:
                    query = json.dumps(query)
                    raw = True
                query = query % params
            except KeyError, err:
                raise MissedParameter(qname, str(err))
        if raw:
            try:
                query = json.loads(query)
            except ValueError, err:
                msg = "Failed to parse query"
                if qname:
                    msg += " (%r)" % qname
                msg += ": %s" % err
                raise QueryError(msg)
        q['body'] = query
        try:
            r = self.client().search(**q)
        except RequestError, err:
            msg = "Query failed"
            if qname:
                msg += ": (%r)" % qname
            msg += ": %s" % err
            raise QueryError(msg)
        return r
