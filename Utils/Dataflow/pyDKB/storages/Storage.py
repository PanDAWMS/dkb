"""
pyDKB.storages.Storage
"""

import sys
from datetime import datetime

from exceptions import (StorageNotConfigured,
                        QueryError)


class Storage(object):
    """ Interface class for external and internal DKB storages. """

    # Storage name (identifier)
    name = None

    # Storage type (storageType member)
    type = None

    # Storage client
    c = None

    # Stored queries
    stored_queries = {}

    def __init__(self, name):
        """ Initialize Storage object.

        Raise ``StorageException`` in case of error.

        :param name: storage identifier
        :type name: str
        """
        self.name = name

    def log(self, level, message):
        """ Output log message. """
        if level not in ('TRACE', 'DEBUG', 'INFO', 'WARN', 'WARNING', 'ERROR',
                         'CRITICAL'):
            level = 'INFO'
        dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sys.stderr.write('(%s) %s (%s) %s\n' % (level, dt,
                                                self.__class__.__name__,
                                                message))

    def log_cfg(self, cfg):
        """ Log storage configuration.

        :param cfg: configuration to be logged
        :type cfg: dict
        """
        fname = ''
        if cfg.get('__file'):
            fname = ' (%s)' % cfg['__file']
        self.log("INFO", "'%s' storage configuration%s:" % (self.name, fname))
        key_len = len(max(cfg.keys(), key=len))
        pattern = "%%-%ds : '%%s'" % key_len
        self.log("INFO", "---")
        for key in cfg:
            if key.startswith('__'):
                continue
            self.log("INFO", pattern % (key, cfg[key]))
        self.log("INFO", "---")

    def configure(self, cfg):
        """ Apply storage configuration (initialize client).

        :param cfg: configuration parameters
        :type cfg: dict
        """
        raise NotImplementedError

    def client(self):
        """ Get storage client.

         Raise ``StorageNotConfigured`` if called before configuration.

        :return: client object, corresponding given storage type.
        :rtype: object
        """
        if not self.c:
            raise StorageNotConfigured(self.name)
        return self.c

    def get(self, id, **kwargs):
        """ Get object / record from storage by ID.

        Raise ``NotFound`` exception if object / record not found.

        :param id: object / record identfier
        :type id: str, int

        :return: record with given ID
        :rtype: dict
        """
        raise NotImplementedError

    def read_query(self, fname, qname=None):
        """ Read query from file and save it.

        :param fname: file name
        :type fname: str
        :param qname: query name (for futher usage)
        :type qname: str
        """
        raise NotImplementedError

    def query_is_raw(self, query):
        """ Check if given query is not compiled ("raw").

        :param query: query body
        :type query: obj

        :return: True/False
        :rtype: bool
        """
        raise NotImplementedError

    def save_query(self, query, qname=None, raw=False):
        """ Save query for further usage.

        :param query: query content
        :type query: object
        :param qname: query name (must not start with '__')
        :type qname: str
        :param raw: store "raw" (not compiled) version of query
        :type raw: bool
        """
        if qname and qname.startswith('__'):
            raise ValueError("Query name must not start with '__'"
                             " (reserved for service needs).")
        if not raw:
            try:
                raw = self.query_is_raw(query)
            except NotImplementedError:
                pass
        prefix = ''
        if not qname:
            qname = '__last'
        if raw:
            prefix = '__raw'
        self.stored_queries[prefix + qname] = query
        self.stored_queries[prefix + '__last'] = query

    def get_query(self, qname):
        """ Get query by name.

        Raise ``QueryError`` if query not found.

        :param qname: query name (if None, last stored/used query will be used)
        :type qnmae: str

        :return: stored query
        :rtype: object
        """
        if not qname:
            qname = '__last'
        try:
            q = self.stored_queries[qname]
            self.stored_queries['__last'] = q
        except KeyError:
            # There still may be raw version of the query
            try:
                q = self.stored_queries['__raw' + qname]
            except KeyError:
                raise QueryError("Query used before saving: '%s'"
                                 % qname)
        self.stored_queries['__last'] = q
        return q

    def exec_query(self, qname=None, **kwargs):
        """ Execute stored query with given parameters.

        :param qname: query name (if None, last used/read
                      one will be used)
        :type qname: str, NoneType
        :param kwargs: query parameters (applied with old-style
                       string formatting operator '%')
        :type kwargs: dict

        :return: storage response
        :rtype: object
        """
        raise NotImplementedError

    def execute(self, query, **kwargs):
        """ Execute query with given parameters.

        :param query: query content
        :type query: object
        :param kwargs: query parameters (applied with old-style
                       string formatting operator '%')
        :type kwargs: dict

        :return: storage response
        :rtype: object
        """
        self.save_query(query)
        return self.exec_query(**kwargs)
