"""
pyDKB.storages.Storage
"""

import sys
from datetime import datetime

from exceptions import StorageNotConfigured


class Storage(object):
    """ Interface class for external and internal DKB storages. """

    # Storage name (identifier)
    name = None

    # Storage type (storageType member)
    type = None

    # Storage client
    c = None

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

    def save_query(self, query, qname=None):
        """ Save query for further usage.

        :param query: query content
        :type query: object
        :param qname: query name (must not start with '__')
        :type qname: str
        """
        raise NotImplementedError

    def get_query(self, qname):
        """ Get query by name.

        :param qname: query name (if None, last stored/used query will be used)
        :type qnmae: str

        :return: stored query
        :rtype: object
        """
        raise NotImplementedError

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
