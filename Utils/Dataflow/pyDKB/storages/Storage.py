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
