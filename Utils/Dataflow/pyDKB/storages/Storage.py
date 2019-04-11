"""
pyDKB.storages.Storage
"""

from . import storageType


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

        Raise ``StorageAlreadyExists`` if storage with given name
        was already created.

        :param name: storage identifier
        :type name: str
        """
        raise NotImplementedError

    def configure(self, cfg):
        """ Apply storage configuration (initialize client).

        :param cfg: configuration parameters
        :type cfg: dict
        """
        raise NotImplementedError

    def get(self, id, **kwargs):
        """ Get object / record from storage by ID.

        Raise ``NotFound`` exception if object / record not found.

        :param id: object / record identfier
        :type id: str, int

        :return: record with given ID
        :rtype: dict
        """
        raise NotImplementedError
