"""
pyDKB.storages
"""

from ..common import Type

from Storage import Storage
from exceptions import (StorageAlreadyExists,
                        StorageNotConfigured)


storageType = Type()
storages = {}


def create(name, stype):
    """ Create storage of given type.

    Raise ``StorageAlreadyExists`` if storage with given name was created
    earlier.

    :param name: storage identifier
    :type name: str
    :param stype: storage type
    :type stype: storageType member

    :return: Storage object
    :rtype: Storage
    """
    global storages
    if name in storages:
        raise StorageAlreadyExists(name)
    storages[name] = Storage(name)
    return storages[name]


def get(name):
    """ Get storage client by name.

    Raise ``StorageNotConfigured`` if the storage was not `create()`d earlier.

    :param name: storage name
    :type name: str

    :return: object representing given storage
    :rtype: Storage
    """
    if name not in storages:
        raise StorageNotConfigured(name)
    return storages[name]
