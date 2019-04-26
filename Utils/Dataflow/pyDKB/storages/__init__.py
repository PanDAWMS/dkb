"""
pyDKB.storages
"""

from ..common import Type

from Storage import Storage
from exceptions import (StorageAlreadyExists,
                        StorageNotConfigured)


storageType = Type()
storageClass = {}
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
    cls = storageClass.get(stype)
    if not cls:
        sname = storageType.memberName(stype)
        if not sname:
            raise ValueError("Unknown storage type: '%s'" % stype)
        raise NotImplementedError("Storage class is not implemented for: '%s'"
                                  % storageType.memberName(stype))
    storages[name] = cls(name)
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
