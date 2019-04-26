"""
pyDKB.storages
"""

from types import storageType
from Storage import Storage
from es import ES
from exceptions import (StorageAlreadyExists,
                        StorageNotConfigured)


storageClass = {storageType.ES: ES}
storages = {}


def create(name, stype, cfg=None):
    """ Create storage of given type.

    Raise ``StorageAlreadyExists`` if storage with given name was created
    earlier.

    :param name: storage identifier
    :type name: str
    :param stype: storage type
    :type stype: storageType member
    :param cfg: storage configuration (if None, won't be applied)
    :type cfg: dict, NoneType

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
    if cfg is not None:
        storages[name].configure(cfg)
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
