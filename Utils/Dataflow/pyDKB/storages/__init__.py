"""
pyDKB.storages
"""

from ..common import Type

from Storage import Storage
from exceptions import StorageAlreadyExists


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
