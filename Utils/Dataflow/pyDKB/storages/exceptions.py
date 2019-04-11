"""
pyDKB.storages.exceptions
"""


class StorageException(Exception):
    """ Base exception for all storage-related exceptions. """
    pass


class StorageAlreadyExists(StorageException):
    """ Exception indicating that storage name was already used. """

    def __init__(self, name):
        """ Initialize exception.

        :param name: storage name
        :type name: str
        """
        message = "Name already in use: '%s'" % name
        super(StorageAlreadyExists, self).__init__(message)


class NotFound(StorageException):
    """ Exeption indicating that record with given ID  not found. """

    def __init__(self, name, id):
        """ Initialize exception.

        :param name: storage name
        :type name: str
        :param id: record ID
        :type id: str, int
        """
        message = "Record not found in '%s' (id: '%s')" % (name, id)
        super(NotFound, self).__init__(message)
