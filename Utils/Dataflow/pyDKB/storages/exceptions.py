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


class StorageNotConfigured(StorageException):
    """ Exception indicating that requested storage is not configured. """

    def __init__(self, name):
        """ Initialize exception.

        :param name: storage name
        :type name: str
        """
        message = "Storage '%s' used before configuration." % name
        super(StorageNotConfigured, self).__init__(message)


class NotFound(StorageException):
    """ Exeption indicating that record with given ID  not found. """

    def __init__(self, name, **kwargs):
        """ Initialize exception.

        :param name: storage name
        :type name: str
        :param kwargs: record parameters
        :type kwargs: dict
        """
        message = "Record not found in '%s'" % (name)
        if kwargs:
            params = [': '.join((key, '%r' % kwargs[key])) for key in kwargs]
            params = ', '.join(params)
            message = message + ' (%s)' % params
        super(NotFound, self).__init__(message)
