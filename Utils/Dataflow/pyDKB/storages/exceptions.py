"""
pyDKB.storages.exceptions
"""


class StorageException(Exception):
    """ Base exception for all storage-related exceptions. """
    pass


class NotFound(StorageException):
    """ Exception indicating that record with given ID is not found. """

    def __init__(self, **kwargs):
        """ Initialize exception.

        :param kwargs: record primary key parameters
        :type kwargs: dict
        """
        message = "Record not found"
        if kwargs:
            params = [': '.join((key, '%r' % kwargs[key])) for key in kwargs]
            params = ', '.join(params)
            message = message + ' (%s)' % params
        super(NotFound, self).__init__(message)
