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


class InvalidRequest(StorageException):
    """ Exception indicating wrong user request. """

    def __init__(self, message, *args, **kwargs):
        """ Initialize exception.

        Message formatting: old-style ('%' operator) only.

        :param message: error message
        :type message: str
        :param args: message format positional parameters
        :type args: list
        :param kwargs: message format named parameters
        :type kwargs: dict
        """
        if args and kwargs:
            raise ValueError("Message formatting supports only one type "
                             "of parameters: positional OR named.")
        if args:
            message = message % params
        elif kwargs:
            message = message % kwargs
        super(InvalidRequest, self).__init__(message)
