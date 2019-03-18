"""
Exception definitions for DKB API server.
"""

from ..exceptions import DkbApiException


class StorageException(DkbApiException):
    """ Base exception for storage failures. """
    code = 550
    details = "Failed to get data from DKB storage."


class StorageClientException(StorageException):
    """ Exception indicating failure when creating storage client. """
    code = 551

    def __init__(self, storage, reason=None):
        message = "Failed to initialize storage client for '%s'." % storage
        if reason:
            message += " Reason: %s" % reason
        self.details = message
        super(StorageClientException, self).__init__(message)
