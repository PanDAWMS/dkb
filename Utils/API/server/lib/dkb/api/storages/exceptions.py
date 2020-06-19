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


class NoDataFound(StorageException):
    """ Exception indicating that requested data were not found. """
    code = 552

    def __init__(self, storage, message=None):
        reason = "No data found in %s" % storage
        if message:
            reason += ": %s" % message
        self.details = reason


class QueryException(StorageException):
    """ Exception indicating any failure in query preparation. """
    code = 560

    def __init__(self, details="Query failed"):
        self.details = details
        super(QueryException, self).__init__(details)


class QueryNotFound(QueryException):
    """ Exception indicating that query was not found. """
    code = 561

    def __init__(self, qname, fname=None):
        message = "%s: file not found" % qname
        if fname:
            message += " (%s)" % fname
        self.details = message
        super(QueryNotFound, self).__init__(message)


class MissedParameter(QueryException):
    """ One or mode query parameters are missed. """
    code = 562

    def __init__(self, qname, param=None):
        message = 'Missed parameters'
        if param:
            if isinstance(param, list):
                p = ', '.join(param)
            else:
                p = param
            message += ": %s" % p
        message += " (%s)" % qname
        self.details = message
        super(MissedParameter, self).__init__(message)
