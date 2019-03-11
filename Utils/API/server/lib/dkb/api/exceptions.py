"""
Exception definitions for DKB API server.
"""

ADDR = '%%ADDR%%'


class DkbApiException(Exception):
    """ Base exception for DKB API module. """
    code = 500
    details = 'Unknown error'


class DkbApiNotImplemented(DkbApiException, NotImplementedError):
    """ Exception for not implemented functional. """
    code = 501
    details = "What you are looking for is not implemented yet."


class NotFoundException(DkbApiException):
    """ Base exception for "smth not found" errors. """
    code = 400
    details = "For list of available methods and (sub)categories:" \
              " %s/<category>/info" % ADDR


class CategoryException(DkbApiException):
    """ Base exception for category failures. """
    code = 460

    def __init__(self, category, reason=None):
        message = "Category failure"
        if category:
            message += ": '%s'" % category
        if reason:
            message += ". Reason: %s" % reason
        self.details = message
        super(CategoryException, self).__init__(message)


class CategoryNotFound(CategoryException):
    """ Exception indicating that category not found. """
    code = 461

    def __init__(self, category):
        message = "Category not found: '%s'" % category
        self.details = message
        super(CategoryNotFound, self).__init__(message)


class MethodNotFound(DkbApiException):
    """ Exception indicating that called method is not found. """
    code = 462

    def __init__(self, method, category='', details=''):
        message = "Method not found: '%s'" % method
        if category:
            message += " (category: '%s')" % category
        message += '.'
        if details:
            message += "\nDetails: %s" % details
        self.details = message
        super(MethodNotFound, self).__init__(message)


class MethodAlreadyExists(DkbApiException):
    """ Exception indicating that method being created already exists. """

    def __init__(self, method, category, handler):
        message = "Method '%s' in category '%s' already exists" \
                  " (handler function: %s)" % (method, category, handler)
        self.details = message
        super(MethodAlreadyExists, self).__init__(message)
