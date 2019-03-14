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


class InvalidCategoryName(CategoryException):
    """ Exception indicating that given name can't be a category name. """
    code = 462

    def __init__(self, name, category = None):
        cat = " ('%s')" % category if category else ''
        message = "Invalid (sub)category name: '%s'" % name
        message += cat
        self.details = message
        super(InvalidCategoryName, self).__init__(message)


class MethodException(DkbApiException):
    """ Base exception for method failures. """
    code = 470

    def __init__(self, method, reason=None):
        message = "Method failed"
        if method:
            message += ": '%s'" % method
        if reason:
            message += ". Reason: %s" % reason
        self.details = message
        super(MethodException, self).__init__(message)


class MethodNotFound(MethodException):
    """ Exception indicating that called method is not found. """
    code = 471

    def __init__(self, method, category='', details=''):
        message = "Method not found: '%s'" % method
        if category:
            message += " (category: '%s')" % category
        message += '.'
        if details:
            message += "\nDetails: %s" % details
        self.details = message
        super(MethodNotFound, self).__init__(message)


class MethodAlreadyExists(MethodException):
    """ Exception indicating that method being created already exists. """
    code = 472

    def __init__(self, method, category):
        if type(category) == list:
             category = "categories (%s)" % category
        else:
             category = "category ('%s')" % category
        message = "Method '%s' already exists in %s" \
                  % (method, category)
        self.details = message
        super(MethodAlreadyExists, self).__init__(message)
