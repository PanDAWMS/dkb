"""
Exception definitions for DKB API server.
"""

ADDR = '%%ADDR%%'


class DkbApiException(Exception):
    """ Base exception for DKB API module. """
    code = 500
    details = 'Unknown error'

    def __init__(self, message=None):
        if message:
            self.details = message
            super(DkbApiException, self).__init__()


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
    """ Exception indicating that category was not found. """
    code = 461

    def __init__(self, category):
        message = "Category not found: '%s'" % category
        self.details = message
        super(CategoryNotFound, self).__init__(message)


class InvalidCategoryName(CategoryException):
    """ Exception indicating that given name can't be a category name. """
    code = 462

    def __init__(self, name, category=None):
        cat = " ('%s')" % category if category else ''
        message = "Invalid (sub)category name: '%s'" % name
        message += cat
        self.details = message
        super(InvalidCategoryName, self).__init__(message)


class MethodException(DkbApiException):
    """ Base exception for method failures. """
    code = 470

    def __init__(self, method=None, reason=None):
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

    def __init__(self, path, method='', details=''):
        message = "Method not found: '%s" % path
        if method:
            message += "/%s" % method.strip('/')
        message += "'."
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


class MissedArgument(MethodException):
    """ Exception indicating that method required argument(s) are missed. """
    code = 473

    def __init__(self, method, *args):
        if not args:
            super(MissedArgument, self).__init__(method)
        else:
            args_str = "'" + "', '".join(args) + "'"
            reason = "required arguments are missed (%s)." % args_str
            super(MissedArgument, self).__init__(method, reason)


class InvalidArgument(MethodException):
    """ Exception indicating that passed argument has wrong value. """
    code = 474

    def __init__(self, method, *args):
        if not args:
            super(InvalidArgument, self).__init__(method)
        else:
            args_keyval = []
            for arg in args:
                if not isinstance(arg, (list, tuple)):
                    raise ValueError("InvalidArgument exception expects 'arg'"
                                     " to be list or tuple: (name, [value,"
                                     " [expected_value/type/class]]).")
                keyval = "%s" % arg[0]
                if len(arg) > 1:
                    keyval += "='%s'" % arg[1]
                if len(arg) > 2:
                    exp = arg[2]
                    if isinstance(exp, (list, tuple)):
                        keyval += " (expected one of: '%s')" \
                            % ("', '".join(exp))
                    else:
                        keyval += " (expected: '%s')" % exp
                args_keyval += [keyval]
            reason = "invalid argument value. Get: %s" \
                     % ('; '.join(args_keyval))
            super(InvalidArgument, self).__init__(method, reason)


class ConfigurationError(DkbApiException):
    """ Base exception for server configuration errors. """
    code = 590

    def __init__(self, reason=None):
        message = "Configuration failure"
        if reason:
            message += ": %s" % reason
        self.details = message
        super(ConfigurationError, self).__init__(message)


class ConfigurationNotFound(ConfigurationError, NotFoundException):
    """ Exception indicating that configuration file is not found. """
    code = 591

    def __init__(self, path):
        reason = "file not found (%s)" % path
        super(ConfigurationNotFound, self).__init__(reason)
