"""
Exception definitions for DKB API server.
"""

import inspect

from . import ADDR


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


class CategoryNotFound(DkbApiException):
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


class MethodException(DkbApiException):
    """ Base exception for method failures. """
    code = 570

    def __init__(self, method, reason=None):
        message = "Method failed"
        if method:
            message += ": '%s'" % method
        if reason:
            message += ". Reason: %s" % reason
        self.details = message
        super(MethodException, self).__init__(message)


class MissedArgument(MethodException):
    """ Exception indicating that method required argument(s) are missed. """
    code = 471

    def __init__(self, method, *args):
        if not args:
            super(MissedArgument, self).__init__(method)
        else:
            args_str = "'" + "', '".join(args) + "'"
            reason = "required arguments are missed (%s)." % args_str
            super(MissedArgument, self).__init__(method, reason)


class InvalidArgument(MethodException):
    """ Exception indicating that passed argument has wrong value. """
    code = 472

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
                        keyval += "(expected one of: '%s')" \
                            % ("', '".join(exp))
                    else:
                        keyval += "(expected: '%s')" % exp
                args_keyval += [keyval]
            reason = "invalid argument value. Get: %s" \
                     % ('; '.join(args_keyval))
            super(InvalidArgument, self).__init__(method, reason)
