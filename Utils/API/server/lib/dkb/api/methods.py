"""
Service functions for DKB API methods.

Every method belongs to some 'category' of methods.
E.g. there can be category '/task' for all methods concerning tasks;
and method 'chain' in this category: '/task/chain'.
Base category ('/') is for server methods, like 'servier info' or 'status'.

To add new method:
```

import methods

def my_method_handler(path, **kwargs):
    \""" My method description.

    ...
    \"""
    <method code>


methods.add('/path/to/category', 'method_name', my_method_handler)
```
"""

import traceback
import logging

from exceptions import (CategoryNotFound,
                        MethodNotFound,
                        DkbApiException,
                        DkbApiNotImplemented,
                        NotFoundException)

# Hash of categories and methods
API_METHODS = {}
WILDCARD = '*'


def get_category(path, create=False, analyze_wildcard=False):
    """ Get category definition.

    If category is not defined and ``create`` is False, raise
    ``CategoryNotFound`` exception.

    If category name contains some keywords (service words), raise
    ``InvalidCategoryName`` exception.

    :param path: full path to a category
    :type path: str
    :param create: if ``True``, create category if missed.
    :type create: bool
    :param analyze_wildcard: if ``True``, '*' in category path will
                             be taken as 'any heir'
    :type analyze_wildcard: bool

    :return: hash or (in case of wildcard) list of hashes with category
             methods and subcategories
    :rtype: dict, list(dict)
    """
    raise DkbApiNotImplemented


def list_category(path):
    """ Get available methods and subcategories for given path.

    If category is not defined, raise ``CategoryNotFound`` exception.

    :param path: full path (starts with '/')  to method or category
    :type path: str

    :return: {'methods': {<name>: <callable>}, 'categories': [...],
              'path': '/path/to/category/'}
    :rtype: dict
    """
    raise DkbApiNotImplemented


def add(category, name, handler):
    """ Add method ``name`` with given ``handler`` to ``category``.

    If category does not exist, create category.
    If method already exists, raise ``MethodAlreadyExists`` exception.

    :param category: full path to a category ('/*' can be used for 'all
                     (sub)categories'.
    :type category: str
    :param name: method name (if None or empty string, method becomes
                'root' method of the category)
    :type name: str, None
    :param handler: method handler function
    :type handler: callable

    :return: True on success, False on failure
    :rtype: bool
    """
    raise DkbApiNotImplemented


def handler(path, method=None):
    """ Get handler for given method.

    If method is not found, raise ``MethodNotFound`` exception.

    :param path: full path to method or category (if second parameter
                 specified)
    :type path: str
    :param method: method name
    :type method: str

    :return: method handler function
    :rtype: callable
    """
    raise DkbApiNotImplemented


def error_handler(exc_info):
    """ Generate response with error info.

    :param err: error details
    :type err: Exception
    """
    err = exc_info[1]
    response = {
        'exception': err.__class__.__name__,
    }
    if isinstance(err, DkbApiException):
        response['_status'] = err.code
        response['details'] = err.details
    elif isinstance(err, DkbApiNotImplemented):
        response['_status'] = 501
    if isinstance(err, NotFoundException):
        response['text_info'] = NotFoundException.description
    trace = traceback.format_exception(*exc_info)
    for lines in trace:
        for line in lines.split('\n'):
            if line:
                logging.debug(line)
    return response


def configure():
    """ Configure API methods.

    Raise exceptions: MethodAlreadyExists, DkbApiNotImplemented
    """
    global handlers
    import handlers
