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
import re
from operator import attrgetter

from .exceptions import (InvalidCategoryName,
                        CategoryNotFound,
                        MethodNotFound,
                        MethodAlreadyExists,
                        DkbApiException,
                        DkbApiNotImplemented,
                        NotFoundException)
from .misc import standardize_path
import collections

# Hash representation of method categories structure.
# Keys are:
#  * special keys (see `KEYWORDS` below);
#  * (sub)category names;
#  * method names.
API_METHODS_CATALOG = {'__path': '/'}
KEYWORDS = ['__path']
WILDCARD = '*'

# Hash of method handlers: {'/path/to/method': method_handler}
API_METHOD_HANDLERS = {}

# NOTE: Wildcarded methods are falling into a sub-hash `__regex`:
# { re.compile('^/path/to/.*$'): method_handler,
#   re.compile('^/path/.*/to/method$'): method_handler }
API_METHOD_HANDLERS['__regex'] = {}


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
    keys = path.strip('/').split('/')
    keys = [k for k in keys if k]
    h = API_METHODS_CATALOG
    category = h
    key = ''
    for idx, key in enumerate(keys):
        if key in KEYWORDS:
            raise InvalidCategoryName(key, '/'.join((category.get('__path',
                                                                  ''),
                                                     key)))
        if WILDCARD in key:
            if analyze_wildcard:
                raise NotImplementedError("Wildcard categories lookup is not"
                                          " implemented yet")
        h = h.get(key, False)
        if not h or isinstance(h, collections.Callable):
            if not create:
                break
            category[key] = {}
            if isinstance(h, collections.Callable):
                category[key]['/'] = h
            h = category[key]
            h['__path'] = '/' + '/'.join(keys[:(idx + 1)])
        category = h
    if h is False or isinstance(h, collections.Callable):
        cat_name = category.get('__path', '')
        if cat_name == '/':
            cat_name = ''
        raise CategoryNotFound('/'.join((cat_name, key)))
    if analyze_wildcard:
        result = [category]
    else:
        result = category
    return result


def list_category(path):
    """ Get available methods and subcategories for given path.

    If category is not defined, raise ``CategoryNotFound`` exception.

    :param path: full path (starts with '/')  to category
    :type path: str

    :return: {'methods': [name, ...], 'categories': [...],
              'path': '/path/to/category/'}
    :rtype: dict
    """
    c = get_category(path)
    cat = {'path': c.get('__path'), 'methods': [], 'categories': []}
    for k in list(c.keys()):
        if k in KEYWORDS:
            continue
        if isinstance(c[k], collections.Callable):
            cat['methods'].append(k)
        if isinstance(c[k], dict):
            cat['categories'].append(k)
    return cat


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
    try:
        categories = get_category(category, create=True, analyze_wildcard=True)
    except NotImplementedError:
        categories = []
    exists_in = []
    added_to = []
    if not name:
        name = '/'
    for cat in categories:
        m = cat.get(name, None)
        if m and (isinstance(m, collections.Callable) or '/' in m):
            exists_in.append(cat['__path'])
            break
        else:
            added_to.append(cat['__path'])
            if not m:
                cat[name] = handler
            else:
                m['/'] = handler
    if name == WILDCARD:
        logging.warn("Invalid regex: '%(w)s' -- fixed to '.%(w)s'."
                     % {'w': WILDCARD})
        name = '.*'
    path = standardize_path('/'.join([category, name]))
    if WILDCARD in path:
        regex = '^' + path + '$'
        API_METHOD_HANDLERS['__regex'][re.compile(regex)] = handler
    else:
        API_METHOD_HANDLERS[path] = handler
    if exists_in:
        raise MethodAlreadyExists(name, exists_in)
    if added_to:
        logging.info("Method '%s' defined in categories: %s"
                     % (name, ', '.join(added_to)))
        return True
    return False


def handler(path, method=None):
    """ Get handler for given method.

    If method is not found, raise ``MethodNotFound`` exception.

    NOTE: if some handlers are defined for 'wildcard' paths, first found
          match will be used. Patterns are sorter alphanumerically in
          reverse mode in attempt to first check more specific patterns,
          but there's no guarantee that it will indeed be the "best
          match".

    :param path: full path to method or category (if second parameter
                 specified)
    :type path: str
    :param method: method name
    :type method: str

    :return: handler function
    :rtype: callable
    """
    try:
        handlers
    except NameError:
        logging.error('Handlers not configured.')
        raise MethodNotFound(path)

    full_path = path
    if method:
        full_path = '/'.join(full_path, method)
    full_path = standardize_path(full_path)

    h = API_METHOD_HANDLERS.get(full_path)
    if not h:
        # Try the wildcard paths handlers
        for p in sorted(list(API_METHOD_HANDLERS['__regex'].keys()),
                        key=attrgetter('pattern'), reverse=True):
            if p.match(full_path):
                h = API_METHOD_HANDLERS['__regex'][p]
                break
    if not h:
        raise MethodNotFound(full_path)
    return h


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
        response['status'] = err.code
        response['details'] = err.details
    elif isinstance(err, DkbApiNotImplemented):
        response['status'] = 501
    else:
        response['status'] = 500
        response['details'] = str(err)
    if isinstance(err, NotFoundException):
        response['text_info'] = NotFoundException.details
    trace = traceback.format_exception(*exc_info)
    for lines in trace:
        for line in lines.split('\n'):
            if line:
                logging.debug(line)
    return {}, response


def configure():
    """ Configure API methods.

    Raise exceptions: MethodAlreadyExists, DkbApiNotImplemented
    """
    global handlers
    from . import handlers
