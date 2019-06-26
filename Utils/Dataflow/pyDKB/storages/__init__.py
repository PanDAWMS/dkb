"""
pyDKB.storages
"""

import importlib


import exceptions
import client


_scope = None


def setScope(scope):
    """ Set default scope to look for storages.

    :param scope: scope name
    :type scope: str
    """
    global _scope
    _scope = getScope(scope)


def getScope(scope):
    """ Initialize storages scope for further usage.

    :param scope: scope name
    :type scope: str
    """
    try:
        full_name = __name__ + "." + scope
        scope = importlib.import_module(full_name)
    except ImportError:
        raise exceptions.StorageException("Scope not defined: '%s'" % scope)
    return scope


def getClient(name, scope=None):
    """ Get client for a given storage.

    Raise ``StorageException`` if failed to get client by given name and scope.

    :param name: storage name
    :type name: str
    :param scope: scope name. If not specified, default value set with
                  `setScope()` is used
    :type scope: str, NoneType

    :return: storage client
    :rtype: client.Client
    """
    if scope:
        scope = getScope(scope)
    else:
        scope = _scope
    if scope is None:
        raise exceptions.StorageException("Storages scope not specified")
    cur_scope = scope
    for n in name.split('.'):
        try:
            new_scope = getattr(cur_scope, n, None)
            if new_scope is None:
                new_scope_name = cur_scope.__name__ + "." + n
                new_scope = importlib.import_module(new_scope_name)
            cur_scope = new_scope
        except ImportError:
            raise exceptions.StorageException("Storage not defined in scope "
                                              "'%s': '%s'"
                                              % (scope.__name__.split('.')[-1],
                                                 name))
    client = cur_scope.getClient()
    return client
