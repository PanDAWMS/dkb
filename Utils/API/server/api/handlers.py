"""
Method implementations for DKB API.

All method handlers should be defined as follows:
```
def my_method_handler(path, **kwargs):
    \""" Method description (will be used by ``info`` method).

    Detailed description.

    :param path: request path
    :type path: str

    :return: hash with method response.
             Special field ``'_status'`` can be used to specify return code.
    :rtype: dict
    \"""
    <method implementation>
```
"""

import methods
from exceptions import DkbApiNotImplemented


# =================
# Standard handlers
# =================


def info(path, **kwargs):
    """ Information about available methods and (sub)categories. """
    raise DkbApiNotImplemented


try:
    methods.add('/*', 'info', info)
except DkbApiNotImplemented:
    pass


def server_info(path, **kwargs):
    """ Server info. """
    raise DkbApiNotImplemented


try:
    methods.add('/', None, server_info)
except DkbApiNotImplemented:
    pass
