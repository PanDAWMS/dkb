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

import logging

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


# ===================
# API method handlers
# ===================

def task_chain(path, **kwargs):
    """ Get list of tasks belonging to same chain as ``tid``.

    :param path: full path to the method
    :type path: str
    :param tid: task id
    :type tid: str, int

    :return: list of Task IDs, ordered from first to last task in chain
    :rtype: dict
    """
    logging.debug("'/task/chain' handler called.")
    raise DkbApiNotImplemented


try:
    methods.add('/task', 'chain', task_chain)
except DkbApiNotImplemented:
    pass
