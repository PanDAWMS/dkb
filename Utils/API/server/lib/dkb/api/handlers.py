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
from exceptions import (DkbApiNotImplemented,
                        MethodException,
                        MissedArgument,
                        InvalidArgument
                        )
from . import __version__
import storages

from cStringIO import StringIO

try:
    import matplotlib
    matplotlib.rcParams['backend'] = 'agg'
    from matplotlib import pyplot
except Exception:
    pass


# =================
# Standard handlers
# =================


def info(path, **kwargs):
    """ Information about available methods and (sub)categories. """
    cat = path.rstrip('/')[:-len('info')]
    response = methods.list_category(cat)
    return response


methods.add('/', 'info', info)


def server_info(path, **kwargs):
    """ Server info. """
    response = {"name": "DKB API server",
                "version": __version__}
    return response


methods.add('/', None, server_info)
methods.add('/', 'server_info', server_info)


# ===================
# API method handlers
# ===================


def task_hist(path, **kwargs):
    """ Generate histogram with task steps distribution over time.

    :param path: full path to the method
    :type path: str
    """
    rtype = kwargs.get('rtype', 'img')
    if rtype == 'img':
        try:
            pyplot
        except NameError:
            raise MethodException("Module 'matplotlib' ('pyplot') is not "
                                  "installed")
    htags = kwargs.get('htags')
    if htags is None:
        raise MissedArgument('/task/hist', 'htags')
    if not isinstance(htags, (list, str)):
        raise InvalidArgument('/task/hist', ('htags', htags))
    if not isinstance(htags, list):
        kwargs['htags'] = htags.split(',')
    data = storages.task_steps_hist(**kwargs)
    result = {}
    if rtype == 'json':
        # json module doesn't know how to serialize `datetime` objects
        x_data = data['data']['x']
        for i, _ in enumerate(x_data):
            for j, d in enumerate(x_data[i]):
                x_data[i][j] = str(d)
        result = data
    if rtype == 'img':
        pyplot.figure(figsize=(20, 15))
        pyplot.hist(data['data']['x'], weights=data['data']['y'],
                    stacked=True, bins=250)
        pyplot.legend(data['legend'])
        img = StringIO()
        pyplot.savefig(img)
        img.seek(0)
        result['img'] = img.read()
    return result


methods.add('/task', 'hist', task_hist)


def task_chain(path, **kwargs):
    """ Get list of tasks belonging to same chain as ``tid``.

    :param path: full path to the method
    :type path: str
    :param tid: task id
    :type tid: str, int

    :return: list of Task IDs, ordered from first to last task in chain
    :rtype: dict
    """
    method_name = '/task/chain'
    tid = kwargs.get('tid', None)
    if tid is None:
        raise MissedArgument(method_name, 'tid')
    try:
        int(tid)
    except ValueError:
        raise InvalidArgument(method_name, ('tid', tid, int))
    return storages.task_chain(**kwargs)


methods.add('/task', 'chain', task_chain)
