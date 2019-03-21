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
    :param detailed: keep all "* Merge" steps instead of joining them
                     into single "Merge"
    :type detailed: bool
    :param start: left border of the time interval
    :type start: datetime.datetime
    :param stop: right border of the time interval
    :type stop: datetime.datetime
    :param bins: number of bins in the histogram
    :type bins: int
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
    if 'detailed' not in kwargs:
        # Join all '* Merge' steps under common label 'Merge'
        if 'Merge' not in data['legend']:
            data['legend'].append('Merge')
            data['data']['x'].append([])
            data['data']['y'].append([])
        merge_idx = data['legend'].index('Merge')
        to_remove = []
        for idx, step in enumerate(data['legend']):
            if 'Merge' in step and step != 'Merge':
                to_remove.append(idx)
                data['data']['x'][merge_idx] += data['data']['x'][idx]
                data['data']['y'][merge_idx] += data['data']['y'][idx]
        for idx in to_remove[::-1]:
            del data['legend'][idx]
            del data['data']['x'][idx]
            del data['data']['y'][idx]
    result = {}
    if rtype == 'json':
        # json module doesn't know how to serialize `datetime` objects
        x_data = data['data']['x']
        for i, _ in enumerate(x_data):
            for j, d in enumerate(x_data[i]):
                x_data[i][j] = str(d)
        result = data
    if rtype == 'img':
        # Reorder data series according to the steps order
        steps_order = ['Evgen', 'Simul', 'Reco', 'Deriv', 'Merge']
        reordered_idx = [-1] * max(len(data['legend']), len(steps_order))
        extra = 1
        for idx, step in enumerate(data['legend']):
            try:
                reordered_idx[steps_order.index(step)] = idx
            except ValueError:
                reordered_idx[-extra] = idx
                extra += 1
        if -1 in reordered_idx:
            reordered_idx = set(reordered_idx)
            reordered_idx.remove(-1)
        new_data = {'legend': [], 'data': {'x': [], 'y': []}}
        for i in reordered_idx:
            new_data['legend'].append(data['legend'][i])
            new_data['data']['x'].append(data['data']['x'][i])
            new_data['data']['y'].append(data['data']['y'][i])
        data = new_data
        pyplot.figure(figsize=(20, 15))
        bins = kwargs.get('bins')
        if not bins:
            default_max_bins = 400
            total_x = []
            for x in data['data']['x']:
                total_x += x
            bins = min(len(set(total_x)), default_max_bins)
        pyplot.hist(data['data']['x'], weights=data['data']['y'],
                    stacked=True, bins=int(bins))
        pyplot.legend(data['legend'], fontsize=20)
        title = ', '.join(kwargs['htags'])
        font = {}
        font['fontsize'] = 36
        font['fontweight'] = 'bold'
        pyplot.suptitle(title, **font)
        pyplot.xlabel('Days', labelpad=20, fontsize=30)
        pyplot.ylabel('Runnung tasks', labelpad=20, fontsize=30)
        pyplot.tick_params(labelsize=18)
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
