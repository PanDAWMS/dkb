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
                        InvalidArgument,
                        MethodNotFound
                        )
from . import __version__
import storages
from misc import sort_by_prefixes

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
        pyplot.ylabel('Running tasks', labelpad=20, fontsize=30)
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


def task_kwsearch(path, **kwargs):
    """ Get list of tasks by keywords.

    Wildcard search is performed by ``taskname`` only.

    :param path: full path to the method
    :type path: str
    :param kw: list of keywords (or a single keyword)
    :type kw: list, str
    :param analysis: if analysis tasks should be searched
                     (default: True)
    :type analysis: str, bool
    :param production: if production tasks should be searched
                       (default: True)
    :type production: str, bool
    :param size: number of task documents in response (default: 2000)
    :type size: str, int
    :param ds_size: max number of dataset documents returned for each task
                    (default: 20)
    :type size: str, int
    :param timeout: request execution timeout (sec) (default: 120)
    :type timeout: str, int

    :return: tasks and related datasets metadata with additional info:
             { _took_storage_ms: <storage query execution time in ms>,
               _total: <total number of matching tasks>,
               _data: [..., {..., output_dataset: [{...}, ...], ...}, ...],
               _errors: [..., <error message>, ...]
             }
             (field `_errors` may be omitted if no error has occured)
    :rtype: dict
    """
    method_name = '/task/kwsearch'
    params = {
        'analysis': True,
        'production': True,
        'size': 2000,
        'ds_size': 20,
        'timeout': 120
    }
    params.update(kwargs)
    if (kwargs.get('analysis') is True):
        params['production'] &= bool(kwargs.get('production'))
    if (kwargs.get('production') is True):
        params['analysis'] &= bool(kwargs.get('analysis'))
    if not (params.get('analysis') or params.get('production')):
        raise MethodException(method_name, "Parameters 'production' and "
                              "'analysis' should not be set to False at the "
                              "same time.")
    kw = kwargs.get('kw')
    if kw is None:
        raise MissedArgument(method_name, 'kw')
    if not isinstance(kw, list):
        kw = [kw]
    params['kw'] = kw
    for p in ('size', 'ds_size', 'timeout'):
        try:
            params[p] = int(params[p])
        except ValueError:
            raise InvalidArgument(method_name, (p, params[p], int))
    return storages.task_kwsearch(**params)


methods.add('/task', 'kwsearch', task_kwsearch)


def task_deriv(path, **kwargs):
    """ Calculate statistics of derivation efficiency.

    :param path: full path to the method
    :type path: str
    :param project: project name
    :type project: str
    :param amitag: amitag (or several)
    :type amitag: str or list

    :return: calculated statistics
    :rtype: dict
    """
    method_name = '/task/deriv'
    if 'project' not in kwargs:
        raise MissedArgument(method_name, 'project')
    if 'amitag' not in kwargs:
        raise MissedArgument(method_name, 'amitag')
    return storages.task_derivation_statistics(**kwargs)


methods.add('/task', 'deriv', task_deriv)


def campaign_stat(path, **kwargs):
    """ Calculate values for campaign progress overview.

    :param path: full path to the method
    :type path: str
    :param htag: hashtag to select campaign tasks
    :type htag: str, list
    :param events_src: source of data for 'output' events.
                       Possible values:
                       * 'ds'   -- number of events in output datasets
                                   (default);
                       * 'task' -- number of processed events of 'done'
                                   and 'finished' tasks;
                       * 'all'  -- provide all possible values as hash.
    :type events_src: str

    :return: calculated campaign statistics
    :rtype: dict
    """
    method_name = '/campaign/stat'
    events_src_values = ['ds', 'task', 'all']
    if 'htag' not in kwargs:
        raise MissedArgument(method_name, 'htag')
    if 'events_src' in kwargs:
        if kwargs['events_src'] not in events_src_values:
            raise InvalidArgument(method_name, ('events_src',
                                                kwargs['events_src'],
                                                events_src_values))
    else:
        kwargs['events_src'] = events_src_values[0]
    return storages.campaign_stat(**kwargs)


methods.add('/campaign', 'stat', campaign_stat)


def task_stat(path, **kwargs):
    """ Get tasks statistics.

    :param path: full path to the method
    :type path: str
    :param stat_type: statistics type: 'steps', 'formats', 'ctag_formats'
                      (default: 'steps')
    :type stat_type: str
    :param pr: production request number (for `stat_type` value: 'formats')
    :type pr: str, int
    :param htag: hashtag (for `stat_type` values: 'steps', 'format_ctag').
                 Hashtag may be prefixed by a modificator:
                 * & -- all these hashtags must be presented (NOT SUPPORTED);
                 * | -- at least one of these hashtags must be presented (default);
                 * ! -- these hatshtags must not be presented (NOT SUPPORTED).
    :type htag: str
    """
    method_name = '/task/stat'
    allowed_types = ['steps', 'formats', 'ctag_formats']
    htag_prefixes = ['&', '|', '!']
    required = {
        allowed_types[0]: ['htag'],
        allowed_types[1]: ['pr'],
        allowed_types[2]: ['htag'],
    }
    params = {
        'stat_type': allowed_types[0]
    }
    params.update(kwargs)
    if (not params['stat_type'] in allowed_types):
        raise InvalidArgument(method_name, ('stat_type', params['stat_type'],
                                            allowed_types))
    req = required.get(params['stat_type'], [])
    for r in req:
        if not params.get(r):
            raise MissedArgument(method_name, r)
    htags = params.get('htag', [])
    if not isinstance(htags, list):
        htags = [htags]
    if htags:
        params['htags'] = sort_by_prefixes(htags, htag_prefixes, 1)
    if params['htags']['&'] or params['htags']['!']:
        raise DkbApiNotImplemented("Operations are not supported: AND (&), NOT (!).")
    try:
        result = methods.handler(method_name, params['stat_type'])(None, **params)
    except MethodNotFound:
        raise DkbApiNotImplemented("Method for statistics type '%s' is not"
                                   " implemented yet." % params['stat_type'])
    return result


methods.add('/task', 'stat', task_stat)


def task_stat_steps(path, **kwargs):
    """ Get tasks statistics by steps for given hashtags combination.

    :param path: full path to the method (if None, method was called
                 by another method and all other parameters are considered to be
                 already analyzed)
    :type path: str, NoneType
    :param htag: list of (unanalyzed) hashtags, or hash of analyzed hashtags.
                 Each unanalyzed hashtag may be prefixed by a modificator:
                 * & -- all these hashtags must be presented (NOT SUPPORTED);
                 * | -- at least one of these hashtags must be presented (default);
                 * ! -- these hatshtags must not be presented (NOT SUPPORTED).
                 Hash of analyzed hashtags has the following format:
                 ```
                 { '&': [htag1, htag2, ...],
                   '|': [...],
                   '!': [...]
                 }
                 ```
    :type htag: str, dict
    """
    if path is not None:
        result = methods.handler('/task', 'stat')(None, stat_type='steps', **kwargs)
    else:
        result = storages.task_stat_steps(**kwargs)
    return result


methods.add('/task/stat', 'steps', task_stat_steps)
