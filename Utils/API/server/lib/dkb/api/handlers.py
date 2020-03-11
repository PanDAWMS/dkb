"""
Method implementations for DKB API.

All method handlers should be defined as follows:
```
def my_method_handler(path, **kwargs):
    \""" Method description (will be used by ``info`` method).

    Detailed description.

    :param path: request path
    :type path: str
    :param rtype: response type (supported types: ...)
    :type rtype: str

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
from common import (MC_STEPS,
                    STEP_TYPES)
from misc import sort_by_prefixes

from cStringIO import StringIO
import logging
import json

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

    If ``rtype`` is set to 'json', the method returns JSON document
    of the following format:

    ```
    {
      ...
      "data": {
        "legend": ["series1_name", "series2_name", ...],
        "data": {
          "x": [
            [x1_1, x1_2, ...],
            [x2_1, x2_2, ...],
            ...
          ],
          "y": [
            [y1_1, y1_2, ...],
            [y2_1, y2_2, ...],
            ...
          ]
        }
      }
    }
    ```

    Series can be of different length, but ``xN`` and ``yN`` arrays
    have same length.

    :param path: full path to the method
    :type path: str
    :param rtype: response type (supported types: 'json', 'img')
    :type rtype: str

    :param detailed: keep all "* Merge" steps instead of joining them
                     into single "Merge"
    :type detailed: bool
    :param start: left border of the time interval
    :type start: datetime.datetime
    :param stop: right border of the time interval
    :type stop: datetime.datetime
    :param bins: number of bins in the histogram
    :type bins: int

    :return: PNG image or JSON document with data for histogram
    :rtype: object
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
    result = storages.task_steps_hist(**kwargs)
    data = result.pop('_data', {})
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
    if rtype == 'json':
        # json module doesn't know how to serialize `datetime` objects
        x_data = data['data']['x']
        for i, _ in enumerate(x_data):
            for j, d in enumerate(x_data[i]):
                x_data[i][j] = str(d)
        result['_data'] = data
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

    Returns JSON document of the following format:

    ```
    {
      ...
      "data": {
        ...,
        taskidN: [childN1_id, childN2_id, ...],
        ...
      }
    }
    ```

    Each element of ``"data"`` clause corresponds to one of the tasks
    in the chain (with task ID ``taskidN``) and provides list of IDs
    of the successor tasks that take given task's output dataset as input.

    :param path: full path to the method
    :type path: str
    :param rtype: response type (only 'json' supported)
    :type rtype: str

    :param tid: task id
    :type tid: str, int

    :return: task chain data
    :rtype: dict
    """
    method_name = '/task/chain'
    if kwargs.get('rtype', 'json') is not 'json':
        raise MethodException(method_name, "Unsupported response type: '%s'"
                                           % kwargs['rtype'])
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

    .. note: Wildcard search is performed by ``taskname`` only.

    Returns JSON document of the following format:

    ```
    {
      ...
      "data": [..., {..., "output_dataset": [{...}, ...], ...}, ...]
    }
    ```

    Each element of the ``"data"`` clause represents single task with
    all its output datasets.

    :param path: full path to the method
    :type path: str
    :param rtype: response type (only 'json' supported)
    :type rtype: str

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

    :return: tasks and related datasets metadata
    :rtype: dict
    """
    method_name = '/task/kwsearch'
    if kwargs.get('rtype', 'json') is not 'json':
        raise MethodException(method_name, "Unsupported response type: '%s'"
                                           % kwargs['rtype'])
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

    Returns JSON document of the following format:

    ```
    {
      ...
      "data": [
        {
          "output": <output_format>,
          "tasks": <n_tasks>,
          "task_ids": [id1, id2, ...],
          "ratio": <output_to_input_bytes_ratio>,
          "events_ratio": <output_to_input_events_ratio>
        },
        ...
      ]
    }
    ```

    Elements of the ``"data"`` clause are ordered by the
    ``<output_format>`` values (in the alphanumeric order).

    :param path: full path to the method
    :type path: str
    :param rtype: response type (only 'json' supported)
    :type rtype: str

    :param project: project name
    :type project: str
    :param amitag: amitag (or several)
    :type amitag: str or list

    :return: calculated statistics
    :rtype: dict
    """
    method_name = '/task/deriv'
    if kwargs.get('rtype', 'json') is not 'json':
        raise MethodException(method_name, "Unsupported response type: '%s'"
                                           % kwargs['rtype'])
    if 'project' not in kwargs:
        raise MissedArgument(method_name, 'project')
    if 'amitag' not in kwargs:
        raise MissedArgument(method_name, 'amitag')
    return storages.task_derivation_statistics(**kwargs)


methods.add('/task', 'deriv', task_deriv)


def campaign_stat(path, rtype='json', step_type=None, events_src=None,
                  **kwargs):
    """ Calculate values for campaign progress overview.

    Returns JSON document of the following format:

    ```
    {
      ...
      "data": {
        "last_update": <last_registered_task_timestamp>,
        "date_format": <datetime_format>,
        "tasks_processing_summary": {
          <step>: {
            <status>: <n_tasks>, ...,
            "start": <earliest_start_time>,
            "end": <latest_end_time>
          },
          ...
        },
        "overall_events_processing_summary": {
          <step>: {
            "input": <n_events>,
            "output": <n_events>,
            "ratio": <output>/<input>
                   /* null if 'events_src' is 'all' */
          },
          ...
        },
        "tasks_updated_24h": {
          <step>: {
            <status>: {
              "total": <n_tasks>,
              "updated": <n_tasks>
            },
            ...
          },
          ...
        },
        "events_24h": {
          <step>: <n_output_events_for_done_finisfed>,
          ...
        }
      }
    }
    ```

    :param path: full path to the method
    :type path: str
    :param rtype: response type (only 'json' supported)
    :type rtype: str

    :param step_type: step definition type: 'step', 'ctag_format'
                      (default: 'step')
    :type step_type: str

    :param <selection_parameter>: defines condition to select tasks for
                                  statistics. Parameter names are mapped
                                  to storage record fields (names and/or
                                  aliases). Values should be provided in
                                  one of the following forms:
                                  * ``None`` (field must not be presented
                                    in selected records);
                                  * exact field value;
                                  * exact field value with logical prefix:
                                    - ``&`` -- field must value this value;
                                    - ``|`` -- field must have one of values
                                               marked with this prefix
                                               (default);
                                    - ``!`` -- field must not have this value;
                                  * list of field values (prefixed or not).
    :type <selection_parameter>: NoneType, str, number, list

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
    if kwargs.get('rtype', 'json') is not 'json':
        raise MethodException(method_name, "Unsupported response type: '%s'"
                                           % kwargs['rtype'])
    allowed_types = STEP_TYPES
    if step_type is None:
        step_type = allowed_types[0]
    if (step_type not in allowed_types):
        raise InvalidArgument(method_name, ('step_type', step_type,
                                            allowed_types))

    events_src_values = ['ds', 'task', 'all']
    if not events_src:
        events_src = events_src_values[0]
    elif events_src not in events_src_values:
        raise InvalidArgument(method_name, ('events_src', events_src,
                                            events_src_values))

    params = {}
    for param in kwargs:
        vals = kwargs[param]
        if not isinstance(vals, list):
            vals = [vals]
        if vals:
            vals = sort_by_prefixes(vals, ['|', '&', '!'])
        params[param] = vals

    return storages.campaign_stat(step_type=step_type, selection_params=params,
                                  events_src=events_src)


methods.add('/campaign', 'stat', campaign_stat)


def step_stat(path, rtype='json', step_type=None, **kwargs):
    """ Get tasks statistics.

    Returns JSON document of the following format:

    ```
    {
      ...
      "data": [
        {
          'name': ...,
          'total_events': ...,
          'input_events': ...,
          'processed_events': ...,
          'input_bytes': ...,
          'input_not_removed_tasks': ...,
          'finished_bytes': ...,
          'finished_tasks': ...,
          'output_bytes': ...,
          'output_not_removed_tasks': ...,
          'total_tasks': ...,
          'hs06': ...,
          'cpu_failed': ...,
          'duration': ...,                   # days
          'step_status': {'Unknown'|'StepDone'|'StepProgressing'
                          |'StepNotStarted'},
          'percent_done': ...,
          'percent_running': ...,
          'percent_pending': ...
        },
        ...
      ]
   }
    ```

    :param path: full path to the method
    :type path: str
    :param rtype: response type (only 'json' supported)
    :type rtype: str

    :param step_type: step definition type: 'step', 'ctag_format'
                      (default: 'step')
    :type step_type: str

    :param <selection_parameter>: defines condition to select tasks for
                                  statistics. Parameter names are mapped
                                  to storage record fields (names and/or
                                  aliases). Values should be provided in
                                  one of the following forms:
                                  * ``None`` (field must not be presented
                                    in selected records);
                                  * exact field value;
                                  * exact field value with logical prefix:
                                    - ``&`` -- field must value this value;
                                    - ``|`` -- field must have one of values
                                               marked with this prefix
                                               (default);
                                    - ``!`` -- field must not have this value;
                                  * list of field values (prefixed or not).
    :type <selection_parameter>: NoneType, str, number, list

    :return: calculated statistics for selected tasks by steps.
             Steps in "data" list are sorted according to:
             * for 'step' steps: the MC campaign steps order
               (see `common.MC_STEPS`);
             * else: number of step input events (desc).

    :rtype: dict
    """
    method_name = '/step/stat'
    if rtype is not 'json':
        raise MethodException(method_name, "Unsupported response type: '%s'"
                                           % rtype)
    allowed_types = STEP_TYPES
    if step_type is None:
        step_type = allowed_types[0]
    if (step_type not in allowed_types):
        raise InvalidArgument(method_name, ('step_type', step_type,
                                            allowed_types))
    params = {}
    for param in kwargs:
        vals = kwargs[param]
        if not isinstance(vals, list):
            vals = [vals]
        if vals:
            vals = sort_by_prefixes(vals, ['|', '&', '!'])
        params[param] = vals
    logging.debug('(%s) parsed parameters:\n%s' % (method_name,
                                                   json.dumps(params,
                                                              indent=2)))
    r = storages.step_stat(step_type=step_type, selection_params=params)
    if step_type == 'step':
        def steps_cmp(x, y):
            """ Compare processing steps for ordering. """
            try:
                x = MC_STEPS.index(x['name'])
                y = MC_STEPS.index(y['name'])
                return cmp(x, y)
            except KeyError, ValueError:
                pass
            return 0
    else:
        def steps_cmp(x, y):
            """ Compare processing steps for ordering. """
            try:
                x = x['input_events']
                y = y['input_events']
                return - cmp(x, y)
            except KeyError:
                pass
            return 0
    r['_data'].sort(steps_cmp)

    return r


methods.add('/step', 'stat', step_stat)
