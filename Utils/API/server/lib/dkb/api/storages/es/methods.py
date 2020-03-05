"""
Interaction with DKB ES storage.
"""

import logging
import json
from datetime import datetime
import time

from api.exceptions import MethodException
from api.storages.exceptions import NoDataFound
from api.common import DATE_FORMAT

from . import STORAGE_NAME
import common
from common import (TASK_KWARGS,
                    WARNINGS)
from common import (init,
                    client,
                    task_info,
                    output_formats,
                    tokens,
                    get_query,
                    get_selection_query,
                    get_step_aggregation_query,
                    get_agg_units_query)
import transform


def task_steps_hist(**kwargs):
    """ Implementation of :py:func:`storages.task_steps_hist` for ES.

    Result hash is of following format:

    ```
    {
      'legend': ['series1_name', 'series2_name', ...],
      'data': {
        'x': [
          [x1_1, x1_2, ...],
          [x2_1, x2_2, ...],
          ...
        ],
        'y': [
          [y1_1, y1_2, ...],
          [y2_1, y2_2, ...],
          ...
        ]
      }
    }
    ```

    Series can be of different length, but ``xN`` and ``yN`` arrays
    have same length.

    :return: hash with histogram data
    :rtype: dict
    """
    init()
    q = dict(TASK_KWARGS)
    start = kwargs.get('start')
    end = kwargs.get('end')
    if end:
        current_ts = end
    else:
        current_ts = datetime.utcnow()
    kwargs['current_ts_ms'] = int(time.mktime(current_ts.timetuple())) * 1000
    q['body'] = get_query('task-steps-hist', **kwargs)
    if start:
        r = {'range': {'end_time': {'gte': start.strftime(DATE_FORMAT)}}}
        q['body']['query']['bool']['must'].append(r)
    if end:
        r = {'range': {'start_time': {'lte': end.strftime(DATE_FORMAT)}}}
        q['body']['query']['bool']['must'].append(r)
    data = client().search(**q)
    result = transform.task_steps_hist(data, start, end)
    return result


def task_chain(**kwargs):
    """ Reconstruct task chain from given task ID.

    If task not found in the ES, raises ``NoDataFound``.

    If for given task no ``chain_id`` found, task ID is used instead.

    :param tid: task ID
    :type tid: int, str

    :return: task chain:
             {
                 ...,
                 taskidN: [childN1_id, childN2_id, ...],
                 ...
             }
    :rtype: dict
    """
    init()
    tid = kwargs.get('tid')
    task_data = task_info(tid, fields=['chain_id'])
    if task_data is None:
        raise NoDataFound(STORAGE_NAME, 'Task (taskid = %s)' % tid)
    chain_id = task_data.get('chain_id', tid)
    data = _chain_data(chain_id)
    result = transform.construct_chain(data)
    return result


def _chain_data(chain_id):
    """ Get full set of chain data from the ES.

    :param chain_id: chain ID ( = root task ID)
    :type chain_id: int, str

    :return: chain data -- lists of task IDs, ordered from root to every task
             belonging to the chain:
             [
                 ...,
                 [chain_id, other_taskid_1, other_taskid_2, ..., taskid],
                 ...
             ]
    :rtype: list
    """
    kwargs = dict(TASK_KWARGS)
    kwargs['body'] = {'query': get_selection_query(chain_id=chain_id)}
    kwargs['_source'] = ['chain_data']
    kwargs['from_'] = 0
    kwargs['size'] = 2000
    rtrn = []
    while True:
        results = client().search(**kwargs)
        if not results['hits']['hits']:
            break
        for hit in results['hits']['hits']:
            chain_data = hit['_source'].get('chain_data')
            if chain_data:
                rtrn.append(chain_data)
        kwargs['from_'] += kwargs['size']
    return rtrn


def _task_kwsearch_query(kw, ds_size=100):
    """ Construct query for task keywords search.

    :param kw: list of (string) keywords
    :type kw: list
    :param ds_size: number of output datasets to return
                    (default: 100)
    :type ds_size: int

    :return: constructed query
    :rtype: dict
    """
    qs_args = []
    wildcard = False
    for w in kw:
        if '?' in w or '*' in w:
            tokens = tokens(w, analyzer='dsname_fields_wildcarded')
            for t in tokens:
                if '?' in t or '*' in t:
                    qs_args.append('taskname.fields:%s' % t)
                    wildcard = True
                else:
                    qs_args.append(t)
        else:
            qs_args.append(w)
    q = {
        'bool': {
            'must': {
                'query_string': {
                    'query': ' AND '.join(qs_args),
                    'analyze_wildcard': wildcard,
                    'all_fields': True,
                    'default_operator': 'AND'
                }
            },
            'should': {
                'has_child': {
                    'type': 'output_dataset',
                    'score_mode': 'sum',
                    'query': {'match_all': {}},
                    'inner_hits': {'size': ds_size}
                }
            }
        }
    }
    return q


def task_kwsearch(**kwargs):
    """ Implementation of :py:func:`storages.task_kwsearch` for ES.

    :param kw: list of (string) keywords
    :type kw: list
    :param analysis: if analysis tasks should be searched
    :type analysis: str, bool
    :param production: if production tasks should be searched
    :type production: str, bool
    :param size: number of documents in response
    :type size: int
    :param ds_size: max number of output datasets to return for each task
    :type ds_size: int
    :param timeout: request execution timeout (sec)
    :type timeout: int

    :return: tasks and related datasets metadata with additional info:
             { _took_storage_ms: <storage query execution time in ms>,
               _total: <total number of matching tasks>,
               _data: [..., {..., output_dataset: [{...}, ...], ...}, ...],
               _errors: [..., <error message>, ...]
             }
             (field `_errors` may be omitted if no error has occured)
    :rtype: dict
    """
    init()
    q = _task_kwsearch_query(kwargs['kw'], kwargs['ds_size'])
    logging.debug("Keyword search query: %r" % q)
    warn = []
    idx = []
    try:
        for name in ('production', 'analysis'):
            idx_name = common.CONFIG['index'][name + '_tasks']
            if kwargs[name]:
                if idx_name:
                    idx.append(idx_name)
                else:
                    msg = "Index name not configured (%s_tasks)." % name
                    warn.append(msg)
                    logging.warn(msg)
    except KeyError, err:
        msg = "Missed parameter in server configuration: %s" % str(err)
        warn.append(msg)
        logging.warn(msg)
    r = client().search(index=idx, body={'query': q}, size=kwargs['size'],
                        request_timeout=kwargs['timeout'], doc_type='task')
    result = {'_took_storage_ms': r['took'], '_data': []}
    if warn:
        result['_errors'] = warn
    if not r['hits']['hits']:
        return result
    result['_total'] = r['hits']['total']
    for hit in r['hits']['hits']:
        task = hit['_source']
        try:
            datasets = hit['inner_hits']['output_dataset']['hits']['hits']
        except KeyError:
            datasets = []
        task['output_dataset'] = [ds['_source'] for ds in datasets]
        result['_data'].append(task)
    return result


def get_derivation_statistics_for_output(project, tags, output_format):
    """ Calculate derivation efficiency for given output format.

    Resulting data has the following structure:
    {
      'output': 'SOME_OUTPUT_FORMAT',
      'tasks': 123,
      'task_ids': [id1, id2, ...],
      'ratio': 0.456,
      'events_ratio': 0.789
    }

    :param project: project name
    :type project: str
    :param amitag: amitags
    :type amitag: list
    :param output_format: output format
    :type output_format: str

    :return: calculated efficiency
    :rtype: dict

    """
    query = dict(TASK_KWARGS)
    kwargs = {'project': project, 'ctag': tags, 'output': output_format}
    query['body'] = get_query('deriv', **kwargs)
    query['_source'] = False
    r = client().search(**query)
    try:
        total = r['hits']['total']
        result_events = (r['aggregations']['output_datasets']['not_removed']
                         ['format']['sum_events']['value'])
        result_bytes = (r['aggregations']['output_datasets']['not_removed']
                        ['format']['sum_bytes']['value'])
        input_events = r['aggregations']['input_events']['value']
        input_bytes = r['aggregations']['input_bytes']['value']
        ratio = 0
        if input_bytes != 0:
            ratio = float(result_bytes) / float(input_bytes)
        events_ratio = 0
        if input_events != 0:
            events_ratio = float(result_events) / float(input_events)
        task_ids = [hit['_id'] for hit in r['hits']['hits']]
    except Exception:
        total = 0
        ratio = 0
        events_ratio = 0
        task_ids = []
    return {'output': output_format, 'tasks': total, 'task_ids': task_ids,
            'ratio': ratio, 'events_ratio': events_ratio}


def task_derivation_statistics(**kwargs):
    """ Calculate statistics of derivation efficiency.

    :param project: project name
    :type project: str
    :param amitag: amitag (or several)
    :type amitag: str or list

    :return: calculated statistics
    :rtype: dict
    """
    init()
    project = kwargs.get('project').lower()
    tags = kwargs.get('amitag')
    if isinstance(tags, (str, unicode)):
        tags = [tags]
    outputs = output_formats(project=project, amitag=tags)
    outputs.sort()
    data = []
    for output in outputs:
        r = get_derivation_statistics_for_output(project, tags, output)
        if r['tasks'] > 0:
            data.append(r)
    result = {'_data': data}
    if WARNINGS.get('output_formats'):
        result['_warning'] = WARNINGS['output_formats']
    return result


def campaign_stat(**kwargs):
    """ Calculate values for campaign progress overview.

    :param path: full path to the method
    :type path: str
    :param htag: hashtag to select campaign tasks
    :type htag: str, list
    :param events_src: source of data for 'output' events.
                       Possible values:
                       * 'ds'   -- number of events in output datasets;
                       * 'task' -- number of processed events of 'done'
                                   and 'finished' tasks;
                       * 'all'  -- provide all possible values as hash.
    :type events_src: str

    :return: calculated campaign statistics:
             { _took_storage_ms: <storage query execution time in ms>,
               _total: <total number of matching tasks>,
               _errors: [..., <error message>, ...],
               _data: {
                 last_update: <last_registered_task_timestamp>,
                 date_format: <datetime_format>,
                 tasks_processing_summary: {
                   <step>: {
                     <status>: <n_tasks>, ...,
                     start: <earliest_start_time>,
                     end: <latest_end_time>
                   },
                 overall_events_processing_summary: {
                   <step>: {
                     input: <n_events>,
                     output: <n_events>,
                     ratio: <output>/<input>
                            /* null if 'events_src' is 'all' */
                   },
                   ...
                 },
                 tasks_updated_24h: {
                   <step>: {
                     <status>: {
                       total: <n_tasks>,
                       updated: <n_tasks>
                     },
                     ...
                   },
                   ...
                 },
                 events_24h: {
                   <step>: <n_output_events_for_done_finisfed>,
                   ...
                 }
               }
             }
             (field `_errors` may be omitted if no error has occured)
    :rtype: dict
    """
    init()
    htags = kwargs.get('htag', [])
    if not isinstance(htags, list):
        htags = [htags]
    query = dict(TASK_KWARGS)
    # Campaign is about "production", so we can say for sure
    # what index we need
    query['index'] = common.CONFIG['index']['production_tasks']
    query_kwargs = {'htags': htags}
    query['body'] = get_query('campaign-stat', **query_kwargs)
    r = {}
    data = {}
    try:
        data = client().search(**query)
        data = transform.campaign_stat(data, kwargs.get('events_src', None))
    except KeyError, err:
        msg = "Failed to parse storage response: %s." % str(err)
        raise MethodException(reason=msg)
    except Exception, err:
        msg = "(%s) Failed to execute search query: %s." % (STORAGE_NAME,
                                                            str(err))
        raise MethodException(reason=msg)

    r.update(data)
    return r


def step_stat(selection_params, step_type='step'):
    """ Calculate statistics for tasks by execution steps.

    :param selection_params: hash of parameter defining task selection
                             (for details see
                              :py:func:`common.get_selection_query`)
    :type selection_params: dict
    :param step_type: step definition type: 'step', 'ctag_format'
                      (default: 'step')
    :type step_type: str

    :return: hash with calculated statistics:
             ```
             { '_took_storage_ms': ...,
               '_total': ...,
               '_data': [
                 { 'name': ...,                       # step name
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
    :rtype: hash
    """
    init()
    # Aborted/failed/broken/obsolete tasks should be excluded form statistics
    status = {}
    skip_statuses = ['aborted', 'failed', 'broken', 'obsolete']
    status = selection_params['status'] = selection_params.get('status', {})
    if '!' in status:
        status['!'] += skip_statuses
    else:
        status['!'] = skip_statuses
    # Construct query
    query = dict(TASK_KWARGS)
    # * for now we don't need any statistics for user (analysis) tasks
    query['index'] = common.CONFIG['index']['production_tasks']
    # * for statistics query we don't need any source documents
    query['size'] = 0
    # * and query body...
    q = get_selection_query(**selection_params)
    step_agg = get_step_aggregation_query(step_type, selection_params)
    agg_units = ['input_events', 'input', 'input__input_bytes',
                 'processed_events', 'total_events', 'hs06', 'hs06_failed',
                 'task_duration', 'output', 'output__bytes', 'output__events',
                 'status', 'status__input_events', 'status__processed_events',
                 'status__input__input_bytes']
    instep_aggs = get_agg_units_query(agg_units)
    instep_clause = step_agg['steps']
    while instep_clause.get('aggs'):
        instep_clause = instep_clause['aggs'].get('substeps')
    if instep_clause:
        instep_clause['aggs'] = {}
        instep_clause = instep_clause['aggs']
    instep_clause.update(instep_aggs)
    query['body'] = {'query': q, 'aggs': step_agg}

    # Default request timeout (10 sec) is not always enough
    # (but hopefully it'll be enough after the ES scheme change)
    query['request_timeout'] = 60

    logging.debug('Steps aggregation query:\n%s' % json.dumps(query, indent=2))

    # Execute query
    r = client().search(**query)
    logging.debug('ES response:\n%s' % json.dumps(r, indent=2))
    # ...and parse its response
    r = transform.step_stat(r, agg_units, step_type)
    if step_type == 'ctag_format' and WARNINGS.get('output_formats'):
        r['_warning'] = WARNINGS['output_formats']
    return r
