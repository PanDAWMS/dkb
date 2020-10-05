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
                    tokens,
                    get_query,
                    get_selection_query,
                    get_step_aggregation_query,
                    get_agg_units_query)
import transform


def task_steps_hist(**kwargs):
    """ Implementation of :py:func:`storages.task_steps_hist` for ES.

    :return: hash with histogram data and method execution metadata
    :rtype: tuple(dict, dict)
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

    If no ``chain_id`` was found for given task, its taskID is used instead.

    :param tid: task ID
    :type tid: int, str

    :return: task chain data and method execution metadata
    :rtype: tuple(dict, dict)
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
             And method execution metadata
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

    :return: constructed query and method execution metadata
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
                'nested': {
                    'path': 'output_dataset',
                    'score_mode': 'sum',
                    'query': {'match_all': {}},
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

    :return: tasks and related datasets metadata, and method execution metadata
    :rtype: tuple(list, dict)
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
    data, metadata = transform.task_info(r)
    if warn:
        # TODO: more accurate metadata join
        metadata['errors'] = warn
    return data, metadata


def task_derivation_statistics(**kwargs):
    """ Calculate statistics of derivation efficiency.

    :param project: project name
    :type project: str
    :param amitag: amitag (or several)
    :type amitag: str or list

    :return: calculated statistics in format required by
             :py:func:`api.handlers.task_deriv` and method execution metadata
    :rtype: tuple(list, dict)
    """
    init()
    project = kwargs.get('project').lower()
    tags = kwargs.get('amitag')
    if isinstance(tags, (str, unicode)):
        tags = [tags]

    q = dict(TASK_KWARGS)
    q['body'] = get_query('deriv', project=project, ctag=tags)
    q['request_timeout'] = 30
  

    data = client().search(**q)
    data, metadata = transform.derivation_statistics(data)

    if WARNINGS.get('output_formats'):
        if not metadata.get('warning'):
            metadata['warning'] = WARNINGS['output_formats']
        elif type(metadata['warning']) is list:
            metadata['warning'].append(WARNINGS['output_formats'])
        else:
            metadata['warning'] = [metadata['warning'],
                                   WARNINGS['output_formats']]
    return data, metadata


def campaign_stat(selection_params, step_type='step', events_src=None):
    """ Calculate values for campaign progress overview.

    :param step_type: step definition type: 'step', 'ctag_format'
                      (default: 'step')
    :type step_type: str

    :param selection_params: defines conditions to select tasks for
                             statistics. Parameter names are mapped
                             to storage record fields (names and/or
                             aliases). Values should be provided in
                             one of the following forms:
                             * ``None`` (field must not be presented
                               in selected records);
                             * (list of) exact field value(s).
                             Field values are broken into categories:
                             * ``&`` -- field must have all these values;
                             * ``|`` -- field must have at least one of
                                        these values;
                             * ``!`` -- field must not have none of these
                                        values.
                             Expected format:
                             ```
                             {
                               <selection_param>: {
                                 <category>: [<values>],
                                 ...
                               },
                               ...
                             }
                             ```
    :type selection_params: dict

    :param events_src: source of data for 'output' events.
                       Possible values:
                       * 'ds'   -- number of events in output datasets;
                       * 'task' -- number of processed events of 'done'
                                   and 'finished' tasks;
                       * 'all'  -- provide all possible values as hash.
    :type events_src: str

    :return: calculated campaign statistics and method execution metadata
    :rtype: tuple(dict, dict)
    """
    init()
    # Construct query
    query = dict(TASK_KWARGS)
    # * campaign is about "production", so we can say for sure
    #   what index we need
    query['index'] = common.CONFIG['index']['production_tasks']
    # * for statistics query we don't need any source documents
    query['size'] = 0
    # * and query body:
    #  - select tasks
    q = get_selection_query(**selection_params)
    #  - divide them into 'steps'
    step_agg = get_step_aggregation_query(step_type)
    #  - get agg values for each step ('instep' aggs)
    instep_aggs = get_query('campaign-stat-step-aggs')
    #  - construct 'last_update' part
    last_update = get_agg_units_query(['last_update'])
    #  - put 'instep' aggs into the innermost (sub) step clause
    instep_clause = step_agg['steps']
    if not instep_clause.get('aggs'):
        instep_clause['aggs'] = {}
    instep_clause = instep_clause['aggs']
    instep_clause.update(instep_aggs)
    #  - join 'query' and 'aggs' parts within request body
    q_body = {'query': q, 'aggs': step_agg}
    #  - add 'last_update' part into the 'aggs' part
    q_body['aggs'].update(last_update)

    query['body'] = q_body

    rdata, metadata = {}, {}
    result = (rdata, metadata)
    try:
        data = client().search(**query)
    except Exception, err:
        msg = "(%s) Failed to execute search query: %s." % (STORAGE_NAME,
                                                            str(err))
        raise MethodException(reason=msg)
    try:
        data, m = transform.campaign_stat(data, events_src)
    except Exception, err:
        msg = "Failed to parse storage response: %s." % str(err)
        raise MethodException(reason=msg)

    try:
        rdata.update(data)
        metadata.update(m)
    except NameError:
        # One of the previously tried operations didn't work
        pass

    return result


def step_stat(selection_params, step_type='step'):
    """ Calculate statistics for tasks by execution steps.

    :param selection_params: hash of parameter defining task selection
                             (for details see
                              :py:func:`common.get_selection_query`)
    :type selection_params: dict
    :param step_type: step definition type: 'step', 'ctag_format'
                      (default: 'step')
    :type step_type: str

    :return: hash with calculated statistics for ``step/stat`` method
             (see :py:func:`api.handlers.step_stat`) and method execution
             metadata
    :rtype: tuple(dict, dict)
    """
    init()
    # Aborted/failed/broken/obsolete tasks should be excluded from statistics
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
    r, m = transform.step_stat(r, agg_units, step_type)
    if step_type == 'ctag_format' and WARNINGS.get('output_formats'):
        # TODO: more accurate metadata join
        m['warning'] = WARNINGS['output_formats']
    return r, m
