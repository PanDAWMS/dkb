"""
Interaction with DKB ES storage.
"""

import logging
import sys
import os
import traceback
import json
from datetime import datetime
import time
import copy

from ..exceptions import (DkbApiNotImplemented,
                          MethodException)
from exceptions import (StorageClientException,
                        QueryNotFound,
                        MissedParameter,
                        NoDataFound
                        )
from . import STEP_TYPES
from .. import config


# To ensure storages are named same way in all messages
STORAGE_NAME = 'Elasticsearch'

# Path to queries
QUERY_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         'es', 'query')

# Default datetime format
DATE_FORMAT = '%d-%m-%Y %H:%M:%S'
ES_DATE_FORMAT = '%d-%m-%y %H:%M:%S'


try:
    import elasticsearch
    from elasticsearch.exceptions import NotFoundError
except ImportError:
    logging.warn("Failed to import module 'elasticsearch'. All methods"
                 " communicating with %s will fail." % STORAGE_NAME)

# ES configuration
CONFIG = None

# ES client instance (global variable)
es = None

TASK_KWARGS = {
    'index': None,
    'doc_type': 'task'
}

# ES field aliases
FIELD_ALIASES = {'amitag': 'ctag',
                 'htag': 'hashtag_list',
                 'pr': 'pr_id'}


def init():
    """ Configure and initialize DKB ElasticSearch client.

    If connection is already established, do nothing.

    Raise StorageClientException in case of failure

    :return: ES client
    :rtype: elasticsearch.client.Elasticsearch
    """
    global CONFIG
    global es
    if es and es.ping():
        return es
    try:
        elasticsearch
    except NameError:
        raise StorageClientException(STORAGE_NAME,
                                     "driver module is not loaded")
    if not CONFIG:
        CONFIG = config.get_config('storages', STORAGE_NAME)
    hosts = CONFIG.get('hosts', None)
    user = CONFIG.get('user', '')
    passwd = CONFIG.get('passwd', '')
    index = CONFIG.get('index', None)
    # Setting default index name
    if isinstance(index, dict):
        TASK_KWARGS['index'] = index['production_tasks']
    else:
        TASK_KWARGS['index'] = index
        CONFIG['index'] = {'production_tasks': index}
    try:
        es = elasticsearch.Elasticsearch(hosts, http_auth=(user, passwd))
    except Exception, err:
        trace = traceback.format_exception(*sys.exc_info())
        for lines in trace:
            for line in lines.split('\n'):
                if line:
                    logging.debug(line)
        raise StorageClientException(STORAGE_NAME, str(err))
    return es


def client():
    """ Get ES connection if configured.

    Raise ``StorageClientException`` if called before client initialization.

    :return: configured ES client
    :rtype: elasticsearch.Elasticsearch
    """
    if es:
        return es
    else:
        raise StorageClientException(STORAGE_NAME,
                                     "client used before initialization.")


def get_query(qname, **kwargs):
    """ Get query from file with given parameter values.

    :return: query JSON or None if not found
    :rtype: dict, NoneType
    """
    fname = os.path.join(QUERY_DIR, qname)
    query = None
    params = {}
    for key in kwargs:
        try:
            params[key] = json.dumps(kwargs[key])
        except TypeError, err:
            if 'datetime' in str(err):
                params[key] = json.dumps(kwargs[key].strftime(DATE_FORMAT))
            else:
                raise
    try:
        with open(fname, 'r') as f:
            query = f.read() % params
        query = json.loads(query)
    except IOError:
        raise QueryNotFound(qname, fname)
    except KeyError, err:
        raise MissedParameter(qname, str(err))
    return query


def get_selection_query(**kwargs):
    """ Construct 'query' part of ES query to select tasks.

    :raises: `MethodException`: no task selection parameters specified.

    Parameter names are ES fields or can be mapped to ones
    (see `FIELD_ALIASES`).
    Values should be provided in one of the following forms:
    * ``None`` (field must not be presented in the document);
    * exact field value (to be used in 'term' query);
    * list of values (to be used in 'terms' query);
    * dict, containing parameter values by categories: `&`,`|`,`!`
      (to form `bool` query). Categories:
      * & -- all these values must be presented
             (NOT SUPPORTED);
      * | -- at least one of these values must
             be presented (default);
      * ! -- these values must not be presented.
      Hash format:
      ```
      { '&': [htag1, htag2, ...],
        '|': [...],
        '!': [...]
      }
      ```

    :return: query
    :rtype: hash
    """
    query = {}
    queries = []
    params = dict(kwargs)

    logging.debug('Selection params: %s' % kwargs)
    # Change and/or/not fields representation
    for (key, val) in kwargs.items():
        if not isinstance(val, dict):
            continue
        if val.get('&'):
            raise DkbApiNotImplemented("Operations are not supported:"
                                       " AND (&).")
        if '!' in val:
            params['!' + str(key)] = val.pop('!')
        if '|' in val:
            params[key] = val['|']
        else:
            del params[key]

    for (key, val) in params.items():
        must_not = False
        if str(key).startswith('!'):
            must_not = True
            key = key[1:]
        fname = FIELD_ALIASES.get(key, key)
        if val and isinstance(val, list):
            q = {'terms': {fname: val}}
        elif val is not None:
            q = {'term': {fname: val}}
        else:
            must_not = not must_not
            q = {'exists': {'field': fname}}
        if must_not:
            q = {'bool': {'must_not': q}}
        queries.append(q)

    if len(queries) > 1:
        # Squash all ``must_not`` sub-queries
        bool_q = None
        transform_to_list = None
        for q in list(queries):
            if isinstance(q, dict) and q.get('bool', {}).get('must_not'):
                if bool_q is None:
                    bool_q = q
                    transform_to_list = True
                    continue
                if transform_to_list is True:
                    bool_q['bool']['must_not'] = [bool_q['bool']['must_not']]
                    transform_to_list = False
                bool_q['bool']['must_not'].append(q['bool']['must_not'])
                queries.delete(q)
        # Join all queries under single ``must`` query
        query['bool'] = {'must': queries}
    elif len(queries) == 1:
        query = queries[0]
    else:
        raise MethodException('No selection parameters specified.')
    return query


def task_steps_hist(**kwargs):
    """ Implementation of ``task_steps_hist`` for ES.

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
    raw_data = _raw_task_steps_hist(**kwargs)
    start = kwargs.get('start')
    end = kwargs.get('end')
    result = {'legend': [], 'data': {'x': [], 'y': []}}
    if raw_data is None:
        return result
    for step in raw_data['aggregations']['steps']['buckets']:
        step_name = step['key']
        x = []
        y = []
        for data in step['chart_data']['buckets']:
            dt = datetime.fromtimestamp(data['key'] / 1000)
            if (not kwargs.get('start') or kwargs['start'] <= dt) \
                    and (not kwargs.get('end') or kwargs['end'] >= dt):
                x.append(dt.date())
                y.append(data['doc_count'])
        result['legend'].append(step_name)
        result['data']['x'].append(x)
        result['data']['y'].append(y)
    return result


def _raw_task_steps_hist(**kwargs):
    """ Get raw data from ES for hist data construction.

    :return: hash with ES response
    :rtype: dict, NoneType
    """
    logging.debug("_raw_task_steps_hist(%s)" % kwargs)
    q = dict(TASK_KWARGS)
    if kwargs.get('end'):
        current_ts = kwargs['end']
    else:
        current_ts = datetime.utcnow()
    kwargs['current_ts_ms'] = int(time.mktime(current_ts.timetuple())) * 1000
    q['body'] = get_query('task-steps-hist', **kwargs)
    if kwargs.get('start'):
        r = {"range": {"end_time": {"gte":
                                    kwargs['start'].strftime(DATE_FORMAT)}}}
        q['body']['query']['bool']['must'].append(r)
    if kwargs.get('end'):
        r = {"range": {"start_time": {"lte":
                                      kwargs['end'].strftime(DATE_FORMAT)}}}
        q['body']['query']['bool']['must'].append(r)
    r = client().search(**q)
    return r


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
    task_data = _task_info(tid, fields=['chain_id'])
    if task_data is None:
        raise NoDataFound(STORAGE_NAME, 'Task (taskid = %s)' % tid)
    chain_id = task_data.get('chain_id', tid)
    data = _chain_data(chain_id)
    result = _construct_chain(data)
    return result


def _task_info(tid, fields=None):
    """ Return information by Task ID.

    :param tid: task ID
    :type tid: int, str
    :param fields: list of fields to be retrieved (``None`` for all fields)
    :type fields: list, None

    :return: retrieved fields (None if task with ``tid`` is not found)
    :rtype: dict, NoneType
    """
    kwargs = dict(TASK_KWARGS)
    kwargs['id'] = tid
    if fields is not None:
        kwargs['_source'] = fields
    try:
        r = client().get(**kwargs)
    except NotFoundError, err:
        kwargs.update({'storage': STORAGE_NAME})
        logging.warn("Failed to get information from %(storage)s: id='%(id)s',"
                     " index='%(index)s', doctype='%(doc_type)s'" % kwargs)
        return None
    return r.get('_source', {})


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


def _construct_chain(chain_data):
    """ Reconstruct chain structure from the chain_data.

    Chain is a group of tasks where the first task is the root, and each next
    task has one of the previous ones as its parent (parent's output includes
    child's input).

    :param chain_data: chain_data of all tasks in the chain (in the form
                       corresponding outcome of ``_chain_data()``)
    :type chain_data: list

    :return: constructed chain -- hash with keys of task IDs and values of the
             given task's child IDs:
             {
                 ...,
                 taskidN: [childN1_id, childN2_id, ...],
                 ...
             }
    :rtype: dict
    """
    chain = {}
    # Order data from longest to shortest lists. Processing [1, 2, 3] is faster
    # than processing [1], then [1, 2] and then [1, 2, 3].
    # TODO: check this more extensively.
    chain_data.sort(key=lambda cd: len(cd), reverse=True)
    for cd in chain_data:
        cd.reverse()
        child = False
        for tid in cd:
            if tid in chain:
                if child:
                    chain[tid].append(child)
                break
            else:
                chain[tid] = []
                if child:
                    chain[tid].append(child)
                child = tid
    return chain


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
            tokens = _get_tokens(w, analyzer='dsname_fields_wildcarded')
            for t in tokens:
                if '?' in t or '*' in t:
                    qs_args.append('taskname.fields:%s' % t)
                    wildcard = True
                else:
                    qs_args.append(t)
        else:
            qs_args.append(w)
    q = {
        "bool": {
            "must": {
                "query_string": {
                    "query": " AND ".join(qs_args),
                    "analyze_wildcard": wildcard,
                    "all_fields": True,
                    "default_operator": "AND"
                }
            },
            "should": {
                "has_child": {
                    "type": "output_dataset",
                    "score_mode": "sum",
                    "query": {"match_all": {}},
                    "inner_hits": {"size": ds_size}
                }
            }
        }
    }
    return q


def _get_tokens(text, index='', field=None, analyzer=None):
    """ Split text into tokens according to task/ds name fields.

    :param text: text to split into tokens
    :type text: str
    :param index: ES index name (required for `field` usage)
                  If not specified, configured default index is used;
                  to ignore any index settings, should be set to ``None``
    :type index: str
    :param field: field name to derive analyzer (`index` is required)
    :type field: str
    :param analyzer: analyzer name or definition (if analyzer is defined
                     for an index (not globally), `index` is required)
    :type analyzer: str, dict

    :return: list of tokens
    :trype: list
    """
    if index is '':
        index = TASK_KWARGS['index']
    body = {"text": text}
    result = []
    if field:
        if not index:
            logging.warn("Index is not specified (will fail to tokenize string"
                         " as field).")
        body['field'] = field
    elif analyzer:
        body['analyzer'] = analyzer
    try:
        res = client().indices.analyze(index=index, body=body)
        for r in res['tokens']:
            result.append(r['token'])
    except Exception, err:
        logging.error("Failed to tokenize string: %r (index: %r). Reason: %s"
                      % (body, index, err))
        result = [text]
    return result


def task_kwsearch(**kwargs):
    """ Implementation of ``task_kwsearch`` for ES.

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
            idx_name = CONFIG['index'][name + '_tasks']
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
    r = client().search(index=idx, body={"query": q}, size=kwargs['size'],
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


def get_output_formats(**kwargs):
    """ Get output formats corresponding to given project and amitags.

    Acsepts keyword parameters in form supported by
    :py:func:`get_selection_query`.

    :return: output formats
    :rtype: list
    """
    formats = []
    query = dict(TASK_KWARGS)
    task_q = get_selection_query(**kwargs)
    ds_q = {"has_parent": {"type": "task", "query": task_q}}
    agg = {"formats": {"terms": {"field": "data_format", "size": 500}}}
    query['body'] = {"query": ds_q, "aggs": agg}
    query['doc_type'] = 'output_dataset'
    query['size'] = 0
    r = client().search(**query)
    return [bucket['key'] for bucket in
            r['aggregations']['formats']['buckets']]


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
    outputs = get_output_formats(project=project, amitag=tags)
    outputs.sort()
    data = []
    for output in outputs:
        r = get_derivation_statistics_for_output(project, tags, output)
        if r['tasks'] > 0:
            data.append(r)
    result = {'_data': data}
    return result


def _transform_campaign_stat(stat_data, events_src=None):
    """ Transform ES response into user-friendly format.

    :param stat_data: ES response to 'campaign-stat' query
    :type stat_data: dict
    :param events_src: source of data for 'output' events.
                       Possible values:
                       * 'ds'   -- number of events in output datasets
                                   (default);
                       * 'task' -- number of processed events of 'done'
                                   and 'finished' tasks;
                       * 'all'  -- provide all possible values as hash.
    :type events_src: str

    :return: properly formatted response for ``campaign_stat()``
    :rtype: dict
    """
    r = {}
    data = {}
    events_src_values = ['ds', 'task', 'all']
    if not events_src:
        events_src = events_src_values[0]
    elif events_src not in events_src_values:
        raise ValueError('(events_src) unexpected value: %s' % events_src)
    r['_took_storage_ms'] = stat_data.pop('took', None)
    r['_total'] = stat_data.get('hits', {}).pop('total', None)
    r['_data'] = data

    data['tasks_processing_summary'] = {}
    data['overall_events_processing_summary'] = {}
    data['tasks_updated_24h'] = {}
    data['events_24h'] = {}
    data['last_update'] = stat_data.get('aggregations', {}) \
                                   .get('last_update', {}) \
                                   .get('value_as_string', None)
    data['date_format'] = ES_DATE_FORMAT

    steps = stat_data.get('aggregations', {}) \
                     .get('steps', {}) \
                     .get('buckets', [])
    for step in steps:
        # Events processing summary
        esp_o = {}
        esp_o['ds'] = step.get('output_events', {}) \
                          .get('output_events', {}) \
                          .get('value', None)
        esp_o['task'] = step.get('finished', {}) \
                            .get('processed_events', {}) \
                            .get('value', None)
        esp_o = esp_o[events_src] if events_src != 'all' else esp_o

        eps = {'input': step.get('input_events', {}).get('value', None),
               'output': esp_o,
               'ratio': None
               }
        try:
            eps['ratio'] = eps['output'] / eps['input']
        except TypeError:
            # Values are not numeric (None or dict)
            pass

        # Tasks processing: summary and updates
        tps = {}
        tps['total'] = step['doc_count']
        tps['start'] = step.get('start', {}) \
                           .get('value_as_string', None)
        tps['end'] = step.get('end', {}) \
                         .get('value_as_string', None)

        tu24h = {}
        e24h = {}
        statuses = step.get('status', {}) \
                       .get('buckets', [])
        for status in statuses:
            tps[status['key']] = status['doc_count']
            tu24h[status['key']] = {}
            tu24h[status['key']]['total'] = status.get('doc_count', None)
            tu24h[status['key']]['updated'] = status.get('updated_24h', {}) \
                                                    .get('doc_count', None)

            # Events in last 24 hours
            e24h_cur = {}
            e24h_cur['ds'] = status.get('updated_24h', {}) \
                                   .get('finished', {}) \
                                   .get('output', {}) \
                                   .get('events', {}) \
                                   .get('value', None)
            e24h_cur['task'] = status.get('updated_24h', {}) \
                                     .get('finished', {}) \
                                     .get('processed_events', {}) \
                                     .get('value', None)
            for key in e24h_cur.keys():
                try:
                    e24h[key] = e24h.get(key, 0) + e24h_cur[key]
                except TypeError:
                    # Current value is None
                    pass

        e24h = e24h.get(events_src, None) if events_src != 'all' else e24h
        if e24h == {}:
            e24h = None

        data['tasks_processing_summary'][step['key']] = tps
        data['overall_events_processing_summary'][step['key']] = eps
        data['tasks_updated_24h'][step['key']] = tu24h
        data['events_24h'][step['key']] = e24h
    return r


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
    query['index'] = CONFIG['index']['production_tasks']
    query_kwargs = {'htags': htags}
    query['body'] = get_query('campaign-stat', **query_kwargs)
    r = {}
    data = {}
    try:
        data = client().search(**query)
        data = _transform_campaign_stat(data, kwargs.get('events_src', None))
    except KeyError, err:
        msg = "Failed to parse storage response: %s." % str(err)
        raise MethodException(msg)
    except Exception, err:
        msg = "(%s) Failed to execute search query: %s." % (STORAGE_NAME,
                                                            str(err))
        raise MethodException(msg)

    r.update(data)
    return r


def _step_aggregation(step_type=None, selection_params={}):
    """ Construct "aggs" part of ES query for steps aggregation.

    :raises: `ValueError`: unknown step type.

    :param step_type: what should be considered as step:
                      'step', 'ctag_format' (default: 'step')
    :type step_type: str
    :param selection_params: parameters that define set of tasks for which
                             the aggregation will be performed
                             (see :py:func:`get_selection_query`)
    :type selection_params: dict

    :return: "aggs" part of ES query
    :rtype: dict
    """
    aggs = {}
    if not step_type:
        step_type = STEP_TYPES[0]
    elif step_type not in STEP_TYPES:
        raise ValueError(step_type, "Unknown step type (expected one of: %s)"
                                    % STEP_TYPES)
    if step_type == 'ctag_format':
        formats = get_output_formats(**selection_params)
        filters = {f:
                    {'has_child':
                     {'type': 'output_dataset',
                      'query': {'term': {'data_format': f}}
                      }}
                   for f in formats}
        aggs = {'steps': {'filters': {'filters': filters},
                          'aggs': {'substeps': {'terms': {'field': 'ctag'}}}}}
    elif step_type == 'step':
        aggs = {'steps': {'terms': {'field': 'step_name.keyword'}}}
    else:
        raise DkbApiNotImplemented("Aggregation by steps of type '%s' is not"
                                   " implemented yet.")
    return aggs


def _agg_units(units):
    """ Construct part of ES query "aggs" section for specific units.

    :param units: list of unit names. Possible values:
                  * ES task field name (to get sum of values);
                  * ES task field alias ('hs06', 'hs06_failed', ...);
                  * units with unique aggregation rules ('input_bytes',
                    'task_duration', 'status', ...);
                  * ES output_dataset field name (with prefix 'output_');
                  * in-status aggregation (with prefix 'status_').
    :type units: list(str)

    .. warning:: This constructor does not support nested prefixes
                 like `status_output_events`.

    :returns: part of ES query "aggs" section
    :rtype: dict
    """
    aggs = {}
    field_mapping = {'hs06': 'toths06',
                     'hs06_failed': 'toths06_failed',
                     }
    prefix_aggs = {'status': {'terms': {'field': 'status'}},
                   'output': {'children': {'type': 'output_dataset'},
                              'aggs': {'not_removed':
                                  {'filter': {'term': {'deleted': False}}}}},
                   'input': {'filter': {'range': {'input_events': {'gt': 0}}}}
                   }
    special_aggs = {'task_duration':
                        {'filter' : {'bool': {'must':
                             [{'exists' : { 'field' : 'end_time' }},
                              {'exists' : { 'field' : 'start_time' }},
                              {'script':
                                  {'script': "doc['end_time'].value >"
                                             " doc['start_time'].value"}}]}},
                         'aggs': {'task_duration': {'avg':
                             {'script': {'inline': "doc['end_time'].value -"
                                                   " doc['start_time'].value"}}}}
                         }
                     }
    prefixed_units = {}
    clean_units = list(units)

    for unit in units:
        u = unit
        for p in prefix_aggs:
            if unit.startswith(p + '__'):
                clean_units.remove(unit)
                u = unit[(len(p) + 2):]
                prefixed_units[p] = prefixed_units.get(p, [])
                prefixed_units[p].append(u)
                break
        if not u:
            raise ValueError(unit, 'Invalid aggregation unit name.')

    for p in prefixed_units:
        agg = copy.deepcopy(prefix_aggs[p])
        add_aggs =_agg_units(prefixed_units[p])
        if p == 'output':
            agg['aggs']['not_removed']['aggs'] = add_aggs
        else:
            agg['aggs'] = add_aggs

        aggs[p] = agg

    for unit in clean_units:
        aggs[unit] = agg = aggs.get(unit, {})
        agg_field = field_mapping.get(unit, unit)
        if unit in special_aggs:
            agg.update(special_aggs[unit])
        elif unit in prefix_aggs:
            agg.update(prefix_aggs[unit])
        else:
            agg.update({'sum': {'field': agg_field}})
    return aggs


def steps_iterator(data):
    """ Gerenator for iterator over steps data.

    Recursively check all buckets within `steps` and `substeps`
    clauses of the `data`.

    :param data: full data to extract steps information from
    :type data: dict

    :returns: steps data in iterable representation.
              Each call of `next()` methor returns tuple:
              (``step_name``, ``step_data``)
    :rtype: iterable object
    """
    if data.get('steps'):
        # `data` contains information about steps
        # (first or recursive calls of the generator)
        buckets = data['steps'].get('buckets', None)
    elif data.get('substeps'):
        # `data` contains information about substeps
        # (recursive calls of the generator)
        buckets = data['substeps'].get('buckets', None)
    else:
        # `data` is data of a single step
        yield None, data
        raise StopIteration

    # Call `steps_iterator` for each bucket
    # (in case there are some sub-steps)
    for bucket in buckets:
        if isinstance(buckets, list):
            bucket_name = bucket.get('key', None)
        elif isinstance(buckets, dict):
            bucket_name = bucket
            bucket = buckets[bucket_name]
        for step_name, step in steps_iterator(bucket):
            step_name = ':'.join([bucket_name, step_name]) if step_name \
                        else bucket_name
            yield step_name, step


def _get_single_stat_value(data, unit):
    """ Get single stat value from data.
    """
    if unit == 'total':
        val = data.get('doc_count', None)
    elif unit:
        while data.get(unit):
            data = data[unit]
        val = data.get('value', None)
    else:
        val = data.get('value', None)
    return val


def _get_bucketed_stat_values(data, units=[]):
    """ Get values of stat units from data with 'buckets'.

    :param data: part of ES response containing statistic values
                 for a single item (e.g. processing step)
    :type data: dict
    :param units: statistics units (:py:func:`_agg_units`)

    :returns: simplified statistics representation, containing
              unit names as keys, and values -- as values
    :rtype: dict
    """
    if not 'buckets' in data:
        raise ValueError('_get_bucketed_stat_values() expects `data` param to'
                         ' contain "buckets".')
    result = {}
    buckets = data['buckets']
    for bucket in buckets:
        if isinstance(buckets, list):
            bucket_name = bucket.get('key', None)
        elif isinstance(buckets, dict):
            bucket_name = i
            bucket = data[i]
        else:
            raise MethodException("Failed to parse ES response.")
        r = result[bucket_name] = {}
        r.update(_get_stat_values(bucket, units))
        if set(r.keys()) == set(['total']):
            result[bucket_name] = r['total']
    return result


def _get_stat_values(data, units=[]):
    """ Get value of statistics units from ES response.

    Input data contains items of one of the following views:
    ```
    1. {<unit_name>: {'value': <desired_value>}}
    2. {<unit_name>: {... {<unit_name>: {'value': <desired_value>}}...}}
    3. {<prefix>: {'buckets': [
           {'key': <bucket_name>, 'doc_count': <n>, <sub-items>},
           ...]
       }}
    4. {<prefix>: {'buckets': {
           <bucket_name>: {'doc_count': <n>, <sub-items>},
           ...}
       }}
    ```

    Output data contains items of one of the following simplified views:
    ```
    1. {<unit_name>: <desired_value>}
    2. {<prefix>: {<bucket_name>: {'total': <n>, <sub-items>}}}
    ```

    ``<Sub-items>`` here are supposed to be of one of the views 1-2 for input
    data and of view 1 for output.

    ``<Unit_name>`` in ``<items>`` is one of the passed ``units``,
    while in ``<sub-items>`` -- the one with stripped ``<prefix>_``.

    .. warning:: This parser does not support nested prefixes
                 like `status_output_events`.

    :param data: part of ES response containing statistic values
                 for a single item (e.g. processing step)
    :type data: dict
    :param units: statistics units (:py:func:`_agg_units`)
    :type units: list

    :returns: simplified statistics representation, containing
              unit names as keys, and values -- as values
    :rtype: dict
    """
    prefixes = ['output', 'input', 'status']
    result = {}
    orig_data = data
    prefixed_units = {}
    clean_units = list(units)

    for unit in units:
        for p in prefixes:
            if unit == p:
                clean_units.remove(unit)
                prefixed_units[p] = prefixed_units.get(p, [])
                prefixed_units[p].append('total')
            if unit.startswith(p + '__'):
                clean_units.remove(unit)
                u = unit[(len(p) + 2):]
                prefixed_units[p] = prefixed_units.get(p, [])
                prefixed_units[p].append(u)
                break

    for p in prefixed_units:
        data = orig_data.get(p, {})
        r = result[p] = {}
        if p == 'output':
            data = data.get('not_removed', {})
        logging.debug('Data:\n%s' % json.dumps(data, indent=2))
        if 'buckets' not in data:
            r.update(_get_stat_values(data, prefixed_units[p]))
        else:
            r.update(_get_bucketed_stat_values(data, prefixed_units[p]))
        logging.debug('Result:\n%s' % json.dumps(r, indent=2))

    if 'buckets' not in data:
        for unit in clean_units:
            result[unit] = _get_single_stat_value(orig_data, unit)
        return result

    bucketed = _get_bucketed_stat_values(data, clean_units)
    for b in bucketed:
        r = result.get(b, {})
        r.update(bucketed[b])
        if set(r.keys()) == set(['total']):
            result[b] = r['total']
    return result


def _transform_task_stat(data, agg_units=[]):
    """ Transform ES query response to required response format.

    :param data: ES response
    :type data: dict

    :returns: prepared response data
    :rtype: dict
    """
    statuses = {0: 'StepNotStarted',
                0.1: 'StepProgressing',
                0.9: 'StepDone'
                }
    r = {}
    r['_took_storage_ms'] = data.pop('took')
    r['_total'] = data.get('hits', {}) \
                      .get('total', None)
    r['data'] = []
    steps = steps_iterator(data.get('aggregations', {}))
    for name, step_data in steps:
        d = {'name': name}
        simplified = _get_stat_values(step_data, agg_units)
        logging.debug('Step data:\n%s' % json.dumps(step_data, indent=2))
        logging.debug('Simplified step data:\n%s' % json.dumps(simplified, indent=2))
        d.update(simplified)
        r['data'].append(d)
    return r


def task_stat(**kwargs):
    """ Calculate statistics for tasks by execution steps.

    :param selection_params: parameters that define how to get tasks
                             for statistics calculation:
                             * pr: production request number;
                             * htag: hash of hashtags divided into categories:
                               * & -- all these hashtags must be presented
                                      (NOT SUPPORTED);
                               * | -- at least one of these hashtags must
                                      be presented (default);
                               * ! -- these hatshtags must not be presented
                                      (NOT SUPPORTED).
                              Hash format:
                              ```
                              { '&': [htag1, htag2, ...],
                                '|': [...],
                                '!': [...]
                              }
                              ```
    :type selection_params: dict

    :param step_type: what should be considered as step:
                      'step', 'ctag_format' (default: 'step')
    :type step_type: str

    :return: hash with calculated statistics:
             ```
             { '_took_storage_ms': ...,
               '_total': ...,
               'data': [
                 { 'name': ...,                       # step name
                   'total_events': ...,
                   'input_events': ...,
                   'input_bytes': ...,
                   'input_not_removed_tasks': ...,
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
             Steps in `data` list are sorted according to step type:
             * 'step': the MC campaign steps order (see `api.config.MC_STEPS`);
             * 'ctag_format': input events number.
    :rtype: hash
    """
    init()
    selection_params = kwargs.get('selection_params', {})
    step_type = kwargs.get('step_type')
    # Aborted/failed/broken/obsolete tasks should be excluded form statistics
    status = {}
    if 'status' in selection_params:
        status = {'|': selection_params['status']}
    status['!'] = ['aborted', 'failed', 'broken', 'obsolete']
    selection_params['status'] = status
    # Construct query
    query = dict(TASK_KWARGS)
    # * for now we don't need any statistics for user (analysis) tasks
    query['index'] = CONFIG['index']['production_tasks']
    # * for statistics query we don't need any source documents
    query['size'] = 0
    # * and query body...
    q = get_selection_query(**selection_params)
    step_agg = _step_aggregation(step_type, selection_params)
    agg_units = ['input_events', 'input__input_bytes', 'processed_events',
                 'total_events', 'hs06', 'hs06_failed', 'task_duration',
                 'output__bytes', 'output__events', 'status',
                 'status__input_events', 'status__processed_events',
                 'status__input__input_bytes', 'output']
    instep_aggs = _agg_units(agg_units)
    instep_clause = step_agg['steps']
    while instep_clause.get('aggs'):
        instep_clause = instep_clause['aggs'].get('substeps')
    if instep_clause:
        instep_clause['aggs'] = {}
        instep_clause = instep_clause['aggs']
    instep_clause.update(instep_aggs)
    query['body'] = {'query': q, 'aggs': step_agg}
    logging.debug('Steps aggregation query:\n%s' % json.dumps(query, indent=2))

    # Execute query
    r = client().search(**query)
    # ...and parse its results
    r = _transform_task_stat(r, agg_units)
    return r
