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

from ..exceptions import DkbApiNotImplemented
from exceptions import (StorageClientException,
                        QueryNotFound,
                        MissedParameter,
                        NoDataFound
                        )
from .. import config

# To ensure storages are named same way in all messages
STORAGE_NAME = 'Elasticsearch'

# Path to queries
QUERY_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         'es', 'query')

# Default datetime format
DATE_FORMAT = '%d-%m-%Y %H:%M:%S'

with open('ctag_formats.json') as f:
    OUTPUT_FORMATS = json.load(f)


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
    kwargs['body'] = {'query': {'term': {'chain_id': chain_id}}}
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
               _data: [ ..., {..., output_dataset: [ {...}, ...], ... }, ... ]
             }
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


def get_output_formats(tags):
    formats = []
    for tag in tags:
        f = OUTPUT_FORMATS.get(tag)
        if f:
            formats += f
    return formats


def task_derivation_statistics(**kwargs):
    init()
    project = kwargs.get('project').lower()
    tags = [tag for tag in kwargs.get('amitag').split(',') if tag]
    outputs = get_output_formats(tags)
    result = {'_data': outputs}
    return result
