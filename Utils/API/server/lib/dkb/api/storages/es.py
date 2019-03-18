"""
Interaction with DKB ES storage.
"""

import logging
import sys
import os
import traceback
import json
from datetime import date

from ..exceptions import DkbApiNotImplemented
from exceptions import StorageClientException, QueryNotFound, MissedParameter
from .. import config

# To ensure storages are named same way in all messages
STORAGE_NAME = config.STORAGES['ES']

# Path to queries
QUERY_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         'es', 'query')


try:
    import elasticsearch
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
        CONFIG = config.read_config('storage', STORAGE_NAME)
    hosts = CONFIG.get('hosts', None)
    user = CONFIG.get('user', '')
    passwd = CONFIG.get('passwd', '')
    TASK_KWARGS['index'] = CONFIG.get('index', None)
    try:
        es = elasticsearch.Elasticsearch(hosts, http_auth=(user, passwd),
                                         sniff_on_start=True)
    except Exception, err:
        trace = traceback.format_exception(*sys.exc_info())
        for lines in trace:
            for line in lines.split('\n'):
                if line:
                    logging.debug(line)
        raise StorageClientException(str(err))
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
    params = {key: json.dumps(kwargs[key]) for key in kwargs}
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
    result = {'legend': [], 'data': {'x': [], 'y': []}}
    if raw_data is None:
        return result
    for step in raw_data['aggregations']['steps']['buckets']:
        step_name = step['key']
        x = []
        y = []
        for data in step['chart_data']['buckets']:
            x.append(date.fromtimestamp(data['key'] / 1000))
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
    q['body'] = get_query('task-steps-hist', **kwargs)
    r = client().search(**q)
    return r
