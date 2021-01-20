"""
Common functions and variables for interaction with DKB ES storage.
"""

import logging
import sys
import os
import traceback
import json
from datetime import datetime
import copy

from api.exceptions import (DkbApiNotImplemented,
                            MethodException)
from api.storages.exceptions import (StorageClientException,
                                     QueryNotFound,
                                     MissedParameter)
from api.common import (STEP_TYPES,
                        DATE_FORMAT)
from api import config


# To ensure storages are named same way in all messages
STORAGE_NAME = 'Elasticsearch (nested)'

try:
    import elasticsearch
    from elasticsearch.exceptions import NotFoundError
except ImportError:
    logging.warn("Failed to import module 'elasticsearch'. All methods"
                 " communicating with %s will fail." % STORAGE_NAME)

# Path to queries
QUERY_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         'query')

# Default datetime format for ES
ES_DATE_FORMAT = '%d-%m-%y %H:%M:%S'

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

STEP_FIELDS = {STEP_TYPES[0]: 'step_name',
               STEP_TYPES[1]: 'ctag_format_step'}

# ES prefix aggregations
# NOTE: all intermediate aggregations MUST be named after the prefix name
PREFIX_AGGS = {
    'status': {'terms': {'field': 'status'}},
    'output_dataset': {'nested': {'path': 'output_dataset'},
                       'aggs': {'output_dataset': {'filter': {
                           'term': {'output_dataset.deleted': False}}}}},
    'input': {'filter': {'range': {'input_bytes': {'gt': 0}}}}
}

# Frequently used warning messages
WARNINGS = {}


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
        es = elasticsearch.Elasticsearch(hosts, http_auth=(user, passwd),
                                         timeout=600)
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
        if '!' in val and val['!']:
            params['!' + str(key)] = val.pop('!')
        if '|' in val and val['|']:
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
                queries.remove(q)
        # Join all queries under single ``must`` query
        query['bool'] = {'must': queries}
    elif len(queries) == 1:
        query = queries[0]
    else:
        raise MethodException(reason='No selection parameters specified.')
    return query


def task_info(tid, fields=None):
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


def tokens(text, index='', field=None, analyzer=None):
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
    body = {'text': text}
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


def get_step_aggregation_query(step_type=None):
    """ Construct "aggs" part of ES query for steps aggregation.

    :raises: `ValueError`: unknown step type.

    :param step_type: what should be considered as step:
                      'step', 'ctag_format' (default: 'step')
    :type step_type: str

    :return: "aggs" part of ES query
    :rtype: dict
    """
    aggs = {}
    if not step_type:
        step_type = STEP_TYPES[0]
    elif step_type not in STEP_TYPES:
        raise ValueError(step_type, "Unknown step type (expected one of: %s)"
                                    % STEP_TYPES)
    step_field = STEP_FIELDS.get(step_type)
    if step_field:
        aggs = {'steps': {'terms': {'field': '%s.keyword' % step_field}}}
    else:
        raise DkbApiNotImplemented("Aggregation by steps of type '%s' is not"
                                   " implemented yet.")
    return aggs


def get_agg_units_query(units):
    """ Construct part of ES query "aggs" section for specific units.

    :param units: list of unit names. Possible values:
                  * ES task field name (to get sum of values);
                  * ES task field alias ('hs06', 'hs06_failed', ...);
                  * units with special aggregation rules
                    (e.g. 'task_duration', 'last_update');
                  * prefixed values (prefix is separated from the rest of the
                    value with '__'). Supported prefixes are:
                    - 'output_dataset': for not removed output datasets
                                        (ds size (bytes) > 0);
                    - 'input': same for input datasets;
                    - 'status': for values calculated separately for tasks
                                with different task statuses;
                  * prefixes themselves (to get number of records over which
                    prefixed units are aggregated).
    :type units: list(str)

    :returns: part of ES query "aggs" section
    :rtype: dict
    """
    aggs = {}
    field_mapping = {'hs06': 'toths06',
                     'hs06_failed': 'toths06_failed',
                     }
    prefix_aggs = copy.deepcopy(PREFIX_AGGS)
    # NOTE: all intermediate aggregations MUST be named after the unit name
    special_aggs = {
        'task_duration': {
            'filter': {'bool': {'must': [
                {'exists': {'field': 'end_time'}},
                {'exists': {'field': 'start_time'}},
                {'script': {'script': "doc['end_time'].value >"
                                      " doc['start_time'].value"}}]}},
            'aggs': {'task_duration': {'avg': {
                'script': {'inline': "doc['end_time'].value -"
                                     " doc['start_time'].value"}}}}},
        'last_update': {'max': {'field': 'task_timestamp'}}
    }
    prefixed_units = {}
    clean_units = list(units)

    for unit in units:
        u = unit
        for p in prefix_aggs:
            if unit.startswith(p + '__'):
                clean_units.remove(unit)
                u = unit[(len(p) + 2):]
                if prefix_aggs[p].get('nested'):
                    u = '.'.join([p, u])
                prefixed_units[p] = prefixed_units.get(p, [])
                prefixed_units[p].append(u)
                break
        if not u:
            raise ValueError(unit, 'Invalid aggregation unit name.')

    for p in prefixed_units:
        agg = copy.deepcopy(prefix_aggs[p])
        add_aggs = get_agg_units_query(prefixed_units[p])
        try:
            a = agg
            while a and a.get('aggs'):
                a = a['aggs'][p]
        except KeyError:
            raise MethodException(reason="Invalid prefix aggregation rule"
                                         "('%s')" % p)
        a['aggs'] = add_aggs
        aggs[p] = agg

    for unit in clean_units:
        agg = aggs[unit] = aggs.get(unit, {})
        if agg and unit in prefix_aggs:
            # Already addressed during prefixed fields handling
            continue
        agg_field = field_mapping.get(unit, unit)
        if unit in special_aggs:
            agg.update(special_aggs[unit])
        elif unit in prefix_aggs:
            agg.update(prefix_aggs[unit])
        else:
            agg.update({'sum': {'field': agg_field}})
    return aggs
