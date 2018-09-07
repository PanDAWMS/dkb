"""
Interaction with DKB ES storage.
"""

import logging
import sys
import traceback
import json

from ..exceptions import DkbApiNotImplemented, StorageClientException

# To ensure storages are named same way in all messages
STORAGE_NAME = 'Elasticsearch'

from .. import config

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
     'index': 'test_prodsys_rucio_ami',
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
        raise StorageClientException(STORAGE_NAME, "driver module is not loaded")
    if not CONFIG:
        CONFIG = config.read_config('storage', STORAGE_NAME)
    hosts = CONFIG.get('hosts', None)
    user = CONFIG.get('user', '')
    passwd = CONFIG.get('passwd', '')
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


def task_chain(tid):
    """ Implementation of ``task_chain`` for ES.

    :param tid: task ID
    :type tid: int, str

    :return: task chain: ``{root: {child1: {...}, child2: {...}, ...}}``
             (None if failed to get result).
    :rtype: dict, None
    """
    es = init()
    root_tid = _chain_root(tid, es)
    if root_tid is not None:
        result = {int(root_tid): _construct_chain(root_tid, es)}
    else:
        result = None
    return result


def _chain_root(tid, es=None):
    """ Get root element of task chain.

    :param tid: task ID
    :type tid: int, str
    :param es: ES client
    :type es: elasticsearch.client.Elasticsearch

    :return: root Task ID (None if failed to get information)
    :rtype: int, NoneType
    """
    fields = ['parent_tid']
    cur_tid = tid
    prev_tid = None
    while cur_tid and cur_tid != prev_tid:
        task = _task_info(cur_tid, fields)
        if task is None:
            logging.warn("Task chain is broken: task not found"
                         " (tid='%s')." % cur_tid)
            break
        prev_tid = cur_tid
        cur_tid = task.get('parent_tid', None)
    if cur_tid is None:
        logging.warn("Failed to detect task parent (tid='%s')." % prev_tid)
    return prev_tid


def _construct_chain(root_tid, es=None):
    """ Construct chain starting from given task ID.

    :param root_tid: chain root task ID
    :type root_tid: int, str
    :param es: ES client
    :type es: elasticsearch.client.Elasticsearch

    :return: task chain:
             ``{<root_tid>: {<child1_tid>: {...}, <child2_tid>: {...}, ...}}``.
             Hash keys are of type ``int``.
    :rtype: dict
    """
    result = {}
    cur_hash = result
    children = _search_tasks([('parent_tid', cur_tid)])
    hits = children.get('hits', {}).get('hits', [])
    for child in hits:
        tid = child.get('_id')
        result[int(tid)] = _construct_chain(tid)
    if not result:
        result = None
    return result


def _task_info(tid, fields=None, es=None):
    """ Return information by Task ID.

    Raise ``elasticsearch.exceptions.NotFoundError`` if there\`s
    no document with given ``tid``.

    :param tid: task ID
    :type tid: int, str
    :param fields: list of fields to be retrieved (``None`` for all fields)
    :type fields: list, None
    :param es: ES client
    :type es: elasticsearch.client.Elasticsearch

    :return: retrieved fields (None if task with ``tid`` is not found)
    :rtype: dict, NoneType
    """
    kwargs = dict(TASK_KWARGS)
    kwargs['id'] = tid
    if not es:
        es = init()
    if fields is not None:
        kwargs['_source'] = fields
    try:
        r = es.get(**kwargs)
    except NotFoundError, err:
        kwargs.update({'storage': STORAGE_NAME})
        logging.warn("Failed to get information from %(storage)s: id='%(id)s',"
                     " index='%(index)s', doctype='%(doc_type)s'" % **kwargs)
        return None
    return r.get('_source', {})


def _search_tasks(fields, strict=True, es=None):
    """ Search task by field values.

    :param fields: pairs of (field, value)
    :type fields: list
    :param strict: True -- use ``term`` query, False -- ``match``
    :type strict: bool
    :param es: ES client
    :type es: elasticsearch.client.Elasticsearch

    :return: task IDs
    :rtype: list(str)
    """
    kwargs = dict(TASK_KWARGS)
    q = {'query': {'must': []}}
    if strict:
        match = 'term'
    else:
        match = 'match'
    for field, val in fields:
        q['query']['must'] += [{match: {field: val}}]
    kwargs['body'] = json.dumps(q)
    kwargs['_source'] = ['taskid']
    result = []
    if not es:
        es = init()
    r = es.search(**kwargs)
    for h in r.get('hits',{}).get('hits',[]):
        tid = h.get('_id', None)
        if tid:
            result += [tid]
    return result
