"""
Interaction with DKB ES storage.
"""

import logging
import sys
import traceback

from ..exceptions import DkbApiNotImplemented, StorageClientException
from .. import config

# To ensure storages are named same way in all messages
STORAGE_NAME = config.STORAGES['ES']


try:
    import elasticsearch
except ImportError:
    logging.warn("Failed to import module 'elasticsearch'. All methods"
                 " communicating with %s will fail." % STORAGE_NAME)

# ES configuration
CONFIG = None

# ES client instance (global variable)
es = None


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


def task_steps_hist(**kwargs):
    """ Implementation of ``task_steps_hist`` for ES.

    :return: hash with histogram data;
             False in case of ES client failure.
    :rtype: dict, bool
    """
    es = init()
    raise DkbApiNotImplemented
