"""
Interaction with DKB ES storage.
"""

import logging

from ..exceptions import DkbApiNotImplemented, StorageClientException
from .. import config

# To ensure storages are named same way in all messages
STORAGE_NAME = 'Elasticsearch'

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
    if not CONFIG:
        CONFIG = config.read_config('storage', STORAGE_NAME)
    raise DkbApiNotImplemented


def task_steps_hist(**kwargs):
    """ Implementation of ``task_steps_hist`` for ES.

    :return: hash with histogram data;
             False in case of ES client failure.
    :rtype: dict, bool
    """
    es = init()
    raise DkbApiNotImplemented
