"""
Interaction with DKB ES storage.
"""

import logging

from ..exceptions import DkbApiNotImplemented, StorageClientException

# To ensure storages are named same way in all messages
STORAGE_NAME = 'Elasticsearch'

try:
    import elasticsearch
except ImportError:
    logging.warn("Failed to import module 'elasticsearch'. All methods"
                 " communicating with %s will fail." % STORAGE_NAME)

# ES client instance (global variable)
es = None


def init(config):
    """ Configure and initialize DKB ElasticSearch client.

    If connection is already established, do nothing.

    Raise StorageClientException in case of failure

    :param config:
    :type config:

    :return: ES client
    :rtype: elasticsearch.client.Elasticsearch
    """
    raise DkbApiNotImplemented


def task_steps_hist(**kwargs):
    """ Implementation of ``task_steps_hist`` for ES.

    :return: hash with histogram data;
             False in case of ES client failure.
    :rtype: dict, bool
    """
    raise DkbApiNotImplemented
