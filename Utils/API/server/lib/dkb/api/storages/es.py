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


def task_chain(tid):
    """ Implementation of ``task_chain`` for ES.

    :param tid: task ID
    :type tid: int, str

    :return: list of TaskIDs (empty if task with ``tid`` was not found);
             False in case of ES client failure.
    :rtype: list, bool
    """
    raise DkbApiNotImplemented
