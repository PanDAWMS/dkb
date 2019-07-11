"""
pyDKB.storages.client.es
"""

from Client import Client
from pyDKB.common.misc import try_to_import


_ESClient = try_to_import('elasticsearch', 'Elasticsearch')

ParentClientClass = _ESClient if _ESClient else object


class ESClient(Client, ParentClientClass):
    """ Implement common interface for ES client. """

    index = None

    def __init__(self, *args, **kwargs):
        """ Initialize instance as parent client class object. """
        ParentClientClass.__init__(self, *args, **kwargs)
