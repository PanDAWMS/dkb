"""
pyDKB.storages.atlas.rucio
"""

import os

from ..client import Client
from ..exceptions import (StorageException, NotFound)
from pyDKB.common.misc import (log, logLevel)
from pyDKB.common.misc import try_to_import


if not os.environ.get("VIRTUAL_ENV", None):
    user_rucio_dir = os.path.expanduser("~/.rucio")
    if os.path.exists(user_rucio_dir):
        os.environ["VIRTUAL_ENV"] = os.path.join(user_rucio_dir)
    else:
        os.environ["VIRTUAL_ENV"] = os.path.join(base_dir, ".rucio")
    log("Set VIRTUAL_ENV: %s" % os.environ["VIRTUAL_ENV"], logLevel.INFO)

_RucioClient = try_to_import('rucio.client', 'Client')
RucioException = try_to_import('rucio.common.exception', 'RucioException')


_client = None


def _initClient():
    """ Initialize client. """
    global _client
    if not _RucioClient:
        raise StorageException("Failed to initialize Rucio client: required "
                               "module(s) not loaded.")
    _client = RucioClient()


def getClient():
    """ Get Rucio client. """
    if not _client:
        _initClient()
    return _client


ParentClientClass = _RucioClient if _RucioClient else object


class RucioClient(Client, ParentClientClass):
    """ Implement common interface for Rucio client. """

    def __init__(self, *args, **kwargs):
        """ Initialize instance as parent client class object. """
        ParentClientClass.__init__(self, *args, **kwargs)

    def get(self, oid, **kwargs):
        """ Get dataset metadata.

        Implementation of interface method `Clent.get()`.

        :param oid: dataset name
        :type oid: str
        :param fields: list of requested metadata fields
                       (None = all metadata)
        :type fields: list

        :return: dataset metadata
        :rtype: dict
        """
        scope, name = self._scope_and_name(oid)
        try:
            result = self.get_metadata(scope=scope, name=name)
        except ValueError, err:
            raise StorageException("Failed to get metadata from Rucio: %s"
                                   % err)
        except RucioException, err:
            if 'Data identifier not found' in str(err):
                raise NotFound(scope=scope, name=name)
            raise StorageException("Failed to get metadata from Rucio: %s"
                                   % err)
        if kwargs.get('fields') is not None:
            result = {f: result.get(f, None) for f in kwargs['fields']}
        return result

    def _scope_and_name(self, dsn):
        """ Construct normalized scope and dataset name.

        As input accepts dataset names in two forms:
        * dot-separated string: "<XXX>.<YYY>[.<...>]";
        * dot-separated string with prefix: "<scope>:<XXX>.<YYY>[.<...>]".

        In first case ID is taken as a canonical dataset name and scope is set
        to its first field (or two first fields, if the ID starts with 'user'
        or 'group').
        In second case prefix is taken as scope, and removed from ID to get the
        canonical dataset name.

        :param dsn: dataset name
        :type dsn: str

        :return: scope, datasetname
        :rtype: tuple
        """
        result = dsn.split(':')
        if len(result) < 2:
            splitted = dsn.split('.')
            if dsn.startswith('user') or dsn.startswith('group'):
                scope = '.'.join(splitted[0:2])
            else:
                scope = splitted[0]
            result = (scope, dsn)
        return result
