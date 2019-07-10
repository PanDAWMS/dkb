"""
pyDKB.storages.atlas.rucio
"""

import os

from ..client import Client
from ..exceptions import StorageException
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
        """ Initialize parent client class. """
        ParentClientClass.__init__(self, *args, **kwargs)
