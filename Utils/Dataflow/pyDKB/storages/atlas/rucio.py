"""
pyDKB.storages.atlas.rucio
"""

from ..client import Client


_client = None


def _initClient():
    """ Initialize client. """
    global _client
    _client = RucioClient()


def getClient():
    """ Get Rucio client. """
    if not _client:
        _initClient()
    return _client


class RucioClient(Client):
    pass
