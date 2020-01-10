"""
pyDKB.storages.client.Client
"""

from pyDKB.common import LoggableObject


class Client(LoggableObject):
    """ Interface class for external and internal DKB storage clients. """

    def __init__(self):
        """ Initialize Storage object. """
        raise NotImplementedError

    def configure(self, cfg):
        """ Apply storage configuration (initialize client).

        :param cfg: configuration parameters
        :type cfg: dict
        """
        raise NotImplementedError

    def get(self, oid, **kwargs):
        """ Get object / record from storage by ID.

        :raise NotFound: object / record is not found

        :param oid: object / record identifier
        :type oid: str, int

        :return: record with given ID
        :rtype: dict
        """
        raise NotImplementedError
