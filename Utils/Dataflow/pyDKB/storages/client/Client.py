"""
pyDKB.storages.client.Client
"""

from pyDKB.common import LoggableObject


class Client(LoggableObject):
    """ Interface class for external and internal DKB storage clients. """

    # Storage client
    c = None

    def __init__(self):
        """ Initialize Storage object. """
        raise NotImplementedError

    def configure(self, cfg):
        """ Apply storage configuration (initialize client).

        :param cfg: configuration parameters
        :type cfg: dict
        """
        raise NotImplementedError

    def get(self, id, **kwargs):
        """ Get object / record from storage by ID.

        Raise ``NotFound`` exception if object / record not found.

        :param id: object / record identfier
        :type id: str, int

        :return: record with given ID
        :rtype: dict
        """
        raise NotImplementedError
