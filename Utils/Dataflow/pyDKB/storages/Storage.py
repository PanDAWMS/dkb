"""
pyDKB.storages.Storage
"""


class Storage(object):
    """ Interface class for external and internal DKB storages. """

    # Storage name (identifier)
    name = None

    # Storage type (storageType member)
    type = None

    # Storage client
    c = None

    def __init__(self, name):
        """ Initialize Storage object.

        :param name: storage identifier
        :type name: str
        """
        self.name = name

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
