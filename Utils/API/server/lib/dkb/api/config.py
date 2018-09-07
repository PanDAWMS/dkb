"""
Module for handling configuration files.
"""

from exceptions import DkbApiNotImplemented, ConfigurationNotFound
from . import CONFIG_DIR

from storages.es import STORAGE_NAME as ES


def read_config(cfg_type, cfg_name):
    """ Read configuration file for given type and name.

    Raise ``ConfigurationNotFound`` if config file does not exist.

    :param cfg_type: type of config file ('storage', ...)
    :type cfg_type: str
    :param cfg_name: name of object to be configured
    :type cfg_name: str

    :return: confoguration parameters
    :rtype: hash
    """
    if (cfg_type, cfg_name) == ('storage', ES):
        hosts = '%%ES_ADDR%%'.split(',')
        return {'hosts': hosts, 'user': '%%ES_USER%%',
                'passwd': '%%ES_PASSWD%%'}
    raise DkbApiNotImplemented
