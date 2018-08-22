"""
Module for handling configuration files.
"""

from exceptions import DkbApiNotImplemented, ConfigurationNotFound
from . import CONFIG_DIR


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
    raise DkbApiNotImplemented
