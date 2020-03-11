"""
Module for handling configuration files.
"""

from exceptions import (DkbApiNotImplemented,
                        ConfigurationNotFound,
                        ConfigurationError)
from . import CONFIG_DIR

import os
import yaml


def get_config(cfg_type, cfg_name):
    """ Get configuration section corresponding type and name.

    :param cfg_type: type of config object ('storages', ...)
    :type cfg_type: str
    :param cfg_name: name of object to be configured
    :type cfg_name: str

    :return: confoguration parameters
    :rtype: hash
    """
    cfg = read_config('dkb.yaml')
    try:
        return cfg[cfg_type][cfg_name]
    except KeyError:
        raise ConfigurationError('failed to find section in configuration '
                                 'file: %r -> %r' % (cfg_type, cfg_name))


def read_config(fname):
    """ Read configuration file into dict.

    :raises: ConfigurationNotFound: config file does not exist.
    :raises: ConfigurationException: failed to read config file.

    :param fname: config file name
    :type fname: str

    :return: read configuration
    :rtype: dict
    """
    # If `fname` is an absolute path, `CONFIG_DIR` will be ignored
    full_path = os.path.join(CONFIG_DIR, fname)
    if not os.path.isfile(full_path):
        raise ConfigurationNotFound(full_path)
    with open(full_path) as f:
        try:
            parsed = yaml.load(f)
        except yaml.YAMLError, e:
            raise ConfigurationError('Format error: %s' % e)
    return parsed
