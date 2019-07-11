"""
pyDKB.storages.client.es
"""

from Client import Client
from pyDKB.common.misc import try_to_import
from pyDKB.common.types import logLevel


_ESClient = try_to_import('elasticsearch', 'Elasticsearch')

ParentClientClass = _ESClient if _ESClient else object


class ESClient(Client, ParentClientClass):
    """ Implement common interface for ES client. """

    index = None

    def __init__(self, *args, **kwargs):
        """ Initialize instance as parent client class object. """
        ParentClientClass.__init__(self, *args, **kwargs)

    def configure(self, cfg):
        """ Apply configuration.

        Configuration parameters:
          hosts  (str)  -- comma separated list of 'host:port' records
          host   (str)  -- host name or IP (single) (ignored if hosts defined)
          port   (str)  -- host port (ignored if hosts defined)
          index  (str)  -- default index name
          user   (str)
          passwd (str)

        :param cfg: configuration parameters
        :type cfg: dict
        """
        kwargs = {}

        hosts = None
        host = {}
        if cfg.get('hosts'):
            hosts = [h.strip() for h in cfg['hosts'].split(',')]
        if cfg.get('host'):
            if cfg.get('hosts'):
                self.log("Configuration parameter ignored: 'host' ('hosts' "
                         "specified)")
            else:
                host['host'] = cfg['host']
        if cfg.get('port'):
            if cfg.get('hosts'):
                self.log("Configuration parameter ignored: 'port' ('hosts' "
                         "specified)")
            else:
                host['port'] = cfg['port']
        if hosts or host:
            kwargs['hosts'] = hosts if hosts else [host]

        if cfg.get('user'):
            auth = (cfg['user'], )
            if cfg.get('passwd'):
                auth += (cfg['passwd'], )
            else:
                self.log("Configuration parameter missed: 'passwd' ('user' "
                         "specified)", logLevel.WARN)
            kwargs['http_auth'] = auth
        elif cfg.get('passwd'):
            self.log("Configuration parameter ignored: 'passwd' ('user' "
                     "not specified)")

        if cfg.get('index'):
            self.index = cfg['index']

        # Re-initialize self as parent client class instance
        ParentClientClass.__init__(self, **kwargs)
