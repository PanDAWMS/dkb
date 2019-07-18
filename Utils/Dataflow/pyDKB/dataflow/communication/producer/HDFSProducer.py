"""
pyDKB.dataflow.communication.producer.HDFSProducer

Data producer implementation for common (static) files in HDFS.

TODO: think about:
      * pipes (better, from the point of StreamProducer)
      * multiple parallel dests
      * ...
"""

import os
import time

from FileProducer import FileProducer
from pyDKB.common import hdfs


class HDFSProducer(FileProducer):
    """ Data producer implementation for HDFS data dest. """

    def config_dir(self, config={}):
        """ Configure output directory. """
        if not config:
            config = self.config
        conf_dir = config.get('output_dir', '')
        if hdfs.path.isabs(conf_dir):
            self.dirname(conf_dir)
        else:
            self.log("Output directory is set to subdirectory '%s' of the one"
                     " containing input files or of the '%s'"
                     % (config.get('output_dir'), self.default_dir()))

    def set_default_dir(self):
        """ Set default directory name. """
        self._default_dir = hdfs.join(hdfs.DKB_HOME, 'temp',
                                      str(int(time.time())))

    def subdir(self, base_dir, sub_dir=''):
        """ Construct full path for $sub_dir of $base_dir. """
        return hdfs.join(base_dir, sub_dir)

    def ensure_dir(self):
        """ Ensure that current directory for output files exists. """
        path = self.get_dir()
        hdfs.makedirs(path)
        return path

    def file_info(self):
        """ Return output file metadata (name, directory, ...). """
        f = super(HDFSProducer, self).file_info()
        f['local_path'] = f['name']
        f['hdfs_path'] = hdfs.join(f['dir'], f['name'])
        return f

    def close_file(self):
        """ Close current file and move it to HDFS. """
        super(HDFSProducer, self).close_file()
        f = self.current_file
        if f:
            l_path = f.get('local_path')
            h_path = f.get('hdfs_path')
            if l_path and h_path and os.path.exists(l_path):
                hdfs.movefile(l_path, h_path)
