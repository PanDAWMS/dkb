"""
pyDKB.dataflow.communication.consumer.HDFSConsumer
"""

import FileConsumer
import Consumer
from pyDKB.common import hdfs
from pyDKB.common import HDFSException


class HDFSConsumer(FileConsumer.FileConsumer):
    """ Data consumer implementation for HDFS data source. """

    # Override
    def reconfigure(self, config={}):
        """ Configure HDFS Consumer according to the config parameters. """
        if not config:
            config = self.config

        if not config.get('input_dir'):
            config['input_dir'] = hdfs.DKB_HOME

        super(HDFSConsumer, self).reconfigure(config)

    # Override
    def _filenames(self):
        """ Return iterable object with filenames, taken from input. """
        if self.config.get('mode') == 'm':
            if self.config.get('input_files'):
                self.log("Input file names are ignored in MapReduce mode.")
                del self.config['input_files']
            files = self._filenames_from_stdin()
        else:
            files = super(HDFSConsumer, self)._filenames()
        return files

    # Override
    def _filenames_from_dir(self, dirname):
        """ Return list of files in given HDFS directory.

        Raises pyDKB.common.HDFSException
        """
        try:
            files = hdfs.listdir(dirname, "f")
            # Make files order predictable
            files.sort()
        except HDFSException, err:
            raise Consumer.ConsumerException(err)
        return files

    # Override
    def _adjusted_filenames(self):
        """ Return iterable object, yielding filename and path to file. """
        for f in self._filenames():
            d = {}
            d['name'] = hdfs.basename(f)
            d['dir'] = hdfs.join(self.config.get('input_dir', '').strip(),
                                 hdfs.dirname(f))
            d['full_path'] = hdfs.join(d['dir'], d['name'])
            yield d

    # Override
    def _input_files(self):
        """ Return iterable object, yielding dict with open file metadata.

        Metadata include:
          * fd        -- open file descriptor
          * name      -- file name
          * dir       -- HDFS directory name
          * full_path -- full path to the file in HDFS
        """
        input_files = self._adjusted_filenames()
        for f in input_files:
            with hdfs.File(f['full_path']) as f['fd']:
                yield f
