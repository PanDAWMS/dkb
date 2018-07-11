"""
pyDKB.dataflow.communication.consumer.FileConsumer

Data consumer implementation for common (static) files.

TODO: think about:
      * updatable files
      * pipes (better, from the point of StreamConsumer)
      * round-robin (for updatable sources)
      * ...
"""

import sys
import os

import Consumer
from . import DataflowException
from . import logLevel
from .. import Message


class FileConsumer(Consumer.Consumer):
    """ Data consumer implementation for HDFS data source. """

    # Current file
    current_file = None

    # Override
    def reconfigure(self, config={}):
        """ (Re)initialize consumer with Stage configuration. """
        if not config:
            config = self.config

        if not self.config.get('input_dir'):
            self.config['input_dir'] = os.path.curdir
        self.input_files = None

        super(FileConsumer, self).reconfigure(config)

    def source_is_empty(self):
        """ Check if current source is empty.

        Return value:
            True  (empty)
            False (not empty)
            None  (no source)
        """
        f = self.current_file
        if not f:
            return None
        fd = f['fd']
        if not f.get('size'):
            stat = os.fstat(fd.fileno())
            f['size'] = stat.st_size
        return fd.tell() == f['size']

    def get_source_info(self):
        """ Return current source info. """
        return self.current_file

    def get_source(self):
        """ Get nearest non-empty source (current or next). """
        if self.source_is_empty() is not False:
            result = self.next_source()
        else:
            result = self.current_file['fd']
        return result

    def next_source(self):
        """ Reset $current_file to the next non-empty file.

        Return value:
            File descriptor of the new $current_file
            None (no files left)
        """
        if not self.input_files:
            self.input_files = self._input_files()
        try:
            self.current_file = self.input_files.next()
            result = self.get_source()
        except StopIteration:
            self.current_file = None
            result = None
        return result

    def _filenames(self):
        """ Return iterable object with filenames, taken from input. """
        if self.config.get('input_files'):
            files = self.config['input_files']
        elif self.config.get('input_dir'):
            files = self._filenames_from_dir(self.config['input_dir'])
        else:
            self.log("No input files configured; reading filenames from"
                     " STDIN.", logLevel.WARN)
            files = self._filenames_from_stdin()
        return files

    def _filenames_from_stdin(self):
        """ Return iterable object, yielding filenames read from STDIN. """
        return iter(sys.stdin.readline, "")

    def _filenames_from_dir(self, dirname):
        """ Return list of files in given local directory. """
        files = []
        ext = Message(self.message_type).extension()
        try:
            dir_content = os.listdir(dirname)
            for f in dir_content:
                if os.path.isfile(os.path.join(dirname, f)) \
                        and f.lower().endswith(ext):
                    files.append(f)
        except OSError, err:
            raise Consumer.ConsumerException(err)
        return files

    def _adjusted_filenames(self):
        """ Return iterable object, yielding filename and path to file. """
        for f in self._filenames():
            d = {}
            d['name'] = os.path.basename(f).strip()
            d['dir'] = os.path.join(self.config.get('input_dir', '').strip(),
                                    os.path.dirname(f).strip())
            d['full_path'] = os.path.join(d['dir'], d['name'])
            yield d

    def _input_files(self):
        """ Return iterable object, yielding dict with open file metadata.

        Metadata include:
          * fd        -- open file descriptor
          * name      -- file name
          * dir       -- directory name
          * full_path -- full path to the file
        """
        input_files = self._adjusted_filenames()
        for f in input_files:
            with open(f['full_path'], 'r') as f['fd']:
                yield f
