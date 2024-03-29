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

from . import Consumer
from .Consumer import ConsumerException
from pyDKB.common.types import logLevel
from .. import Message


class FileConsumer(Consumer):
    """ Data consumer implementation for HDFS data source. """

    # Current file
    current_file = None

    # Override
    def reconfigure(self, config={}):
        """ (Re)initialize consumer with Stage configuration. """
        if not config:
            config = self.config

        if not (config.get('input_files', None)
                or config.get('input_dir', None)):
            raise ConsumerException("No input files specified.")

        if not self.config.get('input_dir'):
            self.config['input_dir'] = os.path.curdir
        self.input_files = None

        super(FileConsumer, self).reconfigure(config)

    def source_is_readable(self):
        """ Check if current source is readable.

        :returns: None  -- no source,
                  False -- source is empty / fully read,
                  True  -- source is defined and is not empty
        :rtype: bool, NoneType
        """
        result = None
        fd = self.current_file['fd'] if self.current_file else None
        if self._stream and self._stream.get_fd() == fd:
            result = self.stream_is_readable()
        if fd and result is None:
            # check file directly only when there's no stream bound to it
            stat = os.fstat(fd.fileno())
            result = fd.tell() != stat.st_size
        return result

    def get_source_info(self):
        """ Return current source info. """
        return self.current_file

    def init_sources(self):
        """ Initialize sources iterator if not initialized yet. """
        if not self.input_files:
            self.input_files = self._input_files()

    def get_source(self):
        """ Get nearest non-empty source (current or next). """
        result = None
        if self.source_is_readable() or self.next_source():
            result = self.current_file['fd']
        return result

    def next_source(self):
        """ Reset $current_file to the next non-empty file.

        Return value:
            File descriptor of the new $current_file
            None (no files left)
        """
        if not self.input_files:
            self.init_sources()
        try:
            self.current_file = next(self.input_files)
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
            dir_content = sorted(os.listdir(dirname))
            # Make files order predictable
            for f in dir_content:
                if os.path.isfile(os.path.join(dirname, f)) \
                        and f.lower().endswith(ext):
                    files.append(f)
        except OSError as err:
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
