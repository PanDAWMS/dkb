"""
pyDKB.dataflow.communication.producer.FileProducer

Data producer implementation for common (static) files.

TODO: think about:
      * pipes (better, from the point of StreamProducer)
      * multiple parallel dests
      * ...
"""

import os
import time

from Producer import Producer, ProducerException
from . import logLevel


class FileProducer(Producer):
    """ Data producer implementation for local file data dest. """

    _dir = None
    _default_dir = None
    current_file = None

    def reconfigure(self, config={}):
        """ (Re)configure producer according to the config hash. """
        if not config:
            config = self.config
        self.config_dir(config)
        super(FileProducer, self).reconfigure(config)

    def config_dir(self, config={}):
        """ Configure output directory.  """
        if not config:
            config = self.config
        conf_dir = config.get('output_dir', '')
        cur_dir = os.path.curdir
        par_dir = os.path.pardir
        if conf_dir in (base_dir, par_dir) or \
                os.path.isabs(conf_dir) or \
                conf_dir.startswith(os.path.join(base_dir, '')) or \
                conf_dir.startswith(os.path.join(par_dir, '')):
            # If configured path is specified as a full path
            # (or relative to the current or parent directory)
            # it is preferable; else source directory will be used
            config['output_dir'] = os.path.realpath(conf_dir)
            self.dirname(config['output_dir'])
        elif conf_dir:
            self.log("Output directory is set to subdirectory '%s' of the one"
                     " containing input files or of the current one (%s)."
                     % (config['output_dir'], self.default_dir()))
        else:
            self.log("Output directory is set to the directory with input"
                     " files or to the current one (%s)."
                     % self.default_dir())

    def get_dest(self):
        """ Get destination file descriptor. """
        self.reset_file()
        return self.current_file['fd']

    def get_dest_info(self):
        """ Get current destination info. """
        return self.current_file

    def _source_info(self):
        """ Get information about current source. """
        raise NotImplementedError

    def get_source_info(self):
        """ Set current data source, if any. """
        try:
            source = self._source_info()
        except NotImplementedError:
            source = {}
        return source

    def set_default_dir(self):
        """ Set default directory name. """
        self._default_dir = os.getcwd()

    def default_dir(self):
        """ Get default directory name. """
        if not self._default_dir:
            self.set_default_dir()
        return self._default_dir

    def dirname(self, dirname=None):
        """ Set/get preferable directory name. """
        if dirname is not None:
            self._dir = dirname
            self.log("Output directory is set to: %s" % dirname)
        return self._dir

    def subdir(self, base_dir, sub_dir=''):
        """ Construct full path for $subdir of $base_dir. """
        return os.path.join(base_dir, sub_dir)

    def get_dir(self):
        """ Get current directory for output files. """
        result = self.dirname()
        if not result:
            base_dir = None
            src = self.get_source_info()
            if src is not None:
                base_dir = src.get('dir', '')
            if not base_dir:
                base_dir = self.default_dir()
            result = self.subdir(base_dir,
                                 self.config.get('output_dir', ''))
        return result

    def ensure_dir(self):
        """ Ensure that current directory for output files exists. """
        path = self.get_dir()
        if not os.path.isdir(path):
            try:
                os.makedirs(path, 0770)
            except OSError, err:
                self.log("Failed to create output directory\n"
                         "Error message: %s\n" % err, logLevel.ERROR)
                raise ProducerException
        return path

    def get_filename(self):
        """ Return filename, corresponding the source, or timestamp-based. """
        ext = ''
        msgClass = self.message_class()
        if msgClass:
            ext = msgClass.extension()
        src = self.get_source_info()
        if src and src.get('name'):
            src_name = src['name']
            result = os.path.splitext(src_name)[0] + ext
        else:
            result = str(int(time.time())) + ext
        return result

    def file_info(self):
        """ Return output file metadata (name, directory, ...). """
        f = {}
        f['src'] = self.get_source_info()
        f['dir'] = self.get_dir()
        f['name'] = self.get_filename()
        f['local_path'] = os.path.join(f['dir'], f['name'])
        return f

    def reset_file(self):
        """ Resets current file according to the current source info.

        Metadata include:
          * fd         -- open file descriptor
          * name       -- file name
          * dir        -- directory name
          * local_path -- local path to the file
        """
        prev = self.current_file
        cur = self.file_info()
        if prev and prev.get('fd') and (
                prev.get('src') == cur['src']
                or (os.path.abspath(prev.get('local_path', ''))
                    == os.path.abspath(cur['local_path']))):
            # Save previous open file descriptor, if:
            # * data source has not changed OR
            # * new local output file is the same as before.
            cur['fd'] = prev['fd']
            del prev['fd']
        elif os.path.exists(cur['local_path']):
            raise ProducerException("File already exists: %s"
                                    % cur['local_path'])
        else:
            self.close_file()
            self.ensure_dir()
            cur['fd'] = open(cur['local_path'], 'w', 0)
        self.current_file = cur
        return cur

    def close_file(self):
        """ Close current file. """
        f = self.current_file
        if f and f.get('fd'):
            f['fd'].close()
            del f['fd']

    def close(self):
        """ Close opened files and remove temporary one. """
        super(FileProducer, self).close()
        self.close_file()
