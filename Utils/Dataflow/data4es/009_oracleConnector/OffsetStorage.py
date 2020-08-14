"""
Classes representing Offset Storage.
Provides methods:
- init (create and open the storage)
- get (get current offset)
- commit (write new offset)
"""

import os.path
import sys


class OffsetStorage(object):
    """ Interface class for Offset Storage. """
    def __init__(self):
        """ Create and open storage. """
        raise NotImplementedError

    def get(self):
        """ Get current offset. """
        raise NotImplementedError

    def commit(self, new_offset):
        """ Save new offset. """
        raise NotImplementedError


class FileOffsetStorage(OffsetStorage):
    """ Offset Storage based on local file. """

    storage = None
    current = None

    def __init__(self, filename):
        """ Create and open storage. """
        try:
            if os.path.exists(filename):
                self.storage = open(filename, 'r+', 0)
            else:
                self.storage = open(filename, 'w+', 0)
        except IOError:
            sys.stderr.write("(ERROR) Failed to initialize offset storage"
                             " (%s)\n" % self.__class__.__name__)
            raise

    def get(self):
        """ Get current offset. """
        if self.current is None:
            cur = self.storage.readline().strip()
            if cur == '':
                cur = None
            self.current = cur
        return self.current

    def commit(self, new_offset):
        """ Save new offset. """
        self.storage.seek(0)
        self.storage.truncate()
        self.storage.write(str(new_offset))
        self.current = new_offset
