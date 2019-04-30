#!/usr/bin/env python

"""
Tests for pyDKB.dataflow.stage.ProcessorStage.configure()'s config argument.
Usage: 'python -m unittest discover' from ..
(directory with pyDKB.dataflow.stage code).
"""

import os
import sys
import tempfile
import unittest

from lib import isolate_function_error

# Relative import inside of pyDKB prevents the use of simple 'import pyDKB'.
try:
    base_dir = os.path.dirname(__file__)  # Directory with this file
    dkb_dir = os.path.join(base_dir, os.pardir)  # stage directory
    dkb_dir = os.path.join(dkb_dir, os.pardir)  # dataflow directory
    dkb_dir = os.path.join(dkb_dir, os.pardir)  # pyDKB's directory
    dkb_dir = os.path.join(dkb_dir, os.pardir)  # pyDKB's parent directory
    sys.path.append(dkb_dir)
    import pyDKB
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


class Case(unittest.TestCase):
    def setUp(self):
        self.stage = pyDKB.dataflow.stage.ProcessorStage()
        self.fake_config = tempfile.NamedTemporaryFile(dir='.')

    def tearDown(self):
        self.stage = None
        if not self.fake_config.closed:
            self.fake_config.close()
        self.fake_config = None

    def test_correct_c(self):
        self.stage.configure(['-c', self.fake_config.name, 'something'])
        isfile = isinstance(getattr(self.stage.ARGS, 'config'), file)
        self.assertTrue(isfile)

    def test_correct_config(self):
        self.stage.configure(['--config', self.fake_config.name, 'something'])
        isfile = isinstance(getattr(self.stage.ARGS, 'config'), file)
        self.assertTrue(isfile)

    def test_missing_c(self):
        self.fake_config.close()
        args = ['-c', self.fake_config.name, 'something']
        [msg, result] = isolate_function_error(self.stage.configure, args)
        err = "[Errno 2] No such file or directory: '%s'" %\
              self.fake_config.name
        self.assertIn(err, msg)

    def test_missing_config(self):
        self.fake_config.close()
        args = ['--config', self.fake_config.name, 'something']
        [msg, result] = isolate_function_error(self.stage.configure, args)
        err = "[Errno 2] No such file or directory: '%s'" %\
              self.fake_config.name
        self.assertIn(err, msg)

    def test_unreadable_c(self):
        os.chmod(self.fake_config.name, 0300)
        args = ['-c', self.fake_config.name, 'something']
        [msg, result] = isolate_function_error(self.stage.configure, args)
        err = "[Errno 13] Permission denied: '%s'" %\
              self.fake_config.name
        self.assertIn(err, msg)

    def test_unreadable_config(self):
        os.chmod(self.fake_config.name, 0300)
        args = ['--config', self.fake_config.name, 'something']
        [msg, result] = isolate_function_error(self.stage.configure, args)
        err = "[Errno 13] Permission denied: '%s'" %\
              self.fake_config.name
        self.assertIn(err, msg)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(Case))
    return suite
