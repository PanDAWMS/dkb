#!/usr/bin/env python

'''
Tests for pyDKB.dataflow.communication.messages.
Usage: 'python -m unittest discover' from .. .
'''


import os
import sys
import unittest


# Relative import inside of pyDKB prevents the use of simple 'import pyDKB'.
try:
    base_dir = os.path.dirname(__file__)  # Directory with tests.py
    dkb_dir = os.path.join(base_dir, os.pardir)  # communication directory
    dkb_dir = os.path.join(dkb_dir, os.pardir)  # pyDKB/dataflow directory
    dkb_dir = os.path.join(dkb_dir, os.pardir)  # pyDKB directory
    dkb_dir = os.path.join(dkb_dir, os.pardir)  # pyDKB's parent directory
    sys.path.append(dkb_dir)
    import pyDKB
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


class Case(unittest.TestCase):
    def test_json(self):
        data = {'message': '123'}
        msg = pyDKB.dataflow.communication.messages.JSONMessage(data)
        result = msg.content()
        self.assertEqual(data, result)

    def test_ttl(self):
        data = {'message': '123'}
        msg = pyDKB.dataflow.communication.messages.TTLMessage(data)
        result = msg.content()
        self.assertEqual(data, result)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(Case))
    return suite
