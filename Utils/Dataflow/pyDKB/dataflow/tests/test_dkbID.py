#!/usr/bin/env python

'''
Tests for pyDKB.dataflow.dkbID.
Usage: 'python -m unittest discover' from .. .
'''


import os
import sys
import unittest


# Relative import inside of pyDKB prevents the use of simple 'import pyDKB'.
try:
    base_dir = os.path.dirname(__file__)  # Directory with tests.py
    dkb_dir = os.path.join(base_dir, os.pardir)  # pyDKB/common directory
    dkb_dir = os.path.join(dkb_dir, os.pardir)  # pyDKB directory
    dkb_dir = os.path.join(dkb_dir, os.pardir)  # pyDKB's parent directory
    sys.path.append(dkb_dir)
    import pyDKB
    # pyDKB.dataflow's __init__.py overwrites the name dkbID, so the functions
    # from this module must be imported explicitly.
    from pyDKB.dataflow.dkbID import firstValue
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


class Case(unittest.TestCase):
    def test_not_list(self):
        inp = "123"
        self.assertEqual(firstValue(inp), inp)

    def test_empty_list(self):
        inp = []
        self.assertEqual(firstValue(inp), None)

    def test_list(self):
        inp = [None, 1, None, 2]
        result_function = firstValue(inp)
        result_known = 1
        self.assertEqual(result_function, result_known)

    def test_list_in_list(self):
        inp = [[None, None], [None, 1], None, 2]
        result_function = firstValue(inp)
        result_known = 1
        self.assertEqual(result_function, result_known)

    def test_zero(self):
        inp = [0, 1]
        result_function = firstValue(inp)
        result_known = 0
        self.assertEqual(result_function, result_known)

    def test_false(self):
        inp = [False, True]
        result_function = firstValue(inp)
        result_known = False
        self.assertEqual(result_function, result_known)

    def test_empty_string(self):
        inp = ['', '1']
        result_function = firstValue(inp)
        result_known = '1'
        self.assertEqual(result_function, result_known)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(Case))
    return suite
