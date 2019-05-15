#!/usr/bin/env python

'''
Tests for pyDKB.common.json_utils.valueByKey().
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
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


class Case(unittest.TestCase):
    def setUp(self):
        self.data = {'1': '2',
                     '3': [
                         {'4': '5'},
                         {'6': '7'}
                     ],
                     '8': {'9': '10'}}

    def tearDown(self):
        self.data = None

    def test_empty_key_list(self):
        keys = []
        result_function = pyDKB.common.json_utils.valueByKey(self.data, keys)
        result_known = self.data
        self.assertEqual(result_function, result_known)

    def test_str(self):
        keys = '1'
        result_function = pyDKB.common.json_utils.valueByKey(self.data, keys)
        result_known = '2'
        self.assertEqual(result_function, result_known)

    def test_wrong_key(self):
        keys = '2'
        result_function = pyDKB.common.json_utils.valueByKey(self.data, keys)
        result_known = None
        self.assertEqual(result_function, result_known)

    def test_list(self):
        keys = '3'
        result_function = pyDKB.common.json_utils.valueByKey(self.data, keys)
        result_known = [{'4': '5'}, {'6': '7'}]
        self.assertEqual(result_function, result_known)

    def test_list_str(self):
        keys = '3.4'
        result_function = pyDKB.common.json_utils.valueByKey(self.data, keys)
        result_known = ['5', None]
        self.assertEqual(result_function, result_known)

    def test_dict_str(self):
        keys = '8.9'
        result_function = pyDKB.common.json_utils.valueByKey(self.data, keys)
        result_known = '10'
        self.assertEqual(result_function, result_known)

    def test_too_many_keys(self):
        keys = '8.9.10.11.12'
        result_function = pyDKB.common.json_utils.valueByKey(self.data, keys)
        result_known = None
        self.assertEqual(result_function, result_known)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(Case))
    return suite
