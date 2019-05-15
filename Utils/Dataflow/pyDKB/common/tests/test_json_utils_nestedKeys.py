#!/usr/bin/env python

'''
Tests for pyDKB.common.json_utils.nestedKeys().
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
    def test_list(self):
        key = [1, 2, 3]
        self.assertEqual(pyDKB.common.json_utils.nestedKeys(key), key)

    def test_simple(self):
        key = '1.2.3'
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ['1', '2', '3']
        self.assertEqual(result_function, result_known)

    def test_quotes(self):
        key = "1.'2'.3"
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ['1', '2', '3']
        self.assertEqual(result_function, result_known)

    def test_quotes_dot(self):
        key = "1.'2.3'.4"
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ["1", "2.3", "4"]
        self.assertEqual(result_function, result_known)

    def test_double_quotes(self):
        key = '1."2".3'
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ['1', '2', '3']
        self.assertEqual(result_function, result_known)

    def test_double_quotes_dot(self):
        key = '1."2.3".4'
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ['1', '2.3', '4']
        self.assertEqual(result_function, result_known)

    def test_quotes_inside(self):
        key = "1.2'3'4.5"
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ["1", "2'3'4", "5"]
        self.assertEqual(result_function, result_known)

    def test_double_quotes_inside(self):
        key = '1.2"3"4.5'
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ['1', '2"3"4', '5']
        self.assertEqual(result_function, result_known)

    def test_quoted_first_key(self):
        key = "'1.2'.3"
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ["1.2", "3"]
        self.assertEqual(result_function, result_known)

    def test_quoted_last_key(self):
        key = "1.'2.3'"
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ["1", "2.3"]
        self.assertEqual(result_function, result_known)

    def test_double_quoted_first_key(self):
        key = '"1.2".3'
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ['1.2', '3']
        self.assertEqual(result_function, result_known)

    def test_double_quoted_last_key(self):
        key = '1."2.3"'
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ['1', '2.3']
        self.assertEqual(result_function, result_known)

    def test_quotes_inside(self):
        key = "1.2'3'4.5"
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ["1", "2'3'4", "5"]
        self.assertEqual(result_function, result_known)

    def test_quoted_key_in_key(self):
        key = '1."2.\'3\'.4".5'
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ['1', '2.\'3\'.4', '5']
        self.assertEqual(result_function, result_known)

    def test_double_quoted_key_in_key(self):
        key = '1."2.\'3\'.4".5'
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ['1', '2.\'3\'.4', '5']
        self.assertEqual(result_function, result_known)

    def test_wrong_quote(self):
        key = "1.'2"
        with self.assertRaises(ValueError):
            result_function = pyDKB.common.json_utils.nestedKeys(key)

    def test_wrong_double_quote(self):
        key = '1."2'
        with self.assertRaises(ValueError):
            result_function = pyDKB.common.json_utils.nestedKeys(key)

    def test_wrong_different_quotes(self):
        key = '1."2.3\'.4'
        with self.assertRaises(ValueError):
            result_function = pyDKB.common.json_utils.nestedKeys(key)

    def test_empty(self):
        # It is impossible to get empty list [] if key is a string.
        key = ''
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ['']
        self.assertEqual(result_function, result_known)

    def test_dots(self):
        key = '..'
        result_function = pyDKB.common.json_utils.nestedKeys(key)
        result_known = ['', '', '']
        self.assertEqual(result_function, result_known)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(Case))
    return suite
