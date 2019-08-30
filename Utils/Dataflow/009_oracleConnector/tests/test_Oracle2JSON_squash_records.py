#!/usr/bin/env python

"""
Tests for Stage 009 function squash_records().
Usage: 'python -m unittest discover' from stage directory.
"""

import unittest
import Oracle2JSON


class Case(unittest.TestCase):
    def test_single_entry(self):
        buf = [{'taskid': 1, 'ds': 'Dataset 1'},
               {'taskid': 2, 'ds': 'Dataset 2'},
               {'taskid': 3, 'ds': 'Dataset 3'}]
        exp = list(buf)
        result = []
        for r in Oracle2JSON.squash_records(buf):
            result += [r]
        self.assertEqual(result, exp)

    def test_multiple_entry(self):
        buf = [{'taskid': 1, 'ds': 'Dataset 1'},
               {'taskid': 2, 'ds': 'Dataset 2 (1)'},
               {'taskid': 2, 'ds': 'Dataset 2 (2)'},
               {'taskid': 2, 'ds': 'Dataset 2 (3)'},
               {'taskid': 3, 'ds': 'Dataset 3'}]
        exp = [{'taskid': 1, 'ds': 'Dataset 1'},
               {'taskid': 2, 'ds': ['Dataset 2 (1)', 'Dataset 2 (2)', 'Dataset 2 (3)']},
               {'taskid': 3, 'ds': 'Dataset 3'}]
        result = []
        for r in Oracle2JSON.squash_records(buf):
            result += [r]
        self.assertEqual(result, exp)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(Case))
    return suite
