#!/usr/bin/env python

"""
Tests for Stage 009 function join_results().
Usage: 'python -m unittest discover' from stage directory.
"""

import unittest
import Oracle2JSON


class Case(unittest.TestCase):
    def test_equal_buffers(self):
        buf1 = [{'taskid': 1, 'info': 'Task 1'},
                {'taskid': 2, 'info': 'Task 2'},
                {'taskid': 3, 'info': 'Task 3'}]
        buf2 = [{'taskid': 1, 'ds': 'Dataset 1'},
                {'taskid': 2, 'ds': 'Dataset 2'},
                {'taskid': 3, 'ds': 'Dataset 3'}]
        exp = [{'taskid': 1, 'info': 'Task 1', 'ds': 'Dataset 1'},
               {'taskid': 2, 'info': 'Task 2', 'ds': 'Dataset 2'},
               {'taskid': 3, 'info': 'Task 3', 'ds': 'Dataset 3'}]
        result = []
        for r in Oracle2JSON.join_results(buf1, buf2):
            result += [r]
        self.assertEqual(result, exp)

    def test_first_short(self):
        buf1 = [{'taskid': 2, 'info': 'Task 2'},
                {'taskid': 3, 'info': 'Task 3'}]
        buf2 = [{'taskid': 1, 'ds': 'Dataset 1'},
                {'taskid': 2, 'ds': 'Dataset 2'},
                {'taskid': 3, 'ds': 'Dataset 3'}]
        exp = [{'taskid': 2, 'info': 'Task 2', 'ds': 'Dataset 2'},
               {'taskid': 3, 'info': 'Task 3', 'ds': 'Dataset 3'},
               {'taskid': 1, 'ds': 'Dataset 1'}]
        result = []
        for r in Oracle2JSON.join_results(buf1, buf2):
            result += [r]
        self.assertEqual(result, exp)

    def test_second_short(self):
        buf1 = [{'taskid': 1, 'info': 'Task 1'},
                {'taskid': 2, 'info': 'Task 2'},
                {'taskid': 3, 'info': 'Task 3'}]
        buf2 = [{'taskid': 1, 'ds': 'Dataset 1'},
                {'taskid': 3, 'ds': 'Dataset 3'}]
        exp = [{'taskid': 1, 'info': 'Task 1', 'ds': 'Dataset 1'},
               {'taskid': 3, 'info': 'Task 3', 'ds': 'Dataset 3'},
               {'taskid': 2, 'info': 'Task 2'}]
        result = []
        for r in Oracle2JSON.join_results(buf1, buf2):
            result += [r]
        self.assertEqual(result, exp)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(Case))
    return suite
