#!/usr/bin/env python

"""
Tests for Stage 016's function add_es_index_info().
Usage: 'python -m unittest discover' from ..(directory with Stage 016 code).
"""

import unittest
import task2es


class Case(unittest.TestCase):
    def test_wrong_type(self):
        data = 1
        self.assertEqual(task2es.add_es_index_info(data), False)

    def test_no_taskid(self):
        data = {}
        self.assertEqual(task2es.add_es_index_info(data), False)

    def test_normal(self):
        data = {'taskid': '123'}
        self.assertEqual(task2es.add_es_index_info(data), True)
        self.assertEqual(data['_id'], data['taskid'])
        self.assertEqual(data['_type'], 'task')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(Case))
    return suite
