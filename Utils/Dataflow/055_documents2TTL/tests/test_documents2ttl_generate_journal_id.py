#!/usr/bin/env python

"""
Tests for Stage 055's function generate_journal_id().
Usage: 'python -m unittest discover' from ..(directory with Stage 055 code).
"""

import unittest
import documents2ttl


class Case(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(documents2ttl.generate_journal_id({}), '')

    def test_title(self):
        journal_dict = {'title': 'T I T L E\n'}
        result_function = documents2ttl.generate_journal_id(journal_dict)
        result_known = 'TITLE\n'
        self.assertEqual(result_function, result_known)

    def test_volume(self):
        journal_dict = {'volume': 'o ne \n'}
        result_function = documents2ttl.generate_journal_id(journal_dict)
        result_known = '_one\n'
        self.assertEqual(result_function, result_known)

    def test_year(self):
        journal_dict = {'year': ' 2 0 1 8 \n'}
        result_function = documents2ttl.generate_journal_id(journal_dict)
        result_known = '_2018\n'
        self.assertEqual(result_function, result_known)

    def test_full(self):
        journal_dict = {'year': '2018 ', 'title': ' TITLE', 'volume': '1'}
        result_function = documents2ttl.generate_journal_id(journal_dict)
        result_known = 'TITLE_1_2018'
        self.assertEqual(result_function, result_known)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(Case))
    return suite
