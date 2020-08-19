#!/usr/bin/env python

"""
Tests for Stage 055's function arxiv_extraction().
Usage: 'python -m unittest discover' from ..(directory with Stage 055 code).
"""

import unittest
import documents2ttl


class Case(unittest.TestCase):
    def test_empty(self):
        result_function = documents2ttl.arxiv_extraction({})
        result_known = None
        self.assertEqual(result_function, result_known)

    def test_wrong_number_type(self):
        data = {'primary_report_number': 1}
        result_function = documents2ttl.arxiv_extraction(data)
        result_known = None
        self.assertEqual(result_function, result_known)

    def test_string(self):
        data = {'primary_report_number': 'arXiv123'}
        result_function = documents2ttl.arxiv_extraction(data)
        result_known = 'arXiv123'
        self.assertEqual(result_function, result_known)

    def test_list(self):
        data = {'primary_report_number': [None, 'arXiv123', 'something']}
        result_function = documents2ttl.arxiv_extraction(data)
        result_known = 'arXiv123'
        self.assertEqual(result_function, result_known)

    def test_small_x(self):
        data = {'primary_report_number': 'arxiv123'}
        result_function = documents2ttl.arxiv_extraction(data)
        result_known = None
        self.assertEqual(result_function, result_known)

    def test_prefix(self):
        data = {'primary_report_number': '321arXiv123'}
        result_function = documents2ttl.arxiv_extraction(data)
        result_known = None
        self.assertEqual(result_function, result_known)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(Case))
    return suite
