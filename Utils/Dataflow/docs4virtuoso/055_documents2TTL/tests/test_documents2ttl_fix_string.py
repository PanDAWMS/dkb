#!/usr/bin/env python

"""
Tests for Stage 055's function fix_string().
Usage: 'python -m unittest discover' from ..(directory with Stage 055 code).
"""

import unittest
import documents2ttl


class Case(unittest.TestCase):
    def test_wrong_type(self):
        s = 1
        self.assertEqual(documents2ttl.fix_string(s), s)

    def test_backslash_n(self):
        s = "\n"
        fixed_s = r"\\n"
        self.assertEqual(documents2ttl.fix_string(s), fixed_s)

    def test_single_quote(self):
        s = "'"
        fixed_s = r"\\'"
        self.assertEqual(documents2ttl.fix_string(s), fixed_s)

    def test_backslash_double_quote(self):
        s = "\""
        fixed_s = ""
        self.assertEqual(documents2ttl.fix_string(s), fixed_s)

    def test_string_without_characters_to_escape(self):
        s = "Am I supposed to write something _important_/*smart* here?"\
            "Preposterous!"
        self.assertEqual(documents2ttl.fix_string(s), s)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(Case))
    return suite
