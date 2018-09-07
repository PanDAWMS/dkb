"""
Unittest for api.methods module.
"""

import unittest
from .. import methods
from ..exceptions import CategoryNotFound

class get_category_TestCase(unittest.TestCase):
    def setUp(self):
        methods.API_METHODS = {}

    def tearDown(self):
        methods.API_METHODS = {}

    def test_root_category(self):
        for path in ('/', '', '///'):
            self.assertIs(methods.get_category(path), methods.API_METHODS)

    def test_create_category(self):
        path = '/category'
        expect = {'category': {}}
        result = methods.get_category(path, True)
        self.assertEqual(result, {})
        self.assertEqual(methods.API_METHODS, expect)

    def test_create_categories(self):
        path = '/path/to/category'
        expect = {'path': {'to': {'category': {}}}}
        result = methods.get_category(path, True)
        self.assertEqual(result, {})
        self.assertEqual(methods.API_METHODS, expect)

    def test_create_trailing_slash(self):
        path = '/category/'
        expect = {'category': {}}
        result = methods.get_category(path, True)
        self.assertEqual(result, {})
        self.assertEqual(methods.API_METHODS, expect)

    def test_create_trailing_slashes(self):
        path = '/category////'
        expect = {'category': {}}
        result = methods.get_category(path, True)
        self.assertEqual(result, {})
        self.assertEqual(methods.API_METHODS, expect)

    def test_subcategory(self):
        path = '/path/to/category'
        methods.get_category(path, True)
        path = '/path/'
        expect = {'to': {'category': {}}}
        self.assertEqual(methods.get_category(path), expect)

    def test_create_existing_method(self):
        path = '/path/to/category'
        method = self.setUp
        methods.add(path, 'setUp', method)
        expect = {'setUp': {'': method}}
        self.assertEqual(methods.get_category(path + '/setUp', True), expect)

    def test_not_found_existing_method(self):
        path = '/path/to/category'
        method = self.setUp
        methods.add(path, 'setUp', method)
        self.assertRaises(CategoryNotFound, methods.get_category,
                          path + '/setUp')

    def test_not_found(self):
        path = '/path/to/category'
        self.assertRaises(CategoryNotFound, methods.get_category, path)


test_cases = (get_category_TestCase, )


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for case in test_cases:
        suite.addTest(loader.loadTestsFromTestCase(case))
    return suite
