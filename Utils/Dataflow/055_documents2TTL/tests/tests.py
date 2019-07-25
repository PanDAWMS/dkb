#!/usr/bin/env python

"""
Sample tests for Stage 055 to be used as example for writing tests.
Usage: 'python -m unittest discover' from ..(directory with Stage 055 code).
"""

import unittest
import documents2ttl


class SimpleTestCase(unittest.TestCase):
    def test_get_document_iri(self):
        doc_id = "abcd"
        iri = "<%s/document/%s>" % (documents2ttl.GRAPH, doc_id)
        self.assertEqual(documents2ttl.get_document_iri(doc_id), iri)

    def test_documents_links_manual(self):
        '''
        This test contains no calls of other functions or variables,
        everything is defined explicitly. If underlying functions or variables
        are changed, this test has to be changed as well. This approach makes
        sense when calling underlying functions should be avoided - for
        example, they are too time-consuming. Mocking can be used here.
        '''
        data = {"dkbID": "0", "supporting_notes": [{"dkbID": "1"},
                                                   {"dkbID": "2"}]}
        result = "<http://nosql.tpu.ru:8890/DAV/ATLAS/document/0> "\
                 "<http://nosql.tpu.ru/ontology/ATLAS#isBasedOn> "\
                 "<http://nosql.tpu.ru:8890/DAV/ATLAS/document/1> .\n"\
                 "<http://nosql.tpu.ru:8890/DAV/ATLAS/document/0> "\
                 "<http://nosql.tpu.ru/ontology/ATLAS#isBasedOn> "\
                 "<http://nosql.tpu.ru:8890/DAV/ATLAS/document/2> .\n"
        self.assertEqual(documents2ttl.documents_links(data), result)

    def test_documents_links_constructed(self):
        '''
        This test uses functions and variables and simulates the function
        in normal situation. This approach should be used in most cases as it
        may discover some errors which cannot be detected by testing parts of
        the function separately.
        '''
        id0 = "2"
        ids = ["8", "3", "6"]
        data = {"dkbID": id0, "supporting_notes": [{"dkbID": i} for i in ids]}
        result = ""
        iri0 = documents2ttl.get_document_iri(id0)
        for note in data["supporting_notes"]:
            iri = documents2ttl.get_document_iri(note["dkbID"])
            result += "%s <%s#isBasedOn> %s .\n" % (iri0,
                                                    documents2ttl.ONTOLOGY,
                                                    iri)
        self.assertEqual(documents2ttl.documents_links(data), result)


class arxiv_extractionTestCase(unittest.TestCase):
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


class generate_journal_idTestCase(unittest.TestCase):
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


class fix_stringTestCase(unittest.TestCase):
    def test_wrong_type(self):
        s = 1
        self.assertEqual(documents2ttl.fix_string(s), s)

    def test_backslash_n(self):
        s = "\n"
        fixed_s = "\\\\n"
        self.assertEqual(documents2ttl.fix_string(s), fixed_s)

    def test_single_quote(self):
        s = "'"
        fixed_s = "\\\\'"
        self.assertEqual(documents2ttl.fix_string(s), fixed_s)

    def test_single_backslash_single_quote(self):
        s = "\'"
        fixed_s = "\\\\'"
        self.assertEqual(documents2ttl.fix_string(s), fixed_s)

    def test_backslash_double_quote(self):
        # Is it normal that this and previous tests are so different?
        s = "\""
        fixed_s = ""
        self.assertEqual(documents2ttl.fix_string(s), fixed_s)

    def test_string_without_characters_to_escape(self):
        s = "Am I supposed to write something _important_/*smart* here?"\
            "Preposterous!"
        self.assertEqual(documents2ttl.fix_string(s), s)


test_cases = (
    SimpleTestCase,
    arxiv_extractionTestCase,
    generate_journal_idTestCase,
    fix_stringTestCase
    )


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for case in test_cases:
        suite.addTest(loader.loadTestsFromTestCase(case))
    return suite
