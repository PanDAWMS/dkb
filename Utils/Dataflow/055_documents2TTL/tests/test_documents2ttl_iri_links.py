#!/usr/bin/env python

"""
Various tests for Stage 055.
Usage: 'python -m unittest discover' from ..(directory with Stage 055 code).
"""

import unittest
import documents2ttl


class Case(unittest.TestCase):
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


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(Case))
    return suite
