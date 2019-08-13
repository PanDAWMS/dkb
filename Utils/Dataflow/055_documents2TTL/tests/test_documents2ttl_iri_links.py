#!/usr/bin/env python

"""
Tests for Stage 055's functions document_iri() and document_links().
Usage: 'python -m unittest discover' from ..(directory with Stage 055 code).
"""

import unittest
import documents2ttl


class Case(unittest.TestCase):
    def test_get_document_iri(self):
        doc_id = "abcd"
        iri = "<%s/document/%s>" % (documents2ttl.GRAPH, doc_id)
        self.assertEqual(documents2ttl.get_document_iri(doc_id), iri)

    def test_documents_links(self):
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
