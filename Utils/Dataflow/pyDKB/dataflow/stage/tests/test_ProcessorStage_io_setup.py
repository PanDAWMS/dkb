#!/usr/bin/env python

"""
Tests for pyDKB.dataflow.stage.ProcessorStage.configure() that check the
input/output setup.
Usage: 'python -m unittest discover' from ..
(directory with pyDKB.dataflow.stage code).
"""

import os
import sys
import unittest

# Relative import inside of pyDKB prevents the use of simple 'import pyDKB'.
try:
    base_dir = os.path.dirname(__file__)  # Directory with this file
    dkb_dir = os.path.join(base_dir, os.pardir)  # stage directory
    dkb_dir = os.path.join(dkb_dir, os.pardir)  # dataflow directory
    dkb_dir = os.path.join(dkb_dir, os.pardir)  # pyDKB's directory
    dkb_dir = os.path.join(dkb_dir, os.pardir)  # pyDKB's parent directory
    sys.path.append(dkb_dir)
    import pyDKB
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


class Case(unittest.TestCase):
    def setUp(self):
        self.stage = pyDKB.dataflow.stage.ProcessorStage()

    def tearDown(self):
        self.stage = None

    def test_default(self):
        self.stage.configure(['something'])
        c_cons = isinstance(self.stage._ProcessorStage__input,
                            pyDKB.dataflow.communication.consumer.Consumer)
        c_prod = isinstance(self.stage._ProcessorStage__output,
                            pyDKB.dataflow.communication.producer.Producer)
        self.assertEqual(c_cons, True)
        self.assertEqual(c_prod, True)
        self.assertEqual(self.stage._ProcessorStage__stoppable,
                         [self.stage._ProcessorStage__input,
                          self.stage._ProcessorStage__output])

    def test_source_f(self):
        self.stage.configure(['-s', 'f', 'something'])
        c = isinstance(self.stage._ProcessorStage__input,
                       pyDKB.dataflow.communication.consumer.FileConsumer)
        self.assertEqual(c, True)

    def test_source_s(self):
        self.stage.configure(['-s', 's'])
        c = isinstance(self.stage._ProcessorStage__input,
                       pyDKB.dataflow.communication.consumer.StreamConsumer)
        self.assertEqual(c, True)

    def test_source_h(self):
        self.stage.configure(['-s', 'h'])
        c = isinstance(self.stage._ProcessorStage__input,
                       pyDKB.dataflow.communication.consumer.HDFSConsumer)
        self.assertEqual(c, True)

    def test_dest_f(self):
        self.stage.configure(['-d', 'f', 'something'])
        c = isinstance(self.stage._ProcessorStage__output,
                       pyDKB.dataflow.communication.producer.FileProducer)
        self.assertEqual(c, True)

    def test_dest_s(self):
        self.stage.configure(['-d', 's', 'something'])
        c = isinstance(self.stage._ProcessorStage__output,
                       pyDKB.dataflow.communication.producer.StreamProducer)
        self.assertEqual(c, True)

    def test_dest_h(self):
        self.stage.configure(['-d', 'h', 'something'])
        c = isinstance(self.stage._ProcessorStage__output,
                       pyDKB.dataflow.communication.producer.HDFSProducer)
        self.assertEqual(c, True)

    def test_hdfs(self):
        self.stage.configure(['--hdfs'])
        c1 = isinstance(self.stage._ProcessorStage__input,
                        pyDKB.dataflow.communication.consumer.HDFSConsumer)
        c2 = isinstance(self.stage._ProcessorStage__output,
                        pyDKB.dataflow.communication.producer.HDFSProducer)
        self.assertEqual(c1, True)
        self.assertEqual(c2, True)

    def test_mode_s(self):
        self.stage.configure(['--mode', 's'])
        c1 = isinstance(self.stage._ProcessorStage__input,
                        pyDKB.dataflow.communication.consumer.StreamConsumer)
        c2 = isinstance(self.stage._ProcessorStage__output,
                        pyDKB.dataflow.communication.producer.StreamProducer)
        self.assertEqual(c1, True)
        self.assertEqual(c2, True)

    def test_mode_f(self):
        self.stage.configure(['--mode', 'f', 'something'])
        c1 = isinstance(self.stage._ProcessorStage__input,
                        pyDKB.dataflow.communication.consumer.FileConsumer)
        c2 = isinstance(self.stage._ProcessorStage__output,
                        pyDKB.dataflow.communication.producer.FileProducer)
        self.assertEqual(c1, True)
        self.assertEqual(c2, True)

    def test_mode_m(self):
        self.stage.configure(['--mode', 'm'])
        c1 = isinstance(self.stage._ProcessorStage__input,
                        pyDKB.dataflow.communication.consumer.StreamConsumer)
        c2 = isinstance(self.stage._ProcessorStage__output,
                        pyDKB.dataflow.communication.producer.StreamProducer)
        self.assertEqual(c1, True)
        self.assertEqual(c2, True)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(Case))
    return suite
