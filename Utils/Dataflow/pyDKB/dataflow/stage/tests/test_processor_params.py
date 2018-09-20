#!/usr/bin/env python

"""
Tests for Processor Stage parameters parsing and handing.

Default values, overwriting, etc.
"""

import os
import unittest

import pyDKB.dataflow.stage as stage


class dataflow_stage_ProcessorStageArgsTestCase(unittest.TestCase):

    stage_params = ['mode', 'config', 'eom', 'eop', 'source', 'dest', 'input_dir',
                    'output_dir', 'hdfs', 'input_files']
    param_defaults = {
        'mode': 'f',
        'config': None,
        'eom': '\n',
        'eop': '',
        'source': 'f',
        'dest': 'f',
        'input_dir': os.curdir,
        'output_dir': 'out',
        'hdfs': False,
        'input_files': []
    }

    mode_defaults = {
        'f': {'eom': '\n', 'eop': '', 'source': 'f', 'dest': 'f'},
        's': {'eom': '\n', 'eop': '\0', 'source': 's', 'dest': 's'},
        'm': {'eom': '\n', 'eop': '', 'source': 's', 'dest': 's'}
    }

    def setUp(self):
        self.stage = stage.ProcessorStage()

    def tearDown(self):
        self.stage = None

    def get_param_vals(self, params=None):
        """ Get values of given (or all) parameters as list. """
        args = self.stage.ARGS
        if params is None:
            params = self.stage_params
        if not isinstance(params, list):
            raise ValueError("get_param_vals() expects argument of type List"
                             " or None (got %s)." % params.__class__.__name__)
        vals = {}
        for p in params:
            try:
                vals[p] = getattr(args, p)
            except AttributeError:
                raise KeyError("Failed to get value of parameter: '%s'" % p)
        return vals

    def test_defaults(self):
        self.stage.configure()
        param_vals = self.get_param_vals()
        self.assertEqual(param_vals, self.param_defaults)

    def _test_mode(self, mode):
        if mode not in self.mode_defaults:
            raise ValueError("_test_mode() expected 'mode' parameter value"
                             " to be one of: %s (got '%s')."
                             % (self.mode_defaults.keys(), mode))
        args = ['-m', mode]
        self.stage.configure(args)
        param_vals = self.get_param_vals()
        expected_vals = dict(self.param_defaults)
        expected_vals.update(self.mode_defaults[mode])
        expected_vals.update({'mode': mode})
        self.assertEqual(param_vals, expected_vals)

    def test_mode_f(self):
        self._test_mode('f')

    def test_mode_s(self):
        self._test_mode('s')

    def test_mode_m(self):
        self._test_mode('m')


test_cases = (dataflow_stage_ProcessorStageArgsTestCase, )


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for case in test_cases:
        suite.addTest(loader.loadTestsFromTestCase(case))
    return suite
