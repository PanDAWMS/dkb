#!/usr/bin/env python

"""
Tests for pyDKB.dataflow.stage.ProcessorStage.configure().
Usage: 'python -m unittest discover' from ..
(directory with pyDKB.dataflow.stage code).
"""

import os
import sys
import unittest

from lib import isolate_function_error

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


args_to_add = {
    'source': ['f', 's', 'h'],
    'dest': ['f', 's', 'h'],
}


hdfs_args = {
    'hdfs': True,
    'source': 'h',
    'dest': 'h',
}


modes = {
    's': {'mode': 's', 'source': 's', 'dest': 's', 'eom': '\n', 'eop': '\0'},
    'f': {'mode': 'f', 'source': 'f', 'dest': 'f', 'eom': '\n', 'eop': ''},
    'm': {'mode': 'm', 'source': 's', 'dest': 's', 'eom': '\n', 'eop': ''},
}


class Case(unittest.TestCase):
    expected_args = {
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

    def setUp(self):
        self.stage = pyDKB.dataflow.stage.ProcessorStage()
        self.args = dict(self.expected_args)

    def tearDown(self):
        self.stage = None
        self.args = None

    def check_args(self):
        for a in self.args:
            # Such kind of testing does not display the argument's name,
            # hence the {a: ...} addition.
            self.assertEqual({a: getattr(self.stage.ARGS, a)},
                             {a: self.args[a]})

    def test_default(self):
        [msg, result] = isolate_function_error(self.stage.configure, [])
        self.assertIn('No input files specified.', msg)

    def test_hdfs(self):
        self.stage.configure(['--hdfs'])
        self.args.update(hdfs_args)
        self.args['input_dir'] = '/user/DKB/'
        self.check_args()

    def test_e(self):
        self.stage.configure(['-e', '\t', 'something'])
        self.args['eom'] = '\t'
        self.args['input_files'] = ['something']
        self.check_args()

    def test_end_of_message(self):
        self.stage.configure(['--end-of-message', '\t', 'something'])
        self.args['eom'] = '\t'
        self.args['input_files'] = ['something']
        self.check_args()

    def test_E(self):
        self.stage.configure(['-E', '\t', 'something'])
        self.args['eop'] = '\t'
        self.args['input_files'] = ['something']
        self.check_args()

    def test_end_of_process(self):
        self.stage.configure(['--end-of-process', '\t', 'something'])
        self.args['eop'] = '\t'
        self.args['input_files'] = ['something']
        self.check_args()

    def test_e_empty(self):
        self.stage.configure(['-e', '', 'something'])
        self.args['eom'] = ''
        self.args['eop'] = '\n'
        self.args['input_files'] = ['something']
        self.check_args()

    def test_e_empty_E_override(self):
        self.stage.configure(['-e', '', '-E', '', 'something'])
        self.args['eom'] = ''
        self.args['eop'] = ''
        self.args['input_files'] = ['something']
        self.check_args()

    def test_raw_strings(self):
        self.stage.configure(['-e', r'\t', '-E', r'\n', 'something'])
        self.args['eom'] = '\t'
        self.args['eop'] = '\n'
        self.args['input_files'] = ['something']
        self.check_args()

    def test_i(self):
        self.stage.configure(['-i', 'something'])
        self.args['input_dir'] = 'something'
        self.check_args()

    def test_input_dir(self):
        self.stage.configure(['--input-dir', 'something'])
        self.args['input_dir'] = 'something'
        self.check_args()

    def test_o(self):
        self.stage.configure(['-o', 'something', 'something'])
        self.args['output_dir'] = 'something'
        self.args['input_files'] = ['something']
        self.check_args()

    def test_output_dir(self):
        self.stage.configure(['--output-dir', 'something', 'something'])
        self.args['output_dir'] = 'something'
        self.args['input_files'] = ['something']
        self.check_args()

    def test_input_files(self):
        self.stage.configure(['something', 'something_else'])
        self.args['input_files'] = ['something', 'something_else']
        self.check_args()


def add_arg(arg, val, short=False):
    if short:
        args = ['-' + arg[0], val, 'something']
        fname = 'test_%s_%s' % (arg[0], val)
    else:
        args = ['--' + arg, val, 'something']
        fname = 'test_%s_%s' % (arg, val)

    def f(self):
        self.stage.configure(args)
        self.args[arg] = val
        self.args['input_files'] = ['something']
        if self.args['source'] == 's':
            self.args['input_dir'] = None
        elif self.args['source'] == 'h':
            self.args['input_dir'] = '/user/DKB/'
        self.check_args()
    setattr(Case, fname, f)


def add_arg_incorrect(arg, short=False):
    if short:
        val = 'incorrect'
        args = ['-' + arg[0], val]
        fname = 'test_%s_%s' % (arg[0], val)
    else:
        val = 'incorrect'
        args = ['--' + arg, val]
        fname = 'test_%s_%s' % (arg, val)

    def f(self):
        [msg, result] = isolate_function_error(self.stage.parse_args, args)
        err = "error: argument -%s/--%s: invalid choice: '%s'" % (arg[0],
                                                                  arg, val)
        self.assertIn(err, msg)
    setattr(Case, fname, f)


def add_mode(val, short=False):
    if short:
        args = ['-m', val, 'something']
        fname = 'test_m_%s' % (val)
    else:
        args = ['--mode', val, 'something']
        fname = 'test_mode_%s' % (val)

    def f(self):
        self.stage.configure(args)
        self.args.update(modes[val])
        self.args['input_files'] = ['something']
        if val != 'f':
            self.args['input_dir'] = None
        self.check_args()
    setattr(Case, fname, f)


# hdfs >> source-dest >> mode
def add_override_hdfs(arg, val):
    def f(self):
        self.stage.configure(['--hdfs', '--' + arg, val])
        self.args[arg] = val
        self.args.update(hdfs_args)
        self.args['input_dir'] = '/user/DKB/'
        self.check_args()
    setattr(Case, 'test_override_hdfs_%s_%s' % (arg, val), f)


def add_override_mode(arg, val, mode_val):
    if mode_val == 'm' and val == 'f':
        def f(self):
            args = ['--' + arg, val, '--mode', mode_val, 'something']
            [msg, result] = isolate_function_error(self.stage.configure, args)
            err = "File source/destination is not allowed in map-reduce mode."
            self.assertIn(err, msg)
    else:
        def f(self):
            self.stage.configure(['--' + arg, val, '--mode', mode_val,
                                  'something'])
            self.args.update(modes[mode_val])
            self.args[arg] = val
            if self.args['source'] == 's':
                self.args['input_dir'] = None
            elif self.args['source'] == 'h':
                self.args['input_dir'] = '/user/DKB/'
            self.args['input_files'] = ['something']
            self.check_args()
    setattr(Case, 'test_override_%s_%s_mode_%s' % (arg, val, mode_val), f)


def add_override_hdfs_mode(val):
    def f(self):
        self.stage.configure(['--hdfs', '--mode', val])
        self.args.update(modes[val])
        self.args.update(hdfs_args)
        self.args['input_dir'] = '/user/DKB/'
        self.check_args()
    setattr(Case, 'test_override_hdfs_mode_%s' % (val), f)


for a in args_to_add:
    for v in args_to_add[a]:
        add_arg(a, v, True)
        add_arg(a, v)
        add_override_hdfs(a, v)
        for m in modes:
            add_override_mode(a, v, m)
    add_arg_incorrect(a, True)
    add_arg_incorrect(a)


for m in modes:
    add_mode(m, True)
    add_mode(m)
    add_override_hdfs_mode(m)


add_arg_incorrect('mode', True)
add_arg_incorrect('mode')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(Case))
    return suite
