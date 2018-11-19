#!/usr/bin/env python

"""
Tests for pyDKB.dataflow.stage.ProcessorStage.
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


args_to_add = {
    'source': ['f', 's', 'h'],
    'dest': ['f', 's', 'h'],
}


# mode: source, dest, EOM, EOP
modes = {
    's': ['s', 's', '\n', '\0'],
    'f': ['f', 'f', '\n', ''],
    'm': ['s', 's', '\n', ''],
}


class ProcessorStageArgsTestCase(unittest.TestCase):
    default_args = {
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

    def tearDown(self):
        self.stage = None

    def check_args(self, args):
        for a in args:
            # Such kind of testing does not display the argument's name,
            # hence the "a + '_'" addition.
            if isinstance(args[a], str):
                self.assertEqual(a + '_' + getattr(self.stage.ARGS, a),
                                 a + '_' + args[a])
            else:
                self.assertEqual(getattr(self.stage.ARGS, a), args[a])

    def test_default(self):
        self.stage.parse_args('')
        self.check_args(self.default_args)

    def test_hdfs(self):
        self.stage.parse_args(['--hdfs'])
        args = dict(self.default_args)
        args['hdfs'] = True
        self.check_args(args)

    def test_eom(self):
        self.stage.parse_args(['-e', '\t'])
        args = dict(self.default_args)
        args['eom'] = '\t'
        self.check_args(args)

    def test_eop(self):
        self.stage.parse_args(['-E', '\t'])
        args = dict(self.default_args)
        args['eop'] = '\t'
        self.check_args(args)

    def test_input_dir(self):
        self.stage.parse_args(['--input-dir', 'something'])
        args = dict(self.default_args)
        args['input_dir'] = 'something'
        self.check_args(args)

    def test_i(self):
        self.stage.parse_args(['-i', 'something'])
        args = dict(self.default_args)
        args['input_dir'] = 'something'
        self.check_args(args)

    def test_output_dir(self):
        self.stage.parse_args(['--output-dir', 'something'])
        args = dict(self.default_args)
        args['output_dir'] = 'something'
        self.check_args(args)

    def test_o(self):
        self.stage.parse_args(['-o', 'something'])
        args = dict(self.default_args)
        args['output_dir'] = 'something'
        self.check_args(args)




def add_arg(arg, val, short=False):
    def f(self):
        if short:
            self.stage.parse_args(['-' + arg[0], val])
        else:
            self.stage.parse_args(['--' + arg, val])
        args = dict(self.default_args)
        args[arg] = val
        self.check_args(args)
    if short:
        setattr(ProcessorStageArgsTestCase, 'test_%s_%s' % (arg[0], val), f)
    else:
        setattr(ProcessorStageArgsTestCase, 'test_%s_%s' % (arg, val), f)


def add_mode(val, short=False):
    def f(self):
        if short:
            self.stage.parse_args(['-m', val])
        else:
            self.stage.parse_args(['--mode', val])
        args = dict(self.default_args)
        args['mode'] = val
        args['source'] = modes[val][0]
        args['dest'] = modes[val][1]
        args['eom'] = modes[val][2]
        args['eop'] = modes[val][3]
        self.check_args(args)
    if short:
        setattr(ProcessorStageArgsTestCase, 'test_m_%s' % (val), f)
    else:
        setattr(ProcessorStageArgsTestCase, 'test_mode_%s' % (val), f)


for a in args_to_add:
    for v in args_to_add[a]:
        add_arg(a, v)
        add_arg(a, v, True)
for m in modes:
    add_mode(m)
    add_mode(m, True)


test_cases = (
    ProcessorStageArgsTestCase,
)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for case in test_cases:
        suite.addTest(loader.loadTestsFromTestCase(case))
    return suite
