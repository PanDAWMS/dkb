#!/usr/bin/env python

"""
Tests for pyDKB.dataflow.stage.ProcessorStage.
Usage: 'python -m unittest discover' from ..
(directory with pyDKB.dataflow.stage code).
"""


import cStringIO
import os
import sys
import tempfile
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


def isolate_function_error(f, *args):
    """ Silence and retrieve the function's error message.

    The function is expected to throw a SystemExit when run with
    specific arguments. Error stream is redirected into a string during the
    function's execution, and the resulting messages can be analyzed.

    :param f: function to execute
    :type f: function
    :param args: arguments to execute function with
    :type args: list

    :return:
    :rtype:
    """
    buf = cStringIO.StringIO()
    temp_err = sys.stderr
    sys.stderr = buf
    try:
        result = f(*args)
    except SystemExit:
        result = None
    sys.stderr = temp_err
    buf.seek(0)
    msg = buf.read()
    buf.close()
    return [msg, result]


args_to_add = {
    'source': ['f', 's', 'h'],
    'dest': ['f', 's', 'h'],
}


modes = {
    's': {'mode': 's', 'source': 's', 'dest': 's', 'eom': '\n', 'eop': '\0'},
    'f': {'mode': 'f', 'source': 'f', 'dest': 'f', 'eom': '\n', 'eop': ''},
    'm': {'mode': 'm', 'source': 's', 'dest': 's', 'eom': '\n', 'eop': ''},
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
        self.args = dict(self.default_args)

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
        self.args['hdfs'] = True
        self.args['source'] = 'h'
        self.args['dest'] = 'h'
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
    setattr(ProcessorStageArgsTestCase, fname, f)


def add_arg_incorrect(arg, short=False):
    if short:
        val = 'i'
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
    setattr(ProcessorStageArgsTestCase, fname, f)


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
    setattr(ProcessorStageArgsTestCase, fname, f)


# hdfs >> source-dest >> mode
def add_override_hdfs(arg, val):
    def f(self):
        self.stage.configure(['--hdfs', '--' + arg, val])
        self.args[arg] = val
        self.args['hdfs'] = True
        self.args['source'] = 'h'
        self.args['dest'] = 'h'
        self.args['input_dir'] = '/user/DKB/'
        self.check_args()
    setattr(ProcessorStageArgsTestCase,
            'test_override_hdfs_%s_%s' % (arg, val), f)


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
    setattr(ProcessorStageArgsTestCase,
            'test_override_%s_%s_mode_%s' % (arg, val, mode_val), f)


def add_override_hdfs_mode(val):
    def f(self):
        self.stage.configure(['--hdfs', '--mode', val])
        self.args.update(modes[val])
        self.args['hdfs'] = True
        self.args['source'] = 'h'
        self.args['dest'] = 'h'
        self.args['input_dir'] = '/user/DKB/'
        self.check_args()
    setattr(ProcessorStageArgsTestCase,
            'test_override_hdfs_mode_%s' % (val), f)


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


class ProcessorStageConfigArgTestCase(unittest.TestCase):
    def setUp(self):
        self.stage = pyDKB.dataflow.stage.ProcessorStage()
        self.fake_config = tempfile.NamedTemporaryFile(dir='.')

    def tearDown(self):
        self.stage = None
        if not self.fake_config.closed:
            self.fake_config.close()
        self.fake_config = None

    def test_correct_c(self):
        self.stage.configure(['-c', self.fake_config.name, 'something'])
        isfile = isinstance(getattr(self.stage.ARGS, 'config'), file)
        self.assertTrue(isfile)

    def test_correct_config(self):
        self.stage.configure(['--config', self.fake_config.name, 'something'])
        isfile = isinstance(getattr(self.stage.ARGS, 'config'), file)
        self.assertTrue(isfile)

    def test_missing_c(self):
        self.fake_config.close()
        args = ['-c', self.fake_config.name, 'something']
        [msg, result] = isolate_function_error(self.stage.configure, args)
        err = "[Errno 2] No such file or directory: '%s'" %\
              self.fake_config.name
        self.assertIn(err, msg)

    def test_missing_config(self):
        self.fake_config.close()
        args = ['--config', self.fake_config.name, 'something']
        [msg, result] = isolate_function_error(self.stage.configure, args)
        err = "[Errno 2] No such file or directory: '%s'" %\
              self.fake_config.name
        self.assertIn(err, msg)

    def test_unreadable_c(self):
        os.chmod(self.fake_config.name, 0300)
        args = ['-c', self.fake_config.name, 'something']
        [msg, result] = isolate_function_error(self.stage.configure, args)
        err = "[Errno 13] Permission denied: '%s'" %\
              self.fake_config.name
        self.assertIn(err, msg)

    def test_unreadable_config(self):
        os.chmod(self.fake_config.name, 0300)
        args = ['--config', self.fake_config.name, 'something']
        [msg, result] = isolate_function_error(self.stage.configure, args)
        err = "[Errno 13] Permission denied: '%s'" %\
              self.fake_config.name
        self.assertIn(err, msg)


test_cases = (
    ProcessorStageArgsTestCase,
    ProcessorStageConfigArgTestCase,
)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for case in test_cases:
        suite.addTest(loader.loadTestsFromTestCase(case))
    return suite
