"""
Definition of an abstract class for Dataflow Stages.
"""

import sys
import traceback
import ConfigParser
from collections import defaultdict
import textwrap

from . import logLevel

try:
    import argparse
except ImportError, e:
    sys.stderr.write("(ERROR) argparse package is not installed.\n")
    raise e


class AbstractStage(object):
    """
    Class/instance variable description:
    * Argument parser (argparse.ArgumentParser)
        __parser

    * Parsed arguments (argparse.Namespace)
        ARGS

    * Stage config parser (ConfigParser.SafeConfigParser)
        __config

    * Stage custom config (defaultdict(defaultdict(str)))
        CONFIG
    """

    def __init__(self, description="DKB Dataflow stage"):
        """ Initialize the stage

        * set description
        * define common arguments (--mode, ...)
        * ...
        """
        self.CONFIG = None
        self.__config = ConfigParser.SafeConfigParser()
        self.ARGS = None
        self.__parser = argparse.ArgumentParser(
            formatter_class=argparse.RawTextHelpFormatter,
            description=description
        )
        self.defaultArguments()

        self._error = None

    def log(self, message, level=logLevel.INFO):
        """ Output log message with given log level. """
        if not logLevel.hasMember(level):
            self.log("Unknown log level: %s" % level, logLevel.WARN)
            level = logLevel.INFO
        if type(message) == list:
            lines = message
        else:
            lines = message.splitlines()
        if lines:
            out_message = "(%s) (%s) %s" % (logLevel.memberName(level),
                                            self.__class__.__name__,
                                            lines[0])
            for l in lines[1:]:
                out_message += "\n(==) %s" % l
            out_message += "\n"
            sys.stderr.write(out_message)

    def defaultArguments(self):
        """ Config argument parser with parameters common for all stages. """
        self.add_argument('-m', '--mode', action='store', type=str,
                          help=u'processing mode: (f)ile, (s)tream'
                          ' or (m)ap-reduce.\n'
                          'Processing mode is a shortcut for a combination '
                          'of four parameters: '
                          '"-s SRC -d DEST -e EOM -E EOP", '
                          'where:\n'
                          ' \n'
                          ' mode || -s | -d | -e | -E\n'
                          '===========================\n'
                          '  s   ||  s |  s | \\n | \\0\n'
                          '---------------------------\n'
                          '  f   ||  f |  f | \\n | \'\'\n'
                          '---------------------------\n'
                          '  m   ||  s |  s | \\n | \'\'\n'
                          ' \n'
                          'If both MODE and an individual parameter are used, '
                          'the individually specified value will override '
                          'the MODE value\n'
                          'NOTE: for (m)ap-reduce mode:\n'
                          ' * if --source is '
                          'set to (h)dfs (via "-s" or "--hdfs"), names '
                          'of files to be processed will be taken from '
                          'STDIN;\n'
                          ' * if --source is set to (s)tream (by default '
                          'or via "-s"), custom value of EOM will only affect '
                          'output (input messages still should '
                          'be separated by "\\n");\n'
                          ' * source and/or destination '
                          'can not be (f)ile',
                          default='f',
                          metavar='MODE',
                          choices=['f', 's', 'm'],
                          dest='mode'
                          )
        self.add_argument('-c', '--config', action='store',
                          type=argparse.FileType('r'),
                          help=u'stage configuration file',
                          default=None,
                          metavar='CONFIG',
                          dest='config'
                          )
        self.add_argument('-e', '--end-of-message', action='store', type=str,
                          help=u'custom end of message marker\n'
                          'NOTE: in (f)ile mode for JSON messages EOM '
                          'can be set to empty string to read input '
                          'file as single JSON object, not as NDJSON. '
                          'In this case output will also be formatted '
                          'as a single JSON object (array or hash)\n'
                          'DEFAULT: \'\\n\'',
                          default=None,
                          dest='eom'
                          )
        self.add_argument('-E', '--end-of-process', action='store', type=str,
                          help=u'custom end of process marker\n'
                          'DEFAULT: \'\'',
                          default=None,
                          dest='eop'
                          )

    def _is_flag_option(self, **kwargs):
        """ Check if added argument is a flag option. """
        return kwargs.get('action', '').startswith('store_')

    def add_argument(self, *args, **kwargs):
        """ Add specific (not common) arguments. """
        wrapper = textwrap.TextWrapper(width=55, replace_whitespace=False)
        msg = textwrap.dedent(kwargs.get('help', ''))
        if kwargs.get('default', None) is not None \
                and not self._is_flag_option(**kwargs):
            msg += '\nDEFAULT: \'%(default)s\''
        msg_lines = msg.split('\n')
        wrapped_lines = [wrapper.fill(line) for line in msg_lines]
        msg = '\n'.join(wrapped_lines)
        msg += '\n '
        kwargs['help'] = msg
        self.__parser.add_argument(*args, **kwargs)

    def parse_args(self, args):
        """ Parse arguments and set dependant arguments if needed.

        Exits in case of error with code:
            2 -- failed to parse arguments
            3 -- failed to read config file
        """
        self.ARGS = self.__parser.parse_args(args)

        if self.ARGS.eom is None:
            self.ARGS.eom = '\n'
        elif self.ARGS.eom == '':
            self.log("Empty EOM marker specified!", logLevel.WARN)
        else:
            try:
                self.ARGS.eom = self.ARGS.eom.decode('string_escape')
            except (ValueError), err:
                sys.stderr.write("(ERROR) Failed to read arguments.\n"
                                 "(ERROR) Case: %s\n" % (err))
                sys.exit(1)

        if self.ARGS.eop is None:
            if self.ARGS.mode == 's':
                self.ARGS.eop = '\0'
            elif self.ARGS.eom == '':
                self.ARGS.eop = '\n'
            else:
                self.ARGS.eop = ''
        else:
            try:
                self.ARGS.eop = self.ARGS.eop.decode('string_escape')
            except (ValueError), err:
                sys.stderr.write("(ERROR) Failed to read arguments.\n"
                                 "(ERROR) Case: %s\n" % (err))
                sys.exit(1)

        if not self.read_config():
            self.config_error()

    def args_error(self, message):
        """ Output USAGE, error message and exit with code 2. """
        self.__parser.error(message)

    def config_error(self, message="Failed to read config file:"):
        """ Output error message and exit with code 3. """
        if self._error:
            message = message + "\n" + str(self._error['exception'])
        self.output_error(message)
        sys.exit(3)

    def read_config(self):
        """ Reads stage custom config file.

        :return: (True|False)
        """
        if not self.ARGS.config:
            return True

        c = self.__config

        try:
            c.readfp(self.ARGS.config)
        except (AttributeError, ConfigParser.Error), err:
            self.set_error(*sys.exc_info())
            return False

        self.CONFIG = defaultdict(lambda: defaultdict(str))
        for section in c.sections():
            self.CONFIG[section] = {}
            for (key, val) in c.items(section):
                self.CONFIG[section][key] = val

        return True

    def print_usage(self, fd=sys.stderr):
        """ Print usage message. """
        self.__parser.print_usage(fd)

    def set_error(self, err_type, err_val, err_trace):
        """ Set object `_err` variable from the last error info. """
        self._error = {'etype': err_type,
                       'exception': err_val,
                       'trace': err_trace}

    def output_error(self, message=None, exc_info=None):
        """ Output traceback of the passed (or last) error with `message`. """
        if not exc_info:
            err = self._error
            if err:
                exc_info = (err['etype'], err['exception'], err['trace'])
        if not message and exc_info:
            message = str(exc_info[1])
        if message:
            self.log(message, logLevel.ERROR)
        if exc_info:
            if exc_info[0] == KeyboardInterrupt:
                self.log("Interrupted by user.")
            else:
                trace = traceback.format_exception(*exc_info)
                self.log(''.join(trace), logLevel.DEBUG)

    def stop(self):
        """ Stop running processes and output error information. """
        self.output_error()
        self.log("Stopping stage.")

    def run(self):
        """ Run the stage. """
        raise NotImplementedError("Stage method run() is not implemented")
