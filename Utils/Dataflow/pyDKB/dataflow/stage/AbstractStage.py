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
            description=description,
            epilog=textwrap.dedent(
                '''\
                NOTES

                Processing mode
                  is defined as a combination of data source and
                  destination type (local/HDFS file(s) or standard stream)
                  and pre-defined EOP and EOM markers:

                  mode | source | dest | eom | eop
                  -----+--------+------+-----+-----
                    s  |    s   |   s  |  \\n |  \\0
                  -----+--------+------+-----+-----
                    f  |   f/h  |  f/h |  \\n |
                  -----+--------+------+-----+-----
                    m  |   s/h  |   s  |  \\n |
                  -----+--------+------+-----+-----
                    h  |    h   |  h/f |  \\n |
                  -----+--------+------+-----+-----''')
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
                                ' or (m)ap-reduce',
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
                          help=u'custom end of message marker.\n'
                                'DEFAULT: \'\\n\'',
                          default=None,
                          dest='eom'
                          )
        self.add_argument('-E', '--end-of-process', action='store', type=str,
                          help=u'custom end of process marker.\n'
                                'DEFAULT: \'\'',
                          default=None,
                          dest='eop'
                          )

    def add_argument(self, *args, **kwargs):
        """ Add specific (not common) arguments. """
        wrapper = textwrap.TextWrapper(width=55, replace_whitespace=False)
        msg = textwrap.dedent(kwargs.get('help', ''))
        if kwargs.get('default', None) is not None \
                and not kwargs.get('action', '').startswith('store_'):
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

    def output_error(self, message=None):
        """ Output information about last error or `message`. """
        err = self._error
        cur_lvl = logLevel.ERROR
        if message:
            self.log(message, cur_lvl)
        elif err:
            if err['etype'] == KeyboardInterrupt:
                self.log("Interrupted by user.")
            else:
                trace = traceback.format_exception(err['etype'],
                                                   err['exception'],
                                                   err['trace'])
                # Label every line in trace with proper level marker
                labeled_trace = []
                n_lines = len(trace)
                # List of log levels with number of lines
                # to be output with this level
                levels = [(logLevel.DEBUG, -1), (logLevel.ERROR, 1)]
                for i in xrange(n_lines):
                    for lvl, N in levels:
                        if i >= n_lines - N or N < 0:
                            cur_lvl = lvl
                    msg = trace[i]
                    self.log(msg, cur_lvl)

    def stop(self):
        """ Stop running processes and output error information. """
        self.output_error()
        self.log("Stopping stage.")

    def run(self):
        """ Run the stage. """
        raise NotImplementedError("Stage method run() is not implemented")
