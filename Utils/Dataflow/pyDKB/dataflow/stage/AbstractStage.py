"""
Definition of an abstract class for Dataflow Stages.
"""

import sys
import traceback

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
    """

    def __init__(self, description="DKB Dataflow stage"):
        """ Initialize the stage

        * set description
        * define common arguments (--mode, ...)
        * ...
        """
        self.ARGS = None
        self.__parser = argparse.ArgumentParser(description=description)
        self.defaultArguments()

        self._error = None

    def defaultArguments(self):
        """ Config argument parser with parameters common for all stages. """
        self.add_argument('-m', '--mode', action='store', type=str, nargs='?',
                          help=u'Processing mode: (f)ile, (s)tream'
                                ' or (m)ap-reduce (default: %(default)s).',
                          default='f',
                          metavar='MODE',
                          choices=['f', 's', 'm'],
                          dest='mode'
                          )

    def add_argument(self, *args, **kwargs):
        """ Add specific (not common) arguments. """
        self.__parser.add_argument(*args, **kwargs)

    def parse_args(self, args):
        """ Parse arguments and set dependant arguments if needed.

        Exits with code 2 in case of error (just as ArgumentParser does).
        """
        self.ARGS = self.__parser.parse_args(args)
        if not self.ARGS.mode:
            self.args_error("Parameter -m|--mode must be used with value:"
                            " -m MODE.")

    def args_error(self, message):
        """ Output USAGE, error message and exit. """
        self.__parser.error(message)

    def print_usage(self, fd=sys.stderr):
        """ Print usage message. """
        self.__parser.print_usage(fd)

    def set_error(self, err_type, err_val, err_trace):
        """ Set object `_err` variable from the last error info. """
        self._error = {'etype': err_type,
                       'exception': err_val,
                       'trace': err_trace}

    def output_error(self):
        """ Output information about last error. """
        err = self._error
        if err:
            if err['etype'] == KeyboardInterrupt:
                sys.stderr.write("(INFO) Interrupted by user.\n")
            else:
                trace = traceback.format_exception(err['etype'],
                                                   err['exception'],
                                                   err['trace'])
                # Label every line in trace with proper level marker
                labeled_trace = []
                n_lines = len(trace)
                levels = [('TRACE', -1), ('DEBUG', 8), ('ERROR', 3)]
                cur_lvl = 'ERROR'
                for i in xrange(n_lines):
                    for lvl, N in levels:
                        if not N - 1 < i < n_lines - N or N < 0:
                            cur_lvl = lvl
                    msg = trace[i]
                    # `msg` may contain few lines and ends with '\n'
                    messages = msg.strip().split('\n')
                    labeled_trace.append("(%s) %s\n" % (cur_lvl, messages[0]))
                    for m in messages[1:]:
                        labeled_trace.append("(==) %s\n" % m)
                # Output trace
                for line in labeled_trace:
                    sys.stderr.write(line)

    def stop(self):
        """ Stop running processes and output error information. """
        self.output_error()
        sys.stderr.write("(INFO) Stopping stage.\n")

    def run(self):
        """ Run the stage. """
        raise NotImplementedError("Stage method run() is not implemented")
