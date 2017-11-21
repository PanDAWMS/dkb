"""
Definition of an abstract class for Dataflow Stages.
"""

import sys

try:
    import argparse
except ImportError, e:
    sys.stderr.write("Please install 'argparse' package.\n")
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
        self.add_argument('-e', '--end-of-message', action='store', type=str,
                          help=u'Custom end of message marker.',
                          nargs='?',
                          default=None,
                          dest='eom'
                          )
        self.add_argument('-E', '--end-of-process', action='store', type=str,
                          help=u'Custom end of process marker.',
                          nargs='?',
                          default=None,
                          dest='eop'
                          )

    def add_argument(self, *args, **kwargs):
        """ Add specific (not common) arguments. """
        self.__parser.add_argument(*args, **kwargs)

    def parse_args(self, args):
        """ Parse arguments and set dependant arguments if needed. """
        self.ARGS = self.__parser.parse_args(args)
        if not self.ARGS.mode:
            raise ValueError(
                "Parameter -m|--mode must be used with value: -m MODE.")

        if self.ARGS.eom is None:
            self.ARGS.eom = '\n'
        else:
            try:
                self.ARGS.eom = self.ARGS.eom.decode('string_escape')
            except (ValueError), err:
                sys.stderr.write("(ERROR) Failed to read arguments.\n"
                                 "Case: %s\n" % (err))
                sys.exit(1)

        if self.ARGS.eop is None:
            if self.ARGS.mode == 's':
                self.ARGS.eop = '\0'
            else:
                self.ARGS.eop = ''
        else:
            try:
                self.ARGS.eop = self.ARGS.eop.decode('string_escape')
            except (ValueError), err:
                sys.stderr.write("(ERROR) Failed to read arguments.\n"
                                 "Case: %s\n" % (err))
                sys.exit(1)

    def print_usage(self, fd=sys.stderr):
        """ Print usage message. """
        self.__parser.print_usage(fd)

    def run(self):
        """ Run the stage. """
        raise NotImplementedError("Stage method run() is not implemented")
