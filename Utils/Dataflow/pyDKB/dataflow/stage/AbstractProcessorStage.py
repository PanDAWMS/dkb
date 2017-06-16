"""
Definition of an abstract class for Dataflow Data Processing Stages.

        USAGE:
         ProcessorStage [<options>] [<input files>]

        OPTIONS:
         -s, --source       {f|s|h}     - where to get data from:
                                          local (f)iles, (s)tdin, (h)dfs
         -i, --input-dir    DIR         - base directory for relative input
                                          file names (for local and HDFS
                                          sources).
                                          If <input files> not specified,
                                          all files from the directory will
                                          be taken as the input.

         -d, --dest         {f|s|h}     - where to send data to:
                                          local (f)iles, (s)tdin, (h)dfs

         -o, --output-dir   DIR         - base directory for output files
                                          (for local and HDFS sources)

         --hdfs                         - equivalent to "--source h --dest h"

         -m, --mode         MODE        - MODE:
                                          (f)ile      = --source f
                                                        --dest f (can be
                                                           rewritten with 's'
                                                                       or 'h')

                                          (s)tream    = --source s (can be
                                                           rewritten with 'h')
                                                        --dest s

                                          (m)apreduce = --source s (can be
                                                           rewritten with 'h')
                                                        --dest s

"""

import subprocess
import os
import sys

from . import AbstractStage
from . import messageType
from . import Message
from pyDKB.common import hdfs

class AbstractProcessorStage(AbstractStage):
    """ Abstract class to implement Processor stages

    Processor stage -- is a stage for data processing/transfornation.

    Class/instance variable description:
    * Current processing file name:
        __current_file_full  -- full name with path
        __current_file           -- file name

    * Iterable object for input data
        __input
    """

    __input_message_class = None
    __output_message_class = None

    def __init__(self, description="DKB Dataflow data processing stage."):
        """ Initialize the stage

        * set description
        * define common Processor arguments:
            input, --hdfs, --hdfs-dir, --output, ...
        * ...
        """
        self.__current_file_full = None
        self.__current_file = None
        self.__input = []
        super(AbstractProcessorStage, self).__init__(description)

    def _set_input_message_class(self, Type=None):
        """ Set input message type. """
        if not messageType.hasMember(Type):
            raise ValueError("Unknown message type: %s" % Type)
        self.__input_message_class = Message(Type)

    def input_message_class(self):
        """ Get input message class. """
        return self.__input_message_class

    def _set_output_message_class(self, Type=None):
        """ Set output message class. """
        if not messageType.hasMember(Type):
            raise ValueError("Unknown message type: %s" % Type)
        self.__output_message_class = Message(Type)

    def output_message_class(self):
        """ Get output message class. """
        return self.__output_message_class

    def defaultArguments(self):
        """ Default parser configuration. """
        super(AbstractProcessorStage, self).defaultArguments()
        self.add_argument('input_files', type=str, nargs='*',
                          help=u'Source data file.',
                          metavar=u'FILE'
                         )
        self.add_argument('-s', '--source', action='store', type=str, nargs='?',
                          help=u'Where to get data from: '
                                'local (f)iles, (s)tdin, '
                                '(h)dfs (same as --hdfs).',
                          default='f',
                          const='f',
                          choices=['f', 's', 'h'],
                          dest='source'
                          )
        self.add_argument('-i', '--input-dir', action='store', type=str,
                          nargs='?',
                          help=u'Base directory in local file system '
                                'or in HDFS (for relative FILE names). '
                                'If no FILE specified, all files from the '
                                'directory will be taken.',
                          default='',
                          const='',
                          metavar='DIR',
                          dest='input_dir'
                          )
        self.add_argument('-d', '--dest', action='store', type=str, nargs='?',
                          help=u'Where to send results: '
                                'local (f)iles, (s)tdout, '
                                '(h)dfs (same as --hdfs).',
                          default='f',
                          const='f',
                          choices=['f', 's', 'h'],
                          dest='dest'
                          )
        self.add_argument('-o', '--output-dir', action='store', type=str,
                          nargs='?',
                          help=u'Directory or file for output files '
                                '(local or HDFS). '
                                'If the directory doesn\'t exist, use "dir/" '
                                'instead of "dir".',
                          default='',
                          const='output/',
                          metavar='DIR',
                          dest='output_dir'
                          )
        self.add_argument('--hdfs', action='store', type=bool, nargs='?',
                          help=u'Source files are stored in HDFS; '
                                'if no input FILE specified, filenames will '
                                'come to stdin. '
                                'This option is equivalent to '
                                '"--source h --dest h"',
                          default=False,
                          const=True,
                          metavar='HDFS',
                          dest='hdfs'
                          )


    def parse_args(self, args):
        """ Parse arguments and set dependant arguments if neeeded. """
        super(AbstractProcessorStage, self).parse_args(args)

        # HDFS: HDFS file -> local file -> processor -> local file -> HDFS file
        if self.ARGS.hdfs:
            self.ARGS.source = 'h'
            self.ARGS.dest   = 'h'

        # Stream (Kafka) and MapReduce mode: STDIN -> processor -> STDOUT
        # If data source is specified as HDFS, files will be taken from HDFS
        #       and filenames -- from stdin
        if self.ARGS.mode in ('s', 'm'):
            if self.ARGS.source != 'h':
                self.ARGS.source = 's'
            self.ARGS.dest = 's'

        if   self.ARGS.source == 'h':
            if self.ARGS.input_files or self.ARGS.mode == 'm':
            # In MapReduce mode we`re going to get the list of files from STDIN
                self.__input = self.__hdfs_in_files()
            else:
                self.__input = self.__hdfs_in_dir()
        elif self.ARGS.source == 'f':
            if self.ARGS.input_files:
                self.__input = self.__local_in_files()
            else:
                self.__input = self.__local_in_dir()
        elif self.ARGS.source == 's':
            self.__input = [sys.stdin]
        else:
            raise ValueError("Unrecognized source type: %s" % self.ARGS.source)

        # Check that data source is specified
        if self.ARGS.source == 'f' \
            and not (self.ARGS.input_files or self.ARGS.input_dir):
            sys.stderr.write("No input data sources specified.\n")
            self.print_usage(sys.stderr)

    def run(self):
        """ Run process() for every input() message. """
        for msg in self.input():
            if not msg:
                continue
            out = self.process(msg)
            self.output(out)

    def process(self, input_message):
        """ Transform input_message -> output_message.

        To be implemented individually for every stage.
        """
        raise NotImplementedError("Stage method process() is not implemented")

    def parseMessage(self, input_message):
        """ Verify and parse input message.

        Is called from input() method.
        """
        messageClass = self.__input_message_class
        try:
            msg = messageClass(input_message)
            msg.decode()
            return msg
        except (ValueError, TypeError), err:
            sys.stderr.write("(WARN) Failed to read input message as %s.\n"
                             "Cause: %s\n" % (messageClass.typeName(), err))
            return None

    def input(self):
        """ Generator for input messages.

        Returns iterable object.
        Every iteration returns single input message to be processed.
        """
        for fd in self.__input:
            if fd == sys.stdin:
                for r in self.stream_input(fd):
                    yield r
            else:
                for r in self.file_input(fd):
                    yield r

    def stream_input(self, fd):
        """ Generator for input messages.

        Read data from STDIN;
        Split stream into messages;
        Yield Message object.
        """
        iterator = iter(fd.readline, "")
        for line in iterator:
            yield self.parseMessage(line)

    def file_input(self, fd):
        """ Generator for input messages.

        By default reads file just as stream.
        To be implemented individually for other cases.
        """
        return self.stream_input(fd)

    def output(self, message):
        """ Output given message (to the file, files or stdout)

        For now: STDOUT.
        TODO: rewrite to act according to cmdline args.
        """
        print message.content()


    def __local_in_dir(self):
        """ Call file descriptors generator for files in local dir. """
        dirname = self.ARGS.input_dir
        if not dirname:
            return []
        files = []
        try:
            for f in os.listdir(dirname):
                if os.path.isfile(os.path.join(dirname, f)):
                    files.append(f)
        except OSError, err:
            sys.stderr.write("(ERROR) Failed to get list of files.\n"
                             "Error message: %s\n" % err)
        if not files:
            return []
        self.ARGS.input_files = files
        return self.__local_in_files()

    def __local_in_files(self):
        """ Generator for file descriptors to read data from (local files). """
        filenames = self.ARGS.input_files
        for f in filenames:
            name = os.path.basename(f)
            self.__current_file = name
            if self.ARGS.input_dir:
                f = os.path.join(self.ARGS.input_dir, f)
            self.__current_file_full = f
            with open(f, 'r') as infile:
                yield infile


    def __hdfs_in_dir(self):
        """ Call file descriptors generator for files in HDFS dir. """
        dirname = self.ARGS.input_dir
        files = hdfs.listdir(dirname, "f")
        self.ARGS.input_files = files
        if not files:
            return []
        return self.__hdfs_in_files()

    def __hdfs_in_files(self):
        """ Generator for file descriptors to read data from (HDFS files). """
        filenames = self.ARGS.input_files
        if not filenames:
            filenames = iter(sys.stdin.readline, "")
        for f in filenames:
            f = f.strip()
            if self.ARGS.input_dir:
                f = os.path.join(self.ARGS.input_dir, f)
            if not f:
                continue
            name = hdfs.getfile(f)
            self.__current_file_full = f
            self.__current_file = name

            with open(name, 'r') as infile:
                yield infile
            try:
                os.remove(name)
            except OSError:
                sys.stderr.write("(WARN) Failed to remove uploaded file: %s\n"
                                                                        % name)
            self.__current_file = None
