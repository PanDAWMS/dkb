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
                                          local (f)iles, (s)tdout, (h)dfs

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
import types
import time

from . import AbstractStage
from . import messageType
from . import Message
from pyDKB.dataflow import DataflowException
from pyDKB.common import hdfs

class AbstractProcessorStage(AbstractStage):
    """ Abstract class to implement Processor stages

    Processor stage -- is a stage for data processing/transfornation.

    Class/instance variable description:
    * Current processing file name:
        __current_file_full  -- full name with path
        __current_file           -- file name

    * Iterable object for input data sources (file descriptors)
        __input

    * Output messages buffer:
        __output_buffer

    * Generator object for output file descriptor
      OR file descriptor (for (s)tream mode)
        __output

    * List of objects to be "stopped"
        __stoppable
    """

    __input_message_class = None
    __output_message_class = None

    __stream_EOMessage = '\n'
    __stream_EOProcess = '\0'

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
        self.__output_buffer = []
        self.__stoppable = []
        self.__EOMessage = '\n'
        self.__EOProcess = '\n'
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
                          help=u'Directory for output files '
                                '(local or HDFS). ',
                          default='',
                          const='output/',
                          metavar='DIR',
                          dest='output_dir'
                          )
        self.add_argument('--hdfs', action='store_true',
                          help=u'Source files are stored in HDFS; '
                                'if no input FILE specified, filenames will '
                                'come to stdin. '
                                'This option is equivalent to '
                                '"--source h --dest h"',
                          default=False,
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
            self.__current_file = sys.stdin.name
            self.__current_file_full = sys.stdin.name
        else:
            raise ValueError("Unrecognized source type: %s" % self.ARGS.source)

        # Check that data source is specified
        if self.ARGS.source == 'f' \
            and not (self.ARGS.input_files or self.ARGS.input_dir):
            sys.stderr.write("(ERROR) No input data sources specified.\n")
            self.print_usage(sys.stderr)
            raise DataflowException

        self.__stoppable_append(self.__input, types.GeneratorType)

        # Configure output
        if   self.ARGS.dest == 'f':
           self.__output = self.__out_files('l')
        elif self.ARGS.dest == 'h':
           self.__output = self.__out_files('h')
        elif self.ARGS.dest == 's':
           ustdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
           self.__output = ustdout
           self.__EOMessage = self.__stream_EOMessage
           self.__EOProcess = self.__stream_EOProcess

        self.__stoppable_append(self.__output, types.GeneratorType)


    def run(self):
        """ Run process() for every input() message. """
        for msg in self.input():
            try:
                if msg and self.process(self, msg):
                    self.flush_buffer()
            except Exception:
                raise
            else:
                self.forward()
            finally:
                self.clear_buffer()

    def stop(self):
        """ Finalize all the processes and prepare to exit. """
        failures = []
        for p in self.__stoppable:
            try:
                p.close()
            except AttributeError, e:
                sys.stderr.write("(WARN) Close method is not defined for"
                                 " %s.\n" % p)
            except Exception, e:
                failures.append((p, e))
        if failures:
            for f in failures:
                sys.stderr.write("(ERROR) Failed to stop %s: %s\n" % f)


    @staticmethod
    def process(stage, input_message):
        """ Transform input_message -> output_message.

        To be implemented individually for every stage.
        Takes the stage as first argument to allow calling output()
          from inside the function.

        Return value:
            True  -- processing successfully finished
            False -- processing failed (skip the input message)
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
                             "(WARN) Cause: %s\n" % (messageClass.typeName(), err))
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
        if self.__EOMessage == '\n':
            iterator = iter(fd.readline, "")
            for line in iterator:
                yield self.parseMessage(line)
        else:
	    raise NotImplementedError("Stream input is not implemented for"
                                      " custom EOMessage marker.")

    def file_input(self, fd):
        """ Generator for input messages.

        By default reads file just as stream.
        To be implemented individually for other cases.
        """
        return self.stream_input(fd)


    def output(self, message):
        """ Put the (list of) message(s) to the output buffer. """
        if isinstance(message, self.__output_message_class):
            self.__output_buffer.append(message)
        elif type(message) == list:
            for m in message:
                self.output.message(m)
        else:
            raise TypeError("Stage.output() expects parameter to be of type"
                            " %s or %s (got %s)"
                            % (self.__output_message_class, list,
                               type(message))
                           )



    def forward(self):
        """ Send EOPMessage in the streaming output mode. """
        if self.ARGS.dest == 's':
            self.__output.write(self.__EOProcess)

    def flush_buffer(self):
        """ Flush message buffer to the output. """
        if self.ARGS.dest == 's':
            self.stream_flush()
        else:
            self.file_flush()

    def stream_flush(self, fd=None):
        """ Flush message buffer as a stream. """
        if not fd:
            fd = self.__output
        for msg in self.__output_buffer:
            fd.write(msg.encode())
            fd.write(self.__EOMessage)

    def file_flush(self):
        """ Flush message buffer into a file.

        By default writes to file as to a stream.
        To be implemented individually if needed.
        """
        fd = self.__output.next()
        self.stream_flush(fd)

    def clear_buffer(self):
        """ Drop buffered output messages. """
        self.__output_buffer = []

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
                             "(ERROR) Error message: %s\n" % err)
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
            self.__current_file = None
            self.__current_file_full = None

    def __out_files(self, t='l'):
        """ Generator for file descriptors to write data to.

        Parameters:
            t {'l'|'h'} -- (l)ocal or (h)dfs
        """
        if t not in ('l', 'h'):
            raise ValueError("parameter t acceptable values: 'l','h'"
                             " (got '%s')" % t)
        ext = self.output_message_class().extension()
        fd = None
        cf = None
        output_dir = self.ARGS.output_dir
        if output_dir and not os.path.isdir(output_dir):
            if t == 'l':
                try:
                    os.makedirs(output_dir, 0770)
                except OSError, err:
                    sys.stderr.write("(ERROR) Failed to create output directory\n"
                                     "(ERROR) Error message: %s\n" % err)
                    raise DataflowException
            else:
                hdfs.makedirs(output_dir)
        try:
            while self.__current_file_full:
                if cf == self.__current_file_full:
                    yield fd
                    continue
                output_dir = self.ARGS.output_dir
                if not output_dir:
                     output_dir = os.path.dirname(self.__current_file_full)
                if not output_dir:
                     if t == 'l':
                         output_dir = os.getcwd()
                     if t == 'h':
                         output_dir = os.path.join(hdfs.DKB_HOME, "temp",
                                                   str(int(time.time())))
                         hdfs.makedirs(output_dir)
                     self.ARGS.output_dir = output_dir
                     sys.stderr.write("(INFO) Output dir set to: %s\n"
                                      % output_dir)
                if self.__current_file \
                  and self.__current_file != sys.stdin.name:
                    filename = os.path.splitext(self.__current_file)[0] + ext
                else:
                    filename = str(int(time.time())) + ext
                if t == 'l':
                    filename = os.path.join(output_dir, filename)
                if os.path.exists(filename):
                    if fd and os.path.samefile(filename, fd.name):
                        yield fd
                        continue
                    else:
                        raise DataflowException("File already exists: %s\n"
                                                 % filename)
                if fd:
                    fd.close()
                    if t == 'h':
                        hdfs.putfile(fd.name, output_dir)
                        os.remove(fd.name)
                fd = open(filename, "w", 0)
                yield fd
        finally:
            if fd:
                fd.close()
                if t == 'h':
                    hdfs.putfile(fd.name, output_dir)
                    os.remove(fd.name)


    def __hdfs_in_dir(self):
        """ Call file descriptors generator for files in HDFS dir. """
        dirname = self.ARGS.input_dir
        try:
            files = hdfs.listdir(dirname, "f")
        except hdfs.HDFSException, err:
            sys.stderr.write("(ERROR) Failed to get list of files.\n"
                             "(ERROR) Error message: %s\n" % err)
            files = []
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

            try:
                with open(name, 'r') as infile:
                    yield infile
            finally:
                try:
                    os.remove(name)
                except OSError:
                    sys.stderr.write("(WARN) Failed to remove uploaded file:"
                                     " %s\n" % name)
            self.__current_file = None
            self.__current_file_full = None


    def __stoppable_append(self, obj, cls):
        """ Appends OBJ (of type CLS) to the list of STOPPABLE. """
        if isinstance(obj, cls):
            self.__stoppable.append(obj)
