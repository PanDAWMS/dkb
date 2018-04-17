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
from . import logLevel
from . import Message
from pyDKB.dataflow import DataflowException
from pyDKB.common import hdfs
from pyDKB.common import custom_readline


class ProcessorStage(AbstractStage):
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
        super(ProcessorStage, self).__init__(description)

    def set_input_message_type(self, Type=None):
        """ Set input message type. """
        if not messageType.hasMember(Type):
            raise ValueError("Unknown message type: %s" % Type)
        self.__input_message_class = Message(Type)

    def input_message_class(self):
        """ Get input message class. """
        return self.__input_message_class

    def set_output_message_type(self, Type=None):
        """ Set output message class. """
        if not messageType.hasMember(Type):
            raise ValueError("Unknown message type: %s" % Type)
        self.__output_message_class = Message(Type)

    def output_message_class(self):
        """ Get output message class. """
        return self.__output_message_class

    def defaultArguments(self):
        """ Default parser configuration. """
        super(ProcessorStage, self).defaultArguments()
        self.add_argument('input_files', type=str, nargs='*',
                          help=u'Source data file.',
                          metavar=u'FILE'
                          )
        self.add_argument('-s', '--source', action='store', type=str,
                          nargs='?',
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
                          default='out',
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
        """ Parse arguments and set dependant arguments if neeeded.

        Exits with code 2 in case of error (just like ArgumentParser does).
        """
        super(ProcessorStage, self).parse_args(args)

        # HDFS: HDFS file -> local file -> processor -> local file -> HDFS file
        if self.ARGS.hdfs:
            self.ARGS.source = 'h'
            self.ARGS.dest = 'h'

        # Stream (Kafka) and MapReduce mode: STDIN -> processor -> STDOUT
        # If data source is specified as HDFS, files will be taken from HDFS
        #       and filenames -- from stdin
        if self.ARGS.mode in ('s', 'm'):
            if self.ARGS.source != 'h':
                self.ARGS.source = 's'
            self.ARGS.dest = 's'

        if self.ARGS.source == 'h':
            if self.ARGS.input_files or self.ARGS.mode == 'm':
                # In MapReduce mode
                # we`re going to get the list of files from STDIN
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
            self.args_error("Unrecognized source type: %s" % self.ARGS.source)

        # Check that data source is specified
        if self.ARGS.source == 'f' \
                and not (self.ARGS.input_files or self.ARGS.input_dir):
            self.args_error("No input data sources specified.")

        self.__stoppable_append(self.__input, types.GeneratorType)

        # Configure output
        if self.ARGS.dest == 'f':
            self.__output = self.__out_files('l')
        elif self.ARGS.dest == 'h':
            self.__output = self.__out_files('h')
        elif self.ARGS.dest == 's':
            ustdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
            self.__output = ustdout

        self.__stoppable_append(self.__output, types.GeneratorType)

    def run(self):
        """ Run process() for every input() message. """
        exit_code = 0
        err = None
        try:
            for msg in self.input():
                if msg and self.process(self, msg):
                    self.flush_buffer()
                else:
                    self.clear_buffer()
                self.forward()
        except BaseException, err:
            # Catch everything for uniform exception handling
            # Clear buffer -- just in case someone will decide
            # to reuse the object.
            self.clear_buffer()
            exit_code = 1
            self.set_error(*sys.exc_info())
            self.stop()
        finally:
            # If something went wrong in `except` clause, we will still
            # get here and return, so the exceptions from there will never
            # reach the user
            if err and not isinstance(err, Exception):
                sys.exit(exit_code)
            return exit_code

    # Override
    def stop(self):
        """ Finalize all the processes and prepare to exit. """
        super(ProcessorStage, self).stop()
        failures = []
        for p in self.__stoppable:
            try:
                p.close()
            except AttributeError, e:
                self.log("Close method is not defined for %s." % p,
                         logLevel.WARN)
            except Exception, e:
                failures.append((p, e))
        if failures:
            for f in failures:
                self.log("Failed to stop %s: %s" % f, logLevel.ERROR)

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
            self.log("Failed to read input message as %s.\n"
                     "Cause: %s\n"
                     "Original message: '%s'"
                     % (messageClass.typeName(), err, input_message),
                     logLevel.WARN)
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
        if self.ARGS.eom == '\n':
            iterator = iter(fd.readline, "")
        elif self.ARGS.eom == '':
            iterator = [fd.read()]
        else:
            iterator = custom_readline(fd, self.ARGS.eom)
        try:
            for line in iterator:
                yield self.parseMessage(line)
        except KeyboardInterrupt:
            sys.stderr.write("(INFO) Interrupted by user.\n")
            sys.exit()

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
                self.output(m)
        else:
            raise TypeError("Stage.output() expects parameter to be of type"
                            " %s or %s (got %s)"
                            % (self.__output_message_class, list,
                               type(message))
                            )

    def forward(self):
        """ Send EOPMarker to the output stream. """
        if isinstance(self.__output, file):
            fd = self.__output
        else:
            fd = self.__output.next()
        fd.write(self.ARGS.eop)

    def flush_buffer(self):
        """ Flush message buffer to the output. """
        if self.ARGS.dest == 's':
            self.stream_flush()
        else:
            self.file_flush()
        self.clear_buffer()

    def stream_flush(self, fd=None):
        """ Flush message buffer as a stream. """
        if not fd:
            fd = self.__output
        for msg in self.__output_buffer:
            fd.write(msg.encode())
            fd.write(self.ARGS.eom)

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
            self.log("Failed to get list of files.\n"
                     "Error message: %s" % err, logLevel.ERROR)
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

    def get_out_dir(self, t='l'):
        """ Get current output directory name. """
        if t not in ('l', 'h'):
            raise ValueError("get_out_dir() method expects values: 'l','h'"
                             " (got '%s')" % t)
        result = self.ARGS.output_dir
        cf = self.__current_file_full
        if not result and cf:
            if t == 'l':
                result = os.path.dirname(cf)
            else:
                result = hdfs.dirname(cf)
        if not result:
            if t == 'l':
                result = os.getcwd()
            else:
                result = hdfs.join(hdfs.DKB_HOME, 'temp',
                                   str(int(time.time())))
            self.log("Output dir set to: %s" % result)
            self.ARGS.output_dir = result
        return result

    def ensure_out_dir(self, t='l'):
        """ Ensure that current output directory exists. """
        if t not in ('l', 'h'):
            raise ValueError("ensure_out_dir() method expects values: 'l','h'"
                             " (got '%s')" % t)
        dirname = self.get_out_dir(t)
        if t == 'l':
            if not os.path.isdir(dirname):
                try:
                    os.makedirs(dirname, 0770)
                except OSError, err:
                    self.log("Failed to create output directory\n"
                             "Error message: %s\n" % err, logLevel.ERROR)
                    raise DataflowException
        else:
            hdfs.makedirs(dirname)
        return dirname

    def get_out_filename(self):
        """ Get output filename, corresponding current data source. """
        ext = self.output_message_class().extension()
        f = self.__current_file
        if f and f != sys.stdin.name:
            result = os.path.splitext(f)[0] + ext
        else:
            result = str(int(time.time())) + ext
        return result

    def __out_files(self, t='l'):
        """ Generator for file descriptors to write data to.

        Parameters:
            t {'l'|'h'} -- (l)ocal or (h)dfs
        """
        if t not in ('l', 'h'):
            raise ValueError("parameter t acceptable values: 'l','h'"
                             " (got '%s')" % t)
        fd = None
        cf = None
        try:
            while self.__current_file_full:
                if cf == self.__current_file_full:
                    yield fd
                    continue
                cf = self.__current_file_full
                output_dir = self.ensure_out_dir(t)
                filename = self.get_out_filename()
                if t == 'l':
                    local_path = os.path.join(output_dir, filename)
                else:
                    local_path = filename
                    hdfs_path = hdfs.join(output_dir, filename)
                if os.path.exists(local_path):
                    if fd and os.path.samefile(local_path, fd.name):
                        yield fd
                        continue
                    else:
                        raise DataflowException("File already exists: %s\n"
                                                % local_path)
                if fd:
                    fd.close()
                    if t == 'h':
                        hdfs.movefile(fd.name, hdfs_path)
                fd = open(full_path, "w", 0)
                yield fd
        finally:
            if fd:
                fd.close()
                if t == 'h':
                    hdfs.movefile(fd.name, hdfs_path)

    def __hdfs_in_dir(self):
        """ Call file descriptors generator for files in HDFS dir. """
        dirname = self.ARGS.input_dir
        try:
            files = hdfs.listdir(dirname, "f")
        except hdfs.HDFSException, err:
            self.log("Failed to get list of files.\n"
                     "Error message: %s" % err, logLevel.ERROR)
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
                f = hdfs.join(self.ARGS.input_dir, f)
            if not f:
                continue

            with hdfs.File(f) as infile:
                self.__current_file_full = f
                self.__current_file = hdfs.basename(f)
                yield infile

            self.__current_file = None
            self.__current_file_full = None

    def __stoppable_append(self, obj, cls):
        """ Appends OBJ (of type CLS) to the list of STOPPABLE. """
        if isinstance(obj, cls):
            self.__stoppable.append(obj)
