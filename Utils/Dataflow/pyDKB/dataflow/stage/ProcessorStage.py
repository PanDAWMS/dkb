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

from . import AbstractStage
from . import messageType
from . import logLevel
from pyDKB.dataflow import DataflowException
from pyDKB.common import hdfs
from pyDKB.common import custom_readline
from pyDKB.dataflow import communication
from pyDKB.dataflow.communication import stream
from pyDKB.dataflow.communication import consumer
from pyDKB.dataflow.communication import producer


class ProcessorStage(AbstractStage):
    """ Abstract class to implement Processor stages

    Processor stage -- is a stage for data processing/transformation.

    Class/instance variable description:

    * communication.consumer.Consumer instance
        __input

    * Generator object for output file descriptor
      OR file descriptor (for (s)tream mode)
        __output

    * List of objects to be "stopped"
        __stoppable
    """

    __input_message_type = None
    __output_message_type = None

    __input = None
    _out_stream = None

    def __init__(self, description="DKB Dataflow data processing stage."):
        """ Initialize the stage

        * set description
        * define common Processor arguments:
            input, --hdfs, --hdfs-dir, --output, ...
        * ...
        """
        self.__stoppable = []
        super(ProcessorStage, self).__init__(description)

    def set_input_message_type(self, Type=None):
        """ Set input message type. """
        if not messageType.hasMember(Type):
            raise ValueError("Unknown message type: %s" % Type)
        self.__input_message_type = Type
        if self.__input:
            self.__input.set_message_type(Type)

    def input_message_class(self):
        """ Get input message class. """
        return communication.Message(self.__input_message_type)

    def set_output_message_type(self, Type=None):
        """ Set output message class. """
        if not messageType.hasMember(Type):
            raise ValueError("Unknown message type: %s" % Type)
        self.__output_message_type = Type

    def output_message_class(self):
        """ Get output message class. """
        return communication.Message(self.__output_message_type)

    def defaultArguments(self):
        """ Default parser configuration. """
        super(ProcessorStage, self).defaultArguments()
        self.add_argument('input_files', type=str, nargs='*',
                          help=u'source data file\n'
                          'NOTE: required in (f)ile, '
                          'ignored in other modes',
                          metavar=u'FILE'
                          )
        self.add_argument('-s', '--source', action='store', type=str,
                          help=u'where to get data from:\n'
                          '    f -- local files\n'
                          '    s -- stdin\n'
                          '    h -- hdfs files',
                          default='f',
                          choices=['f', 's', 'h'],
                          dest='source'
                          )
        self.add_argument('-i', '--input-dir', action='store', type=str,
                          help=u'directory with input files (local or HDFS), '
                          'or initial directory for relative FILE names. '
                          'If no FILE is specified, all files with '
                          'extension matching input message type will '
                          'be taken from %%(metavar)s\n'
                          'DEFAULT: %s' % os.curdir,
                          default=None,
                          metavar='DIR',
                          dest='input_dir'
                          )
        self.add_argument('-d', '--dest', action='store', type=str,
                          help=u'where to write results:\n'
                          '    f -- local files\n'
                          '    s -- stdout\n'
                          '    h -- hdfs files',
                          default='f',
                          choices=['f', 's', 'h'],
                          dest='dest'
                          )
        self.add_argument('-o', '--output-dir', action='store', type=str,
                          help=u'directory for output files '
                          '(local or HDFS). %%(metavar)s can be:\n'
                          ' * absolute path\n'
                          ' * relative path (for local files)\n'
                          ' * subpath (not absolute nor relative).\n'
                          'In case of subpath (or relative path for '
                          'HDFS output) %%(metavar)s is taken '
                          'as relative to the one of:\n'
                          ' * directory with input files\n'
                          ' * current directory (for local files)\n'
                          ' * \'%stemp\' (for HDFS)' % hdfs.DKB_HOME,
                          default='out',
                          metavar='DIR',
                          dest='output_dir'
                          )
        self.add_argument('--hdfs', action='store_true',
                          help=u'equivalent to "--source h --dest h".\n'
                          'Explicit specification of '
                          '"--source" and "--dest" as well as the default '
                          'values for current processing mode ("--mode") will '
                          'be ignored',
                          default=False,
                          dest='hdfs'
                          )

    def configure(self, args=None):
        """ Configure stage according to the config parameters.

        If $args specified, arguments will be parsed anew.
        """
        if args:
            self.parse_args(args)
        elif self.ARGS is None:
            # Need to initialize ARGS to proceed
            self.parse_args([])
        try:
            # Input
            self.__input = consumer.ConsumerBuilder(vars(self.ARGS)) \
                .setType(self.__input_message_type) \
                .build()
            self.__stoppable_append(self.__input, consumer.Consumer)
            # Output
            self.__output = producer.ProducerBuilder(vars(self.ARGS)) \
                .setType(self.__output_message_type) \
                .setSourceInfoMethod(self.get_source_info) \
                .build()
            self.__stoppable_append(self.__output, producer.Producer)
        except (consumer.ConsumerException, producer.ProducerException), err:
            self.log(str(err), logLevel.ERROR)
            self.stop()
            sys.exit(1)
        self.log_configuration()

    def get_source_info(self):
        """ Get information about current source. """
        if self.__input:
            result = self.__input.get_source_info()
        else:
            result = None
        return result

    def run(self):
        """ Run process() for every input() message. """
        self.log("Starting stage execution.")
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
            exit_code = 1
            self.set_error(*sys.exc_info())
            try:
                self.clear_buffer()
            except DataflowException:
                # In case the previous error is related to the output
                # stream, we should skip this error here to exit the
                # program properly
                pass
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
                failures.append((p, sys.exc_info()))
        if failures:
            for f in failures:
                self.log("Failed to stop %s: %s" % (f[0], f[1][1]),
                         logLevel.ERROR)
                self.output_error(exc_info=f[1])

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

    def input(self):
        """ Generator for input messages.

        Returns iterable object.
        Every iteration returns single input message to be processed.
        """
        for r in self.__input:
            yield r

    def output(self, message):
        """ Put the (list of) message(s) to the output buffer. """
        self.__output.write(message)

    def forward(self):
        """ Send EOPMarker to the output stream. """
        self.__output.eop()

    def flush_buffer(self):
        """ Flush message buffer to the output. """
        self.__output.flush()

    def clear_buffer(self):
        """ Drop buffered output messages. """
        self.__output.drop()

    def __stoppable_append(self, obj, cls):
        """ Appends OBJ (of type CLS) to the list of STOPPABLE. """
        if isinstance(obj, cls):
            self.__stoppable.append(obj)
