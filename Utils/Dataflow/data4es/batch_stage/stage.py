#!/bin/env python
"""
DKB Dataflow stage XXX (StageName).

Stage short description

Authors:
  Author Name (author@cern.ch)
"""

import os
import sys
import traceback

try:
    base_dir = os.path.dirname(__file__)
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
    from pyDKB.dataflow.stage import ProcessorStage
    from pyDKB.dataflow.communication.messages import JSONMessage
    from pyDKB.dataflow.exceptions import DataflowException
    from pyDKB.dataflow import messageType
    from pyDKB.common.types import logLevel
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


def process(stage, messages):
    """ Single or batch message processing.

    This form of batch processing is pretty pointless in terms of efficiency:
    using it will replace, for example, ProcessorStage cycling over 100
    messages with it cycling over 10 batches, and this stage cycling
    over 10 messages in each batch. But for testing and illustrative purposes
    it will do.
    """
    if not isinstance(messages, list):
        messages = [messages]
    for message in messages:
        data = message.content()
        if not isinstance(data, dict):
            stage.log("Cannot process non-dict data: %s." % data,
                      logLevel.WARN)
            continue
        # Processing machinery
        if 'df' in data and isinstance(data['df'], (str, unicode)):
            data['df'] = 'processed ' + data['df']
        else:
            stage.log("Failed to process data %s, required field 'df' not"
                      " found or contains non-str value." % data,
                      logLevel.WARN)
        out_message = JSONMessage(data)
        stage.output(out_message)
    return True


def main(args):
    """ Program body. """
    stage = ProcessorStage()
    stage.set_input_message_type(messageType.JSON)
    stage.set_output_message_type(messageType.JSON)
    stage.set_default_arguments(bnc='')

    stage.process = process

    exit_code = 0
    exc_info = None
    try:
        stage.configure(args)
        stage.run()
    except (DataflowException, RuntimeError), err:
        if str(err):
            sys.stderr.write("(ERROR) %s\n" % err)
        exit_code = 2
    except Exception:
        exc_info = sys.exc_info()
        exit_code = 3
    finally:
        stage.stop()

    if exc_info:
        trace = traceback.format_exception(*exc_info)
        for line in trace:
            sys.stderr.write("(ERROR) %s" % line)

    exit(exit_code)


if __name__ == '__main__':
    main(sys.argv[1:])
