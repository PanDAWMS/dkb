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


def throw_error():
    """ Throw an exception, for testing purposes."""
    a = b + c


def process(stage, message):
    """ Single message processing. """
    data = message.content()
    # Processing machinery
    if data.get('ERROR'):
        throw_error()
    if data.get('DISCARD'):
        stage.log("Discarding data %s because it's very wrong."
                  % data, logLevel.WARN)
        return True
    if 'df' in data and isinstance(data['df'], (str, unicode)):
        data['df'] = 'processed ' + data['df']
    else:
        stage.log("Failed to process data %s, required field 'df' not found"
                  " or contains non-str value." % data, logLevel.WARN)
    out_message = JSONMessage(data)
    stage.output(out_message)
    return True


def batch_process(stage, messages):
    """ Batch of messages processing. """
    batch_data = [message.content() for message in messages]
    # Optimized machinery for processing bunches of messages
    new_batch = []
    for data in batch_data:
        if data.get('ERROR'):
            throw_error()
        if data.get('DISCARD'):
            stage.log("Discarding data %s because it's very wrong."
                      % data, logLevel.WARN)
            continue
        if 'df' in data and isinstance(data['df'], (str, unicode)):
            data['df'] = 'processed ' + data['df']
        else:
            stage.log("Failed to process data %s, required field 'df' not"
                      " found or contains non-str"
                      " value." % data, logLevel.WARN)
        new_batch.append(data)
    for msg in new_batch:
        out_message = JSONMessage(msg)
        stage.output(out_message)
    return True


def main(args):
    """ Program body. """
    stage = ProcessorStage()
    stage.set_input_message_type(messageType.JSON)
    stage.set_output_message_type(messageType.JSON)

    stage.process = process
    stage.batch_process = batch_process

    exit_code = 0

    stage.configure(args)
    stage.run()

    exit(exit_code)


if __name__ == '__main__':
    main(sys.argv[1:])
