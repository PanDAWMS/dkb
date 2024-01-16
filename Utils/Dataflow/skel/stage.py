#!/bin/env python
"""
DKB Dataflow stage XXX (StageName).

Stage short description

Authors:
  Author Name (author@cern.ch)
"""

import os
import sys

try:
    base_dir = os.path.dirname(__file__)
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
    from pyDKB.dataflow.stage import ProcessorStage
    from pyDKB.dataflow.communication.messages import JSONMessage
    from pyDKB.dataflow.exceptions import DataflowException
    from pyDKB.dataflow import messageType
except Exception as err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


def process(stage, message):
    """ Single message processing.

    :param stage: ETL processing stage
    :type stage: pyDKB.dataflow.stage.ProcessorStage
    :param message: input message with data to be processed
    :type message: pyDKB.dataflow.communication.messages.JSONMessage

    :returns: True or False in case of failure
    :rtype: bool
    """
    data = message.content()
    # Processing machinery
    out_data = {"key": "value"}
    out_message = JSONMessage(out_data)
    stage.output(out_message)
    return True


def main(args):
    """ Program body. """
    stage = ProcessorStage()
    stage.set_input_message_type(messageType.JSON)
    stage.set_output_message_type(messageType.JSON)

    stage.process = process

    stage.configure(args)

    exit_code = stage.run()

    if exit_code == 0:
        stage.stop()

    exit(exit_code)


if __name__ == '__main__':
    main(sys.argv[1:])
