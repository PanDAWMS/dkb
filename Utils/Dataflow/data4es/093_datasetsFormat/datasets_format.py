#!/bin/env python
"""
Add 'data_format' field, extracted from datasetname
"""

import sys
import os
import re

base_dir = os.path.abspath(os.path.dirname(__file__))

try:
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
    from pyDKB.dataflow import messageType
    from pyDKB import atlas
except Exception as err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


def main(argv):
    """ Program body. """
    stage = pyDKB.dataflow.stage.ProcessorStage()
    stage.set_input_message_type(messageType.JSON)
    stage.set_output_message_type(messageType.JSON)

    stage.configure(argv)
    stage.process = process
    exit_code = stage.run()

    if exit_code == 0:
        stage.stop()

    sys.exit(exit_code)


def process(stage, message):
    """ Process input message.
    """
    msg = message.content()
    datasets = msg.get('output_dataset', [])
    for ds in datasets:
        ds['data_format'] = atlas.misc.dataset_data_format(ds.get('name'))
    stage.output(pyDKB.dataflow.communication.messages.JSONMessage(msg))

    return True


if __name__ == '__main__':
    main(sys.argv[1:])
