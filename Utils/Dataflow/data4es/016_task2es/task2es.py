#!/bin/env python
"""
DKB Dataflow stage 016 (task2es)

Update task metadata (from ProdSys) with service fields for ES indexing.

Authors:
  Marina Golosova (marina.golosova@cern.ch)
  Vasilii Aulov (vasilii.aulov@cern.ch)
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
    from pyDKB.dataflow import messageType
    from pyDKB.dataflow.exceptions import DataflowException
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


def add_es_index_info(data):
    """ Update data with required for ES indexing info.

    Add fields:
      _id => taskid
      _type => 'task'

    Return value:
      False -- update failed, skip the record
      True  -- update successful
    """
    if type(data) is not dict:
        return False
    if not data.get('taskid'):
        return False
    data['_id'] = data['taskid']
    data['_type'] = 'task'
    return True


def process(stage, message):
    """ Single message processing. """
    data = message.content()

    if not add_es_index_info(data):
        sys.stderr.write("(WARN) Skip message (not enough info"
                         " for ES indexing).\n")
        return False

    out_message = JSONMessage(data)
    stage.output(out_message)
    return True


def main(args):
    """ Program body. """
    stage = ProcessorStage()
    stage.set_input_message_type(messageType.JSON)
    stage.set_output_message_type(messageType.JSON)

    stage.process = process

    stage.configure(args)
    error_code = stage.run()

    if error_code == 0:
        stage.stop()

    exit(error_code)


if __name__ == '__main__':
    main(sys.argv[1:])
