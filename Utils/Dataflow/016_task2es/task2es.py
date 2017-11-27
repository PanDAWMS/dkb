#!/bin/env python
"""
DKB Dataflow stage 016 (task2es)

Transform task metadata (from ProdSys) to fit ES scheme.

Authors:
  Marina Golosova (marina.golosova@cern.ch)
"""

import os
import sys
import traceback

try:
    base_dir = os.path.dirname(__file__)
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
    from pyDKB.dataflow.stage import JSONProcessorStage
    from pyDKB.dataflow.messages import JSONMessage
    from pyDKB.dataflow.exceptions import DataflowException
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


def process(stage, message):
    """ Single message processing. """
    data = message.content()
    # Processing machinery
    out_data = {"key": "value"}
    out_message = JSONMessage(out_data)
    stage.output(out_message)
    return True


def main(args):
    """ Program body. """
    stage = JSONProcessorStage()
    stage.process = process

    exit_code = 0
    exc_info = None
    try:
        stage.parse_args(args)
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
