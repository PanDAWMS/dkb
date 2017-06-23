#!/usr/bin/env python

"""
Stage 018: download PDF files from CDS and upload them to HDFS
"""

import sys
sys.path.append("../")

import pyDKB
from pyDKB.dataflow import DataflowException
from pyDKB.dataflow.stage import JSONProcessorStage

def process(stage, msg):
    """ Message processing function.

    Input message: JSON
    Output message: JSON
    """
    myMessage = stage.output_message_class()(msg.content())
    stage.output(myMessage)
    return True

def main(args):
    """ Main function. """
    stage = JSONProcessorStage()
    stage.process = process

    exit_code = 0
    try:
        stage.parse_args(args)
        stage.run()
    except (DataflowException, RuntimeError), err:
        if str(err):
            sys.stderr.write("(ERROR) %s\n" % err)
        exit_code = 1
    finally:
        stage.stop()

    exit(exit_code)

if __name__ == "__main__":
    main(sys.argv[1:])
