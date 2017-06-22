#!/usr/bin/env python

"""
Stage 0XX: some json data to TTL & SPARQL
"""

import sys
sys.path.append("../../")

import pyDKB

def process(stage, msg):
    """
    Input message: JSON
    Output message: TTL
    """
    cls = pyDKB.dataflow.Message(pyDKB.dataflow.messageType.TTL)
    myMessage = cls(msg.content())
    stage.output(myMessage)
    return True

def main(args):
    """ Main program loop. """
    stage = pyDKB.dataflow.stage.JSON2TTLProcessorStage()
    stage.process = process

    try:
        stage.parse_args(args)
    except pyDKB.dataflow.DataflowException:
        exit(1)
    stage.run()
    stage.stop()

if __name__ == '__main__':
    main(sys.argv[1:])
