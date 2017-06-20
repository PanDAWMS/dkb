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
  myMessage = pyDKB.dataflow.Message(pyDKB.dataflow.messageType.TTL)(msg.content())
  stage.output(myMessage)
  return True

def main(args):
  stage = pyDKB.dataflow.stage.JSON2TTLProcessorStage()
  stage.process = process

  stage.parse_args(args)
  stage.run()
  stage.stop()

if __name__ == '__main__':
  main(sys.argv[1:])
