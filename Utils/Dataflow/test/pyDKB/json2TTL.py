#!/usr/bin/env python

"""
Stage 0XX: some json data to TTL & SPARQL
"""

import sys
import os
import json

try:
    base_dir = os.path.dirname(__file__)
    dkb_dir = os.path.join(base_dir, os.pardir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


def stop_process(msg):
    """ Raise KeyError. """
    raise KeyError(msg)


def pre_stop_process(msg, n=3):
    """ Call itself N times and then all stop_process(). """
    if n > 0:
        pre_stop_process(msg, n - 1)
    else:
        stop_process(msg)


def process(stage, msg):
    """
    Input message: JSON
    Output message: TTL
    """
    cls = pyDKB.dataflow.Message(pyDKB.dataflow.messageType.TTL)
    myMessage = cls(msg.content())
    if "stop" in msg.content():
        pre_stop_process("Key 'stop' in input message.")
    stage.output(myMessage)
    return True


def main(args):
    """ Main program loop. """
    stage = pyDKB.dataflow.stage.JSON2TTLProcessorStage()
    stage.process = process

    stage.parse_args(args)
    if stage.CONFIG:
        str_config = json.dumps(stage.CONFIG, indent=2)
        labeled_config = "(==) " + str_config.replace('\n', '\n(==) ')
        sys.stderr.write("(DEBUG) Config:\n%s\n" % labeled_config)

    if stage.ARGS.source == 's':
        sys.stderr.write("""
Type '{"stop": ""}' to interrupt.
""")
    exit_code = stage.run()

    if exit_code == 0:
        stage.stop()

    exit(exit_code)


if __name__ == '__main__':
    main(sys.argv[1:])
