#!/bin/env python
"""
Add 'data_format' field, extracted from datasetname
"""

import sys
import os

base_dir = os.path.abspath(os.path.dirname(__file__))

try:
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


def main(argv):
    """ Program body. """
    stage = pyDKB.dataflow.stage.JSONProcessorStage()
    exit_code = 0
    try:
        stage.parse_args(argv)
        stage.process = process
        stage.run()
    except (pyDKB.dataflow.exceptions.DataflowException, RuntimeError), err:
        if str(err):
            str_err = str(err).replace("\n", "\n(==) ")
            sys.stderr.write("(ERROR) %s\n" % str_err)
        exit_code = 2
    finally:
        stage.stop()

    sys.exit(exit_code)


def process(stage, message):
    """ Process input message.
    """
    msg = message.content()
    msg["data_format"] = dataset_format(msg.get('datasetname'))
    stage.output(pyDKB.dataflow.messages.JSONMessage(msg))

    return True


def dataset_format(datasetname):
    """
    Extract data format from datasetname
    According to dataset naming nomenclature:
    https://dune.bnl.gov/w/images/9/9e/Gen-int-2007-001_%28NOMENCLATURE%29.pdf
    for MC datasets:
        mcNN_subProject.datasetNumber.physicsShort.prodStep.dataType.Version
    for Real Data:
        DataNN_subProject.runNumber.streamName.prodStep.dataType.Version
    In both cases the dataType filed is required.
    :param datasetname:
    :return: str
    """
    return datasetname.split('.')[4]


if __name__ == '__main__':
    main(sys.argv[1:])
