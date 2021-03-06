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
except Exception, err:
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
    msg["data_format"] = dataset_format(msg.get('datasetname'))
    stage.output(pyDKB.dataflow.communication.messages.JSONMessage(msg))

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

    In case of complex data formats, like 'DAOD_SUSY5',
    the field is splitted by '_' and returns it's full name
    and first part ('DAOD'), defining the general name of the data format.

    :param datasetname:
    :return: list
    """
    if not datasetname:
        return None
    ds_format = pyDKB.atlas.misc.dataset_data_format(datasetname)
    if ds_format and re.match(r'\w+_\w+', ds_format) is not None:
        result = [ds_format, ds_format.split('_')[0]]
    elif ds_format:
        result = [ds_format]
    else:
        result = []
    return result


if __name__ == '__main__':
    main(sys.argv[1:])
