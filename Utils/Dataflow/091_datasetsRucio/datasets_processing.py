#!/bin/env python
"""
DKB Dataflow Stage 091 (datasetsRucio)

Get metadata abourt datasets from Rucio.
Input: from Stage 009.
Output: to Stage 019.

Authors:
  Maria Grigorieva (maria.grigorieva@cern.ch)
  Marina Golosova (golosova.marina@cern.ch)
"""

import sys
import os

base_dir = os.path.abspath(os.path.dirname(__file__))

try:
    if not os.environ.get("VIRTUAL_ENV", None):
        user_rucio_dir = os.path.expanduser("~/.rucio")
        if os.path.exists(user_rucio_dir):
            os.environ["VIRTUAL_ENV"] = os.path.join(user_rucio_dir)
        else:
            os.environ["VIRTUAL_ENV"] = os.path.join(base_dir, ".rucio")
        sys.stderr.write("(TRACE) Set VIRTUAL_ENV: %s\n"
                         % os.environ["VIRTUAL_ENV"])
    import rucio.client
    from rucio.common.exception import RucioException
except ImportError, err:
    sys.stderr.write("(ERROR) Failed to import Rucio module: %s\n" % err)
    sys.exit(1)

try:
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)

rucio_client = None
DS_TYPE = 'output'


def main(argv):
    """ Program body. """
    stage = pyDKB.dataflow.stage.JSONProcessorStage()
    exit_code = 0
    try:
        stage.parse_args(argv)
        stage.process = process
        init_rucio_client()
        stage.run()
    except (pyDKB.dataflow.exceptions.DataflowException, RuntimeError), err:
        if str(err):
            str_err = str(err).replace("\n", "\n(==) ")
            sys.stderr.write("(ERROR) %s\n" % str_err)
        exit_code = 2
    finally:
        stage.stop()

    sys.exit(exit_code)


def init_rucio_client():
    """ Initialize global variable `rucio_client`. """
    global rucio_client
    try:
        rucio_client = rucio.client.Client()
    except RucioException as err:
        sys.stderr.write("(ERROR) Failed to initialize Rucio client.\n")
        err_str = str(err).replace("\n", "\n(==) ")
        sys.stderr.write("(ERROR) %s.\n" % err_str)
        sys.exit(1)


def process(stage, message):
    """ Process input message.

    Generate output JSON document of the following structure:
        { "taskid": <TASKID>,
          "output": []
        }
    """
    json_str = message.content()

    if not json_str.get(DS_TYPE):
        # Nothing to process; over.
        return True

    datasets = json_str[DS_TYPE]
    if type(datasets) != list:
        datasets = [datasets]

    for dataset in datasets:
        ds = get_dataset_info(dataset)
        ds['taskid'] = json_str.get('taskid')
        if not add_es_index_info(ds):
            sys.stderr.write("(WARN) Skip message (not enough info"
                             " for ES indexing).\n")
            return True
        del(ds['taskid'])
        stage.output(pyDKB.dataflow.messages.JSONMessage(ds))

    return True


def get_dataset_info(dataset):
    """ Construct dictionary with dataset info.

    Dict format:
        {"deleted": true | false,
         "datasetname": "<DS_NAME>",
         "bytes": <BYTES>}
    :param dataset: dataset name
    :return: dict
    """
    ds_dict = {}
    ds_dict['datasetname'] = dataset
    try:
        bytes = get_metadata_attribute(dataset, 'bytes')
        if bytes == 'null' or bytes is None:
            ds_dict['bytes'] = -1
        else:
            ds_dict['bytes'] = bytes
            ds_dict['deleted'] = False
    except RucioException:
        # if dataset wasn't find in Rucio, it means that it was deleted from
        # the Rucio catalog. In this case 'deleted' is set to TRUE and
        # the length of file is set to -1
        ds_dict['bytes'] = -1
        ds_dict['deleted'] = True
    return ds_dict


def extract_scope(dsn):
    """ Extract the first field from the dataset name

    Example:
      mc15_13TeV.XXX
      mc15_13TeV:YYY.XXX

    :param dsn: full dataset name
    :return tuple: dataset scope, dataset name
    """
    pos = dsn.find(':')
    if pos > -1:
        result = (dsn[:pos], dsn.split[(pos + 1):])
    else:
        scope = dsn.split('.')[0]
        if dsn.startswith('user') or dsn.startswith('group'):
            scope = '.'.join(dsn.split('.')[0:2])
        result = (scope, dsn)
    return result


def get_metadata_attribute(dsn, attribute_name):
    """ Get attribute value from Rucio

    :param dsn: full dataset name
    :param attribute_name: name of searchable attribute
    :return:
    """
    scope, dataset = extract_scope(dsn)
    metadata = rucio_client.get_metadata(scope=scope, name=dataset)
    return metadata.get(attribute_name, None)


def add_es_index_info(data):
    """ Update data with required for ES indexing info.

    Add fields:
      _id => datasetname
      _type => 'output_dataset'
      _parent => taskid

    Return value:
      False -- update failed, skip the record
      True  -- update successful
    """
    if type(data) is not dict:
        return False
    if not (data.get('datasetname') and data.get('taskid')):
        return False
    data['_id'] = data['datasetname']
    data['_type'] = 'output_dataset'
    data['_parent'] = data['taskid']
    return True


if __name__ == '__main__':
    main(sys.argv[1:])
