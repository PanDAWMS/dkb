#!/bin/env python
"""
DKB Dataflow Stage 091 (datasetsRucio)

Get dataset metadata from Rucio.

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
except Exception, err:
    # rucio.client tries to read Rucio config file, and if it is not found,
    # throws plain Exception
    sys.stderr.write("(ERROR) %s.\n" % err.message)
finally:
    try:
        RucioException
    except NameError:
        RucioException = None

try:
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
    from pyDKB.dataflow import messageType
    from pyDKB.dataflow.exceptions import DataflowException
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)

rucio_client = None

OUTPUT = 'o'
INPUT = 'i'

META_FIELDS = {
    OUTPUT: {'bytes': 'bytes', 'events': 'events', 'deleted': 'deleted'},
    INPUT: {'bytes': 'input_bytes',
            'events': 'primary_input_events',
            'deleted': 'primary_input_deleted'
            }
}

SRC_FIELD = {
    OUTPUT: 'output',
    INPUT: 'primary_input'
}


def main(argv):
    """ Program body. """
    stage = pyDKB.dataflow.stage.ProcessorStage()
    stage.set_input_message_type(messageType.JSON)
    stage.set_output_message_type(messageType.JSON)
    stage.add_argument('-t', '--dataset-type', action='store', type=str,
                       help=u'Type of datasets to work with: (i)nput'
                             ' or (o)utput',
                       nargs='?',
                       default=OUTPUT,
                       choices=[INPUT, OUTPUT],
                       dest='ds_type'
                       )

    stage.configure(argv)
    if stage.ARGS.ds_type == OUTPUT:
        stage.process = process_output_ds
        stage.skip_process = skip_process_output_ds
    elif stage.ARGS.ds_type == INPUT:
        stage.process = process_input_ds
    exit_code = stage.run()

    if exit_code == 0:
        stage.stop()

    sys.exit(exit_code)


def init_rucio_client():
    """ Initialize global variable `rucio_client`. """
    global rucio_client
    try:
        rucio_client = rucio.client.Client()
    except NameError:
        sys.stderr.write("(FATAL) Failed to initialize Rucio client: "
                         "module not loaded.\n")
        raise DataflowException("Module not found or misconfigured: 'rucio'")
    except RucioException as err:
        sys.stderr.write("(ERROR) Failed to initialize Rucio client.\n")
        err_str = str(err).replace("\n", "\n(==) ")
        sys.stderr.write("(ERROR) %s.\n" % err_str)
        sys.exit(1)


def get_rucio_client():
    """ Get initialized Rucio client. """
    if not rucio_client:
        init_rucio_client()
    return rucio_client


def process_output_ds(stage, message):
    """ Process output datasets from input message.

    Generate output JSON document of the following structure:
        { "datasetname": <DSNAME>
          "deleted": <bool>,
          "bytes": <...>,
          ...
          "_type": "output_dataset",
          "_parent": <TASKID>,
          "_id": <DSNAME>
        }
    """
    json_str = message.content()

    if not json_str.get(SRC_FIELD[OUTPUT]):
        # Nothing to process; over.
        return True

    datasets = json_str[SRC_FIELD[OUTPUT]]
    if type(datasets) != list:
        datasets = [datasets]

    mfields = META_FIELDS[OUTPUT]
    for ds_name in datasets:
        ds = get_ds_info(ds_name, mfields)
        ds['datasetname'] = ds_name
        ds['taskid'] = json_str.get('taskid')
        if not add_es_index_info(ds):
            sys.stderr.write("(WARN) Skip message (not enough info"
                             " for ES indexing).\n")
            return True
        del(ds['taskid'])
        stage.output(pyDKB.dataflow.communication.messages.JSONMessage(ds))

    return True


def skip_process_output_ds(stage, message):
    """ Implementation of `ProcessorStage.skip_process()` method.

    Convert input message (representing task) into a set of messages
    representing the task output datasets.
    Each output message contains dataset UID (name) and service fields:
        { "datasetname": <DSNAME>,
          "_type": "output_dataset",
          "_parent": <TASKID>,
          "_id": <DSNAME>
        }
    """
    json_str = message.content()

    if not json_str.get(SRC_FIELD[OUTPUT]):
        # Nothing to process; over.
        return True

    datasets = json_str[SRC_FIELD[OUTPUT]]
    if type(datasets) != list:
        datasets = [datasets]

    for dataset in datasets:
        ds = {'datasetname': dataset}
        ds['taskid'] = json_str.get('taskid')
        if not add_es_index_info(ds):
            sys.stderr.write("(WARN) Skip message (not enough info"
                             " for ES indexing).\n")
            return True
        del(ds['taskid'])
        out_msg = pyDKB.dataflow.communication.messages.JSONMessage(ds)
        out_msg.incomplete(True)
        stage.output(out_msg)

    return True


def process_input_ds(stage, message):
    """ Process input dataset from input message.

    Add to original JSON fields:
     * bytes
     * deleted
    """
    data = message.content()
    mfields = META_FIELDS[INPUT]
    ds_name = data.get(SRC_FIELD[INPUT])
    if ds_name:
        ds = get_ds_info(ds_name, mfields)
        data.update(ds)
    stage.output(pyDKB.dataflow.communication.messages.JSONMessage(data))

    return True


def get_ds_info(dataset, mfields):
    """ Construct dictionary with dataset info.

    Dict format:
        {<deleted>: true | false,
         <bytes>: <BYTES>,
         <events>: <EVENTS>,
         ...}

    :param dataset: dataset name
    :type dataset: str
    :param mfields: fields to get from Rucio metadata
                    with aliases to be used instead of Rucio field names:
                    ``{<rucio_field>: <alias>, ...}``
    :type fields: dict

    :return: dict with dataset info
    :rtype: dict
    """
    ds_dict = {}
    try:
        mdata = get_metadata(dataset, mfields.keys())
        adjust_metadata(mdata)
        for mkey in mfields:
            ds_dict[mfields[mkey]] = mdata[mkey]
    except RucioException:
        # if dataset wasn't found in Rucio, it means that it has been deleted
        # from the Rucio catalog. In this case 'deleted' is set to TRUE and
        # the length of file is set to -1
        ds_dict[mfields['bytes']] = -1
        ds_dict[mfields['deleted']] = True
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
        result = (dsn[:pos], dsn[(pos + 1):])
    else:
        scope = dsn.split('.')[0]
        if dsn.startswith('user') or dsn.startswith('group'):
            scope = '.'.join(dsn.split('.')[0:2])
        result = (scope, dsn)
    return result


def get_metadata(dsn, attributes=None):
    """ Get attribute value from Rucio

    :param dsn: full dataset name
    :param attributes: attribute name OR
                       list of names of searchable attributes
                       (None = all attributes)
    :return: dataset metadata
    :rtype:  dict
    """
    rucio_client = get_rucio_client()
    scope, dataset = extract_scope(dsn)
    try:
        metadata = rucio_client.get_metadata(scope=scope, name=dataset)
    except ValueError, err:
        raise RucioException(err)
    if attributes is None:
        result = metadata
    else:
        result = {}
        if not isinstance(attributes, list):
            attributes = [attributes]
        for attribute_name in attributes:
            result[attribute_name] = metadata.get(attribute_name, None)
    return result


def adjust_metadata(mdata):
    """ Update metadata taken from Rucio with values required to proceed. """
    if not mdata:
        return mdata
    if not isinstance(mdata, dict):
        sys.stderr.write("(ERROR) adjust_metadata() expects parameter of type "
                         " 'dict' (get '%s')" % mdata.__class__.__name__)
    for key in mdata:
        if mdata[key] == 'null':
            mdata[key] = None
    if 'bytes' in mdata.keys():
        val = mdata['bytes']
        if val is None:
            mdata['bytes'] = -1
            mdata['deleted'] = True
        else:
            mdata['deleted'] = False
    return mdata


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
