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
import traceback

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
    from rucio.common.exception import (RucioException,
                                        DataIdentifierNotFound)
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
        DataIdentifierNotFound = None

try:
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
    from pyDKB.dataflow import messageType
    from pyDKB.dataflow.exceptions import DataflowException
    from pyDKB.common.types import logLevel
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

    stage.configure(argv)
    stage.process = process
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
    except IOError as err:
        # When Client fails to read the certificate files for some reason,
        # it does not handle the IOError in any way, so we have to read
        # a pretty long traceback to realise what's the problem.
        # This is an attempt to detect a familiar situation -- problem with
        # the certificate files -- and distinguish it from any other possible
        # `IOError`s.
        tb = traceback.extract_tb(sys.exc_info()[2])
        if '.load_cert_chain(' in tb[-1][-1]:
            # The innermost context to which we can get
            # is the line containing 'load_cert_chain' method call
            raise DataflowException("Failed to inilialize Rucio client:"
                                    " can not load certificate.", reason=err)
        raise
    except RucioException as err:
        raise DataflowException("Failed to initialize Rucio client: %s" % err)


def get_rucio_client():
    """ Get initialized Rucio client. """
    if not rucio_client:
        init_rucio_client()
    return rucio_client


def process(stage, message):
    """ Process input message (both input and output datasets).

    Output JSON will contain same fields as the input and additional ones:

    ```
    {
      "output_dataset": [
        {"name": ..., "events": ..., "bytes": ..., "deleted": ...},
        ...
      ],
      "input_bytes": ...,
      "primary_input_events": ...,
      "primary_input_deleted": ...
    }
    ```

    :param stage: Processor stage for which this function is a method
    :type stage: :py:class:`pyDKB.dataflow.stage.ProcessorStage`
    :param message: input message to be processed
    :type message: :py:class:
                   `pyDKB.dataflow.communication.messages.AbstractMessage`

    :return: processing status: True(success)/False(failure)
    :rtype: bool
    """
    data = message.content()

    incompl = None
    input_ds = process_input_ds(data.get(SRC_FIELD[INPUT]))
    if input_ds:
        if not input_ds.pop('_status'):
            incompl = True
        data.update(input_ds)

    output_ds = process_output_ds(data.get(SRC_FIELD[OUTPUT]))
    if output_ds:
        data['output_dataset'] = output_ds
        for ds in output_ds:
            if not ds.pop('_status', True):
                incompl = True

    msg = stage.output_message_class()(data)
    msg.incomplete(incompl)

    stage.output(msg)
    return True


def process_output_ds(datasets):
    """ Process output datasets.

    Generate output JSON documents of the following structure:
        { "name": <DSNAME>,
          "deleted": <bool>,
          "events": <...>,
          "bytes": <...>
        }

    :param datasets: list of DS names
    :type datasets: list

    :return: list of documents with DS metadata,
             each with service field ``_status``
             representing processing status -- if it
             was successful (True) or failed (False)
    :rtype: list
    """
    if not datasets:
        # Nothing to process; over.
        return None

    if type(datasets) != list:
        datasets = [datasets]

    mfields = META_FIELDS[OUTPUT]
    result = []
    for ds_name in datasets:
        status = True
        try:
            ds = get_ds_info(ds_name, mfields)
        except RucioException, err:
            stage.log(["Failed to get information"
                       " from Rucio for: %s." % ds_name,
                       "Reason: %s." % str(err)],
                      logLevel.WARN)
            status = False
            ds = {}
        ds['name'] = ds_name
        if not is_data_complete(ds, mfields.values()):
            status = False
        ds['_status'] = status
        result.append(ds)
    return result


def process_input_ds(ds_name):
    """ Process input dataset from input message.

    Add to original JSON fields:
     * bytes
     * deleted

    :param ds_name: dataset name
    :type ds_name: str

    :return: dataset metadata with service field ``_status``
             for processing status: successful (True) or failed (False)
    :rtype: dict
    """
    mfields = META_FIELDS[INPUT]
    status = True
    ds = None
    if ds_name:
        try:
            ds = get_ds_info(ds_name, mfields)
        except RucioException, err:
            stage.log(["Failed to get information"
                       " from Rucio for: %s." % ds_name,
                       "Reason: %s." % str(err)],
                      logLevel.WARN)
            status = False
            ds = {}
        if not is_data_complete(ds, mfields.values()):
            status = False
        ds['_status'] = status

    return ds


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
        for mkey in mdata:
            if mkey in mfields:
                ds_dict[mfields[mkey]] = mdata[mkey]
    except DataIdentifierNotFound, err:
        ds_dict[mfields['deleted']] = True
    return ds_dict


def extract_scope(dsn):
    """ Extract the first field from the dataset name

    Example:
      mc15_13TeV.XXX
      mc15_13TeV:YYY.XXX

    :param dsn: full dataset name
    :type dsn: str

    :return: dataset scope
    :rtype: str
    """
    pos = dsn.find(':')
    if pos > -1:
        result = dsn[:pos]
    else:
        scope = dsn.split('.')[0]
        if dsn.startswith('user') or dsn.startswith('group'):
            scope = '.'.join(dsn.split('.')[0:2])
        result = scope
    return result


def normalize_dataset_name(dsn):
    """ Remove an explicitly stated scope from a dataset name.

    According to dataset nomenclature, dataset name cannot include
    a ':' symbol. If a dataset name is in 'A:B' format, then A,
    probably, is an explicitly stated scope that should be removed.

    :param dsn: dataset name
    :type dsn: str

    :return: dataset name without explicit scope,
             unchanged dataset name if it was already normal
    :rtype: str
    """
    pos = dsn.find(':')
    if pos > -1:
        result = dsn[(pos + 1):]
    else:
        result = dsn
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
    scope = extract_scope(dsn)
    dataset = normalize_dataset_name(dsn)
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
            del(mdata[key])
    if 'bytes' in mdata.keys():
        val = mdata['bytes']
        if val is None:
            # 'bytes' is None when, e.g., dataset is a container, and all the
            # datasets within the container are already deleted
            # (yet the container itself still exists)
            mdata['deleted'] = True
        else:
            mdata['deleted'] = False
    return mdata


def is_data_complete(data, fields):
    """ Check if data contains all the required fields.

    :param data: data to be checked
    :type data: dict
    :param fields: list of fields data must contain
    :type fields: list

    :return: True/False
    :rtype: bool
    """
    return set(fields).issubset(set(data.keys()))


if __name__ == '__main__':
    main(sys.argv[1:])
