#!/bin/env python
import json
import re
from ssl import SSLError
import sys
import os
try:
    import pyAMI.client
    import pyAMI.atlas.api as AtlasAPI
    import pyAMI.config
except ImportError:
    sys.stderr.write("(ERROR) Unable to find pyAMI client.\n")

base_dir = os.path.abspath(os.path.dirname(__file__))

try:
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
    from pyDKB.dataflow import messageType
    from pyDKB.dataflow.exceptions import DataflowException
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)

ami_client = None
# Field names in terms of AMI and ES schemes.
PHYS_VALUES = [{"ami": "genFiltEff", "es": "gen_filt_eff"},
               {"ami": "crossSection", "es": "cross_section"},
               {"ami": "crossSectionRef", "es": "cross_section_ref"},
               {"ami": "kFactor", "es": "k_factor"},
               {"ami": "processGroup", "es": "process_group"},
               {"ami": "mePDF", "es": "me_pdf"},
               ]
FILTER = ['AOD', 'EVNT', 'HITS']


def main(argv):
    """ Main program body.

    :param argv: arguments
    :type argv: list
    """
    stage = pyDKB.dataflow.stage.ProcessorStage()
    stage.set_input_message_type(messageType.JSON)
    stage.set_output_message_type(messageType.JSON)

    stage.add_argument('--userkey', help='PEM key file', default='')
    stage.add_argument('--usercert', help='PEM certificate file', default='')

    stage.configure(argv)
    stage.process = process
    if stage.ARGS.userkey and stage.ARGS.usercert:
        init_ami_client(stage.ARGS.userkey, stage.ARGS.usercert)
    exit_code = stage.run()

    if exit_code == 0:
        stage.stop()

    sys.exit(exit_code)


def init_ami_client(userkey='', usercert=''):
    """ Establish a connection to AMI.

    Initialize the global variable ami_client with the resulting
    client object.

    :param userkey: user key pem file
    :type userkey: str
    :param usercert: user certificate pem file
    :type usercert: str
    """
    global ami_client
    try:
        ami_client = pyAMI.client.Client('atlas', key_file=userkey,
                                         cert_file=usercert)
        AtlasAPI.init()
    except NameError:
        sys.stderr.write("(FATAL) Failed to initialise AMI client: "
                         "pyAMI module is not loaded.\n")
        raise DataflowException("Module not found: 'pyAMI'")
    except Exception, err:
        sys.stderr.write(
            "(ERROR) Could not establish pyAMI session."
            " Are you sure you have a valid certificate?\n")
        raise DataflowException(str(err))
    if ami_client.config.conn_mode == ami_client.config.CONN_MODE_LOGIN:
        sys.stderr.write("(ERROR) Login authentication mode is not "
                         "supported. Please provide user certificate or create"
                         "proxy.\n")
        raise DataflowException("Failed to initialise AMI client: certificate "
                                "not provided or not found.")
    try:
        ami_client.execute('ListCommands')
    except Exception as e:
        sys.stderr.write("(WARN) Failed to perform test command "
                         "ListCommands. Exception: %s\n" % str(e))


def get_ami_client():
    """ Get configured AMI client.

    :return: AMI client instance (global variable)
    :rtype: pyAMI.client.Client
    """
    if not ami_client:
        init_ami_client()
    return ami_client


def process(stage, message):
    """ Process a message.

    Implementation of :py:meth:`.ProcessorStage.process` for hooking
    the stage into DKB workflow.

    :param stage: stage instance
    :type stage: pyDKB.dataflow.stage.ProcessorStage
    :param message: input message with data
    :type message: pyDKB.dataflow.communication.messages.JSONMessage

    :return: False (failed to process message) or True (otherwise)
    :rtype: bool
    """
    data = message.content()
    if not isinstance(data, dict):
        sys.stderr.write("(WARN) Cannot update non-dict data: %r\n" % data)
        return False
    # 'data_format' field contains a list of strings,
    # e.g. ['DAOD_SUSY5', 'DAOD']
    formats = data.get('data_format', [])
    update = False
    for f in formats:
        if f in FILTER:
            update = True
    # Update data with information from AMI only if
    # 'data_format' list contains one of the allowed formats
    # or not set at all.
    if update or not formats:
        amiPhysValues(data)
        change_key_names(data)
    stage.output(pyDKB.dataflow.communication.messages.JSONMessage(data))

    return True


def amiPhysValues(data):
    """ Update data with information from AMI.

    :param data: data to update
    :type data: dict

    :return: True (update was successful) or False (otherwise)
    :rtype: bool
    """
    container = container_name(data)
    if not container:
        return False
    ami_client = get_ami_client()
    try:
        res = ami_client.execute(['GetPhysicsParamsForDataset',
                                  '--logicalDatasetName=%s' % container],
                                 format='json')
        json_str = json.loads(res)
        rowset = json_str['AMIMessage'][0]['Result'][0]['rowset']
        if not rowset:
            sys.stderr.write("(WARN) No values found in AMI for dataset '%s'\n"
                             % data['datasetname'])
            return False
        for row in rowset[0]['row']:
            p_name, p_val = None, None
            for field in row['field']:
                if field['@name'] == 'paramName':
                    p_name = field['$']
                elif field['@name'] == 'paramValue':
                    p_val = field['$']
                if p_name and p_val:
                    data[p_name] = p_val
                    break
        return True
    except SSLError as e:
        sys.stderr.write("(ERROR) Failed to process dataset '%s': "
                         "%r\n" % (data['datasetname'], e))
        return False
    except Exception as e:
        sys.stderr.write("(ERROR) Failed to process dataset '%s': "
                         "%r\n" % (data['datasetname'], e))
        return False


def change_key_names(data):
    """ Change parameter names from ones used by AMI to corresponding ES ones.

    :param data: data to update
    :type data: dict

    :return: updated data
    :rtype: dict
    """
    for item in PHYS_VALUES:
        if item["ami"] in data:
            data[item["es"]] = data.pop(item["ami"])
    return data


def container_name(data):
    """ Retrieve container name from information about dataset.

    The container name is extracted from dataset name by removing
    the '_tid...' part.

    :param data: dataset information
    :type data: dict

    :return: container name if it was determined successfully, False otherwise
    :rtype: str or bool
    """
    if 'datasetname' in data:
        dataset = data['datasetname']
    else:
        sys.stderr.write("(WARN) Required field 'datasetname' not found "
                         "in data: %r\n" % data)
        return False
    if not isinstance(dataset, (str, unicode)):
        sys.stderr.write("(WARN) Invalid type of 'datasetname' field: "
                         "expected string, got %s.\n"
                         "(==) Data: "
                         "%r\n" % (dataset.__class__.__name__, data))
        return False
    return re.sub('_tid(.)+', '', dataset)


if __name__ == '__main__':
    main(sys.argv[1:])
