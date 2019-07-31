#!/bin/env python
import json
import re
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
PHYS_VALUES = [{"ami": "genFiltEff", "es": "gen_filt_eff"},
               {"ami": "crossSection", "es": "cross_section"},
               {"ami": "crossSectionRef", "es": "cross_section_ref"},
               {"ami": "kFactor", "es": "k_factor"},
               {"ami": "processGroup", "es": "process_group"},
               {"ami": "mePDF", "es": "me_pdf"},
               ]
FILTER = ['AOD', 'EVNT', 'HITS']


def main(argv):
    """ Main program body. """
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
    """ Initialisation of AMI client into the global variable

    :param userkey: user key pem file
    :param usercert: user certificate pem file
    :return:
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


def get_ami_client():
    """ Get configured AMI client. """
    if not ami_client:
        init_ami_client()
    return ami_client


def process(stage, message):
    """ Single message processing. """
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
    stage.output(pyDKB.dataflow.communication.messages.JSONMessage(data))

    return True


def amiPhysValues(data):
    """ Add elements in JSON string, according to theirs names in ES mapping

    - gen_filt_eff
    - cross_section
    - k_factor
    - cross_section_ref
    """
    dataset = data['datasetname']
    container = remove_tid(dataset)
    ami_client = get_ami_client()
    try:
        res = ami_client.execute(['GetPhysicsParamsForDataset',
                                  '--logicalDatasetName=%s' % container],
                                 format='json')
        json_str = json.loads(res)
        for row in json_str['AMIMessage'][0]['Result'][0]['rowset'][0]['row']:
            p_name, p_val = None, None
            for field in row['field']:
                if field['@name'] == 'paramName':
                    p_name = field['$']
                elif field['@name'] == 'paramValue':
                    p_val = field['$']
                if p_name and p_val:
                    data[p_name] = p_val
                    p_name, p_val = None, None
                    continue
        change_key_names(data)
        return True
    except Exception:
        sys.stderr.write("(WARN) No values found in AMI for dataset '%s'\n"
                         % data['datasetname'])
        return False


def change_key_names(data):
    """ Changing parameter names according to PHYS_VALUES dictionary.

    :param data: JSON string
    :return: JSON string
    """
    for item in PHYS_VALUES:
        if item["ami"] in data:
            data[item["es"]] = data.pop(item["ami"])
    return data


def remove_tid(dataset):
    """ Remove TaskID (_tidXX) part from dataset name.

    As AMI GetPhysicsParamsForDataset works with containers,
    we construct the container name from each dataset name,
    removing the _tid{...} part

    :param dataset: dataset name
    :return: dataset name without _tid => container name
    """
    return re.sub('_tid(.)+', '', dataset)


if __name__ == '__main__':
    main(sys.argv[1:])
