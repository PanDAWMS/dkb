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
    sys.exit(1)

base_dir = os.path.abspath(os.path.dirname(__file__))

try:
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)

ami_client = None
PHYS_VALUES = [{"ami": "genFiltEff", "es": "gen_filt_eff"},
               {"ami": "crossSection", "es": "cross_section"},
               {"ami": "crossSectionRef", "es": "cross_section_ref"},
               {"ami": "kFactor", "es": "k_factor"}]


def main(argv):

    stage = pyDKB.dataflow.stage.JSONProcessorStage()

    stage.add_argument('--userkey', help='PEM key file', required=True)
    stage.add_argument('--usercert', help='PEM certificate file',
                       required=True)

    exit_code = 0
    try:
        stage.parse_args(argv)
        stage.process = process
        init_ami_client(stage.ARGS.userkey, stage.ARGS.usercert)
        stage.run()
    except (pyDKB.dataflow.exceptions.DataflowException, RuntimeError), err:
        if str(err):
            str_err = str(err).replace("\n", "\n(==) ")
            sys.stderr.write("(ERROR) %s\n" % str_err)
        exit_code = 2
    finally:
        stage.stop()

    sys.exit(exit_code)


def init_ami_client(userkey, usercert):
    """
    Initialisation of AMI client into the global variable
    :param userkey: user key pem file
    :param usercert: user certificate pem file
    :return:
    """
    global ami_client
    try:
        ami_client = pyAMI.client.Client('atlas', key_file=userkey,
                                         cert_file=usercert)
        AtlasAPI.init()
    except Exception:
        sys.stderr.write(
            "(ERROR) Could not establish pyAMI session."
            " Are you sure you have a valid certificate?\n")
        sys.exit(1)


def process(stage, message):

    stage.output(pyDKB.dataflow.messages.JSONMessage(
        amiPhysValues(message.content())))

    return True


def amiPhysValues(data):
    """
    Add elements in JSON string, according to theirs names in ES mapping
    - gen_filt_eff
    - cross_section
    - k_factor
    - cross_section_ref
    """
    dataset = data['datasetname']
    container = remove_tid(dataset)
    try:
        res = ami_client.execute(['GetPhysicsParamsForDataset',
                                  "--logicalDatasetName=%s" % container],
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
        return change_key_names(data)
    except Exception:
        sys.stderr.write("(WARN) No values found in AMI\n")


def change_key_names(data):
    """
    Changing parameter names according to PHYS_VALUES dictionary.
    :param data: JSON string
    :return: JSON string
    """
    for item in PHYS_VALUES:
        if item["ami"] in data:
            data[item["es"]] = data.pop(item["ami"])
    return data


def remove_tid(dataset):
    """
    As AMI GetPhysicsParamsForDataset works with containers,
    we construct the container name from each dataset name ,
    removing the _tid{...} part
    :param dataset: dataset name
    :return: dataset name without _tid => container name
    """
    return re.sub('_tid(.)+', '', dataset)


if __name__ == '__main__':
    main(sys.argv[1:])
