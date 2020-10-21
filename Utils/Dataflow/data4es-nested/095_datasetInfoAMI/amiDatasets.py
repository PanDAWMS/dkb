#!/bin/env python
import json
import re
import sys
import os
try:
    import pyAMI.client
    import pyAMI.atlas.api as AtlasAPI
    import pyAMI.config
    from pyAMI.exception import Error as AMIError
    from pyAMI.httpclient import http_client
except ImportError:
    sys.stderr.write("(ERROR) Unable to find pyAMI client.\n")

base_dir = os.path.abspath(os.path.dirname(__file__))

try:
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
    from pyDKB.dataflow import messageType
    from pyDKB.dataflow.exceptions import DataflowException
    from pyDKB import atlas
    from pyDKB.common.misc import execute_with_retry
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)

ami_client = None

# Field names in terms of AMI and ES schemes.
PHYS_VALUES = [{'ami': 'genFiltEff', 'es': 'gen_filt_eff'},
               {'ami': 'crossSection', 'es': 'cross_section'},
               {'ami': 'crossSectionRef', 'es': 'cross_section_ref'},
               {'ami': 'kFactor', 'es': 'k_factor'},
               {'ami': 'processGroup', 'es': 'process_group'},
               {'ami': 'mePDF', 'es': 'me_pdf'},
               ]
FILTER = ['AOD', 'EVNT', 'HITS']
# Scope parts - AMI only has data for datasets with scopes that start
# with something from this list.
SCOPES = ('mc15', 'mc16')


def main(argv):
    """ Main program body.

    :param argv: command line arguments
    :type argv: list
    """
    stage = pyDKB.dataflow.stage.ProcessorStage()
    stage.set_input_message_type(messageType.JSON)
    stage.set_output_message_type(messageType.JSON)

    stage.set_default_arguments(config=os.path.join(base_dir, os.pardir,
                                                    'config', '095.cfg'))

    stage.configure(argv)
    stage.process = process

    if stage.CONFIG['ami'].get('userkey', '') \
            and stage.CONFIG['ami'].get('usercert', ''):
        init_ami_client(stage.CONFIG['ami']['userkey'],
                        stage.CONFIG['ami']['usercert'])

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
    :raises DataflowException: if:
                               - pyAMI module not found
                               - failed to establish pyAMI session
                                 (can be incorrect key/certificate)
                               - key and/or certificate not found
                               - test command failed
    """
    global ami_client
    try:
        ami_client = pyAMI.client.Client(['atlas-replica', 'atlas'],
                                         key_file=userkey, cert_file=usercert)
        AtlasAPI.init()
    except NameError:
        sys.stderr.write("(FATAL) Failed to initialise AMI client:"
                         " pyAMI module is not loaded.\n")
        raise DataflowException("Module not found: 'pyAMI'")
    except Exception, err:
        sys.stderr.write(
            "(ERROR) Could not establish pyAMI session."
            " Are you sure you have a valid certificate?\n")
        raise DataflowException(str(err))
    if ami_client.config.conn_mode == ami_client.config.CONN_MODE_LOGIN:
        sys.stderr.write("(ERROR) Login authentication mode is not"
                         " supported. Please provide user certificate or"
                         " create proxy.\n")
        raise DataflowException("Failed to initialise AMI client: certificate"
                                " not provided or not found.")
    sys.stderr.write("(INFO) Sending test command ListCommands to check"
                     " that AMI client works and the credentials are"
                     " correct...\n")
    try:
        execute_with_retry(ami_client.execute, args=['ListCommands'],
                           retry_on=(AMIError, http_client.HTTPException),
                           sleep=64)
    except Exception as e:
        sys.stderr.write("(ERROR) Test command ListCommands failed. Are you"
                         " sure you have a valid certificate?\n"
                         "(==) Exception: %s\n" % str(e))
        raise DataflowException("Test command ListCommands failed.")
    sys.stderr.write("(INFO) Success.\n")


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
    datasets = data.get('output_dataset', [])
    for ds in datasets:
        ds_format = ds.get('data_format')
        if ds_format in FILTER or not ds_format:
            try:
                amiPhysValues(ds)
            except DataflowException:
                raise
            except Exception:
                stage.output_error("Failed to process dataset '%s'"
                                   % ds['name'], sys.exc_info())
            else:
                # Do not put this into try/except above:
                #   any exception produced by it indicates a problem
                #   with the stage code that demands a full stop.
                change_key_names(ds)
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
    scope = pyDKB.atlas.misc.dataset_scope(container)
    if not scope.startswith(SCOPES):
        return True
    ami_client = get_ami_client()
    exec_params = {'command': ['GetPhysicsParamsForDataset',
                               '--logicalDatasetName=%s' % container,
                               '-scope=%s' % scope],
                   'format': 'json'}
    res = execute_with_retry(ami_client.execute, kwargs=exec_params, sleep=64,
                             retry_on=(AMIError, http_client.HTTPException))
    json_str = json.loads(res)
    try:
        rowset = json_str['AMIMessage'][0]['Result'][0]['rowset']
    except Exception:
        raise Exception("Unexpected AMI response: %s" % json_str)
    if not rowset:
        sys.stderr.write("(WARN) No values found in AMI for dataset '%s'\n"
                         % data['name'])
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


def change_key_names(data):
    """ Change parameter names from ones used by AMI to corresponding ES ones.

    :param data: data to update
    :type data: dict

    :return: updated data
    :rtype: dict
    """
    for item in PHYS_VALUES:
        if item['ami'] in data:
            data[item['es']] = data.pop(item['ami'])
    return data


def container_name(data):
    """ Retrieve container name from information about dataset.

    The container name is extracted from normalized dataset name by removing
    the '_tid...' part.

    :param data: dataset information, must contain 'name' field
    :type data: dict

    :return: container name if it was determined successfully, False otherwise
    :rtype: str or bool
    """
    if 'name' in data:
        dataset = data['name']
    else:
        sys.stderr.write("(WARN) Required field 'name' not found"
                         " in data: %r\n" % data)
        return False
    if not isinstance(dataset, (str, unicode)):
        sys.stderr.write("(WARN) Invalid type of 'name' field:"
                         " expected string, got %s.\n"
                         "(==) Data:"
                         " %r\n" % (dataset.__class__.__name__, data))
        return False
    if len(dataset) == 0:
        sys.stderr.write("(WARN) Required field 'name' is empty"
                         " in data: %r\n" % data)
    dataset = pyDKB.atlas.misc.normalize_dataset_name(dataset)
    return re.sub('_tid(.)+', '', dataset)


if __name__ == '__main__':
    main(sys.argv[1:])
