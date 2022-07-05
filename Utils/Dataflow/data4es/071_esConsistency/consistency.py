#!/bin/env python
'''
Script for checking the supplied task's presence in elasticsearch.

It performs the check by comparing the supplied fields with the corresponding
ones in elasticsearch.

Authors:
  Vasilii Aulov (vasilii.aulov@cern.ch)
'''
import os
import sys
import traceback

from datetime import datetime

import elasticsearch


def log(msg, prefix='DEBUG'):
    ''' Add prefix and current time to message and write it to stderr. '''
    prefix = '(%s)' % (prefix)
    # 11 = len("(CRITICAL) "), where CRITICAL is the longest log level name.
    prefix = prefix.ljust(11)
    sys.stderr.write('%s%s %s\n' % (prefix, datetime.now().isoformat(), msg))


try:
    base_dir = os.path.dirname(__file__)
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
    from pyDKB.dataflow.stage import ProcessorStage
    from pyDKB.dataflow.communication.messages import JSONMessage
    from pyDKB.dataflow.exceptions import DataflowException
except Exception, err:
    log('Failed to import pyDKB library: %s' % err, 'ERROR')
    sys.exit(1)


es = None


INDEX = None
FOUND_DIFF = False


def load_config(fname):
    ''' Open elasticsearch config and obtain parameters from it.

    Setup INDEX as global variable.

    :param fname: config file's name
    :type fname: str
    '''
    cfg = {
        'ES_PROTO': 'http',
        'ES_HOST': '',
        'ES_PORT': '',
        'ES_PATH': '',
        'ES_USER': '',
        'ES_PASSWORD': '',
        'ES_CA_CERTS': '/etc/pki/tls/certs/CERN-bundle.pem',
        'ES_INDEX_TASKS': ''
    }
    with open(fname) as f:
        lines = f.readlines()
    for line in lines:
        if line.startswith('ES'):
            key = False
            value = False
            try:
                (key, value) = line.split()[0].split('=')
            except ValueError:
                pass
            if key in cfg:
                cfg[key] = value
    global INDEX
    INDEX = cfg['ES_INDEX_TASKS']
    return cfg


def es_connect(cfg):
    ''' Establish a connection to elasticsearch.

    Initialize the global variable es with the resulting client object.

    :param cfg: connection parameters
    :type cfg: dict
    '''
    if not cfg['ES_HOST']:
        log('No ES host specified', 'ERROR')
        return False
    if not cfg['ES_PORT']:
        log('No ES port specified', 'ERROR')
        return False

    global es
    s = '%s:%s' % (cfg['ES_HOST'], cfg['ES_PORT'])
    if cfg['ES_USER'] and cfg['ES_PASSWORD']:
        s = '%s://%s:%s@%s/%s/' % (cfg['ES_PROTO'],
                                   cfg['ES_USER'],
                                   cfg['ES_PASSWORD'],
                                   s,
                                   cfg['ES_PATH'])

    ca_certs = cfg['ES_CA_CERTS'] if cfg['ES_PROTO'] == 'https' else None

    es = elasticsearch.Elasticsearch([s], ca_certs=ca_certs)
    return True


def get_fields(index, _id, _type, fields, _parent):
    ''' Get fields value by given _id and _type.

    :param es: elasticsearch client
    :type es: elasticsearch.client.Elasticsearch
    :param index: index to search in
    :type index: str
    :param _id: id of the document to look for
    :type _id: int or str
    :param _type: type of the document to look for
    :type _type: str
    :param fields: field names
    :type fields: list

    :return: field values, or False if the document was not found
    :rtype: dict or bool
    '''
    try:
        results = es.get(index=index, doc_type=_type, id=_id,
                         _source=fields, parent=_parent)
    except elasticsearch.exceptions.NotFoundError:
        return False
    return results['_source']


def process(stage, message):
    ''' Process a message.

    Implementation of :py:meth:`.ProcessorStage.process` for hooking
    the stage into DKB workflow.

    :param stage: stage instance
    :type stage: pyDKB.dataflow.stage.ProcessorStage
    :param msg: input message with document info
    :type msg: pyDKB.dataflow.communication.messages.JSONMessage
    '''
    data = message.content()
    if type(data) is not dict:
        log('Incorrect data:' + str(data), 'WARN')
        return False
    try:
        _id = data.pop('_id')
        _type = data.pop('_type')
    except KeyError:
        log('Insufficient ES info in data:' + str(data), 'WARN')
        return False

    _parent = data.pop('_parent', None)

    # Fields starting with an underscore are service fields. Some of them are
    # treated in special way (see _id above). Service fields should not be
    # checked, so they are removed.
    data = {field: data[field] for field in data if field[0] != '_'}

    # Do not check empty documents with valid _id and _type.
    # It's unlikely that such documents will be produced in DKB. In general,
    # such documents should be checked by es.exists(), and not es.get().
    if not data:
        log('Nothing to check for document (%s, %r)' % (_type, _id), 'WARN')
        return False

    es_data = get_fields(INDEX, _id, _type, data.keys(), _parent)
    if data != es_data:
        log('Document (%s, %r) differs between Oracle and ES: Oracle:%s ES:%s'
            % (_type, _id, data, es_data), 'WARN')
        out_message = JSONMessage({'_type': _type, '_id': _id})
        stage.output(out_message)
        global FOUND_DIFF
        FOUND_DIFF = True
    else:
        log('Document (%s, %r) is up to date in ES' % (_type, _id), 'INFO')

    return True


def main(args):
    ''' Parse command line arguments and run the stage.

    :param argv: arguments
    :type argv: list
    '''

    stage = ProcessorStage()
    stage.set_input_message_type(JSONMessage.msg_type)
    stage.set_output_message_type(JSONMessage.msg_type)
    stage.add_argument('--conf', help='elasticsearch config', required=True)

    stage.configure(args)
    cfg = load_config(stage.ARGS.conf)
    stage.process = process
    if not es_connect(cfg):
        exit_code = 4
    elif not INDEX:
        log('No ES index specified', 'ERROR')
        exit_code = 5
    elif not es.indices.exists(INDEX):
        log('No such index: %s' % INDEX, 'ERROR')
        exit_code = 6
    else:
        exit_code = stage.run()

    if exit_code == 0 and FOUND_DIFF:
        exit_code = 1

    exit(exit_code)


if __name__ == '__main__':
    main(sys.argv[1:])
