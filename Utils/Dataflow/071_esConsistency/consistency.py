#!/bin/env python
'''
Script for checking the supplied task's presence in elasticsearch.

Currently it performs the check by comparing the supplied timestamp
with the one in elasticsearch.

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
    from pyDKB.dataflow.stage import JSONProcessorStage
    from pyDKB.dataflow.messages import JSONMessage
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
        'ES_HOST': '',
        'ES_PORT': '',
        'ES_USER': '',
        'ES_PASSWORD': '',
        'ES_INDEX': ''
    }
    with open(fname) as f:
        lines = f.readlines()
    for l in lines:
        if l.startswith('ES'):
            key = False
            value = False
            try:
                (key, value) = l.split()[0].split('=')
            except ValueError:
                pass
            if key in cfg:
                cfg[key] = value
    global INDEX
    INDEX = cfg['ES_INDEX']
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
    if cfg['ES_USER'] and cfg['ES_PASSWORD']:
        s = 'http://%s:%s@%s:%s/' % (cfg['ES_USER'],
                                     cfg['ES_PASSWORD'],
                                     cfg['ES_HOST'],
                                     cfg['ES_PORT'])
    else:
        s = '%s:%s' % (cfg['ES_HOST'], cfg['ES_PORT'])
    es = elasticsearch.Elasticsearch([s])
    return True


def get_fields(index, _id, _type, fields):
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
                         _source=fields)
    except elasticsearch.exceptions.NotFoundError:
        return False
    return results['_source']


def process(stage, message):
    ''' Process a message.

    Implementation of :py:meth:`.AbstractProcessorStage.process` for hooking
    the stage into DKB workflow.

    :param stage: stage instance
    :type stage: pyDKB.dataflow.stage.ProcessorStage
    :param msg: input message with document info
    :type msg: pyDKB.dataflow.Message
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

    # Crutch. Remove unwanted (for now) field added by Stage 016.
    if 'phys_category' in data:
        del data['phys_category']

    # Do not check empty documents with valid _id and _type.
    # It's unlikely that such documents will be produced in DKB. In general,
    # such documents should be checked by es.exists(), and not es.get().
    if not data:
        log('Nothing to check for document (%s, %d)' % (_type, _id), 'WARN')
        return False

    es_data = get_fields(INDEX, _id, _type, data.keys())
    if data != es_data:
        log('Document (%s, %d) differs between Oracle and ES: Oracle:%s ES:%s'
            % (_type, _id, data, es_data), 'WARN')
        out_message = JSONMessage({'_type': _type, '_id': _id})
        stage.output(out_message)
        global FOUND_DIFF
        FOUND_DIFF = True
    else:
        log('Document (%s, %d) is up to date in ES' % (_type, _id), 'INFO')

    return True


def main(args):
    ''' Parse command line arguments and run the stage.

    :param argv: arguments
    :type argv: list
    '''

    stage = JSONProcessorStage()
    stage.add_argument('--conf', help='elasticsearch config', required=True)

    exit_code = 0
    exc_info = None
    try:
        stage.parse_args(args)
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
            stage.run()
    except (DataflowException, RuntimeError), err:
        if str(err):
            log(err, 'ERROR')
        exit_code = 2
    except Exception:
        exc_info = sys.exc_info()
        exit_code = 3
    finally:
        stage.stop()

    if exc_info:
        trace = traceback.format_exception(*exc_info)
        for line in trace:
            log(line, 'ERROR')

    if exit_code == 0 and FOUND_DIFF:
        exit_code = 1

    exit(exit_code)


if __name__ == '__main__':
    main(sys.argv[1:])
