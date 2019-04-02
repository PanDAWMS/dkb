#!/bin/env python
'''
Script for consistency checking.

Authors:
  Vasilii Aulov (vasilii.aulov@cern.ch)
'''
import os
import sys
import traceback

from datetime import datetime

import elasticsearch


def log(msg, prefix='DEBUG'):
    ''' ??? '''
    prefix = '(%s)' % (prefix)
    prefix = prefix.ljust(8)
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


def load_config(fname):
    ''' ??? '''
    cfg = {
        'ES_HOST': 'localhost',
        'ES_PORT': '9200',
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

    TODO: take parameters from es config.
    '''
    global es
    if cfg['ES_USER'] and cfg['ES_PASSWORD']:
        s = 'http://%s:%s@%s:%s/' % (cfg['ES_USER'],
                                     cfg['ES_PASSWORD'],
                                     cfg['ES_HOST'],
                                     cfg['ES_PORT'])
    else:
        s = '%s:%s' % (cfg['ES_HOST'], cfg['ES_PORT'])
    es = elasticsearch.Elasticsearch([s])


def get_field(index, taskid, field):
    ''' Get field value by given taskid.

    :param es: elasticsearch client
    :type es: elasticsearch.client.Elasticsearch
    :param index: index containing tasks
    :type index: str
    :param taskid: taskid of the task to look for
    :type taskid: int or str
    :param index: field name
    :type index: str

    :return: field value, or False if the task was not found
    :rtype: int or bool
    '''
    try:
        results = es.get(index=index, doc_type='_all', id=taskid,
                         _source=[field])
    except elasticsearch.exceptions.NotFoundError:
        return False
    return results['_source'].get(field)


def process(stage, message):
    ''' Process a message.

    Implementation of :py:meth:`.AbstractProcessorStage.process` for hooking
    the stage into DKB workflow.
    '''
    data = message.content()
    if type(data) is not dict:
        log('Incorrect data:' + str(data), 'INPUT')
        return False
    taskid = data.get('taskid')
    if taskid is None:
        log('No taskid in data:' + str(data), 'INPUT')
        return False
    timestamp = data.get('task_timestamp')
    if timestamp is None:
        log('No timestamp supplied for taskid ' + str(taskid), 'INPUT')
        return False

    es_timestamp = get_field(INDEX, taskid, 'task_timestamp')
    if es_timestamp is None:
        log('No timestamp in ES for taskid ' + str(taskid), 'DIFF')
    elif not es_timestamp:
        log('Taskid %d not found in ES' % taskid, 'DIFF')
    elif es_timestamp != timestamp:
        log('Taskid %d has timestamp %s in ES, %s in Oracle' % (taskid,
                                                                es_timestamp,
                                                                timestamp),
            'DIFF')
    else:
        log('Taskid %d is up to date in ES' % taskid, 'INFO')

    return True


def main(args):
    ''' ??? '''

    stage = JSONProcessorStage()
    stage.add_argument('--conf', help='elasticsearch config', required=True)

    exit_code = 0
    exc_info = None
    try:
        stage.parse_args(args)
        cfg = load_config(stage.ARGS.conf)
        stage.process = process
        es_connect(cfg)
        if not es.indices.exists(INDEX):
            log('No such index: %s' % INDEX, 'ERROR')
            exit_code = 4
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

    exit(exit_code)


if __name__ == '__main__':
    main(sys.argv[1:])
