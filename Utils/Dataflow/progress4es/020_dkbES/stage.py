#!/bin/env python
"""
DKB Dataflow 'progress4es' stage 020 (dkbES).

Get additional metadata from the DKB ES:
 * htags
 * AMI tags
 * output DS formats

Authors:
  Marina Golosova (marina.golosova@cern.ch)
"""

import os
import sys

import time
import json

try:
    import pyDKB
    from pyDKB.dataflow.stage import ProcessorStage
    from pyDKB.dataflow import messageType
    from pyDKB.dataflow.communication.messages import Message
    from pyDKB.dataflow.exceptions import DataflowException
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)

try:
    import elasticsearch
    from elasticsearch.exceptions import (ElasticsearchException,
                                          ConnectionTimeout)
except ImportError, err:
    sys.stderr.write("(ERROR) Failed to import elasticsearch module: %s\n"
                     % err)
finally:
    try:
        ElasticsearchException
    except NameError:
        ElasticsearchException = None


base_dir = os.path.dirname(__file__)

STAGE_ID = '020'
dkb_es = None
es_params = {}
es_queries = {}

# Do not use these values of output dataset "data-format" field
# for step names
IGNORE_FORMATS = ['DAOD', 'DRAW']


def init_es_client(cfg=None):
    """ Initialize connection to ES.

    :param cfg: ES parameters
    :type cfg: dict
    """
    global dkb_es
    global es_params
    http_auth = None
    if cfg:
        if cfg.get('user') and cfg.get('passwd'):
            http_auth = (cfg['user'], cfg['passwd'])
        if cfg.get('hosts'):
            hosts = cfg['hosts'].split(',')
        es_params['index'] = cfg.get('index')
        if cfg.get('queries'):
            queries = os.path.join(base_dir, cfg['queries'])
            if not os.path.isidir(queries):
                raise DataflowException("Directory not found (%s)."
                                        % queries)

    try:
        dkb_es = elasticsearch.Elasticsearch(hosts, http_auth=http_auth)
    except NameError:
        sys.stderr.write("(FATAL) Failed to initialize Elasticsearch client: "
                         "module not loaded.\n")
        raise DataflowException("Module not found: 'elasticsearch'")


def get_es_client():
    """ Get configured client connected to the Chicago ES. """
    if not dkb_es:
        init_es_client()
    return dkb_es


def load_queries(path='./queries'):
    """ Load stage queries.

    :param path: path to query files
    :type path: str
    """
    global es_queries
    if not path:
        return None
    full_path = os.path.join(base_dir, path)
    try:
        for q in os.listdir(full_path):
            if not q.endswith('.json'):
                continue
            with open(os.path.join(full_path, q)) as f:
                query = f.read()
            es_queries[q[:-len('.json')]] = query
    except IOError, e:
        raise DataflowException("Failed to load queries", reason=e)


def task_metadata(taskids, retry=3):
    """ Get additional metadata for given task(s).

    :param taskids: (list of) Task ID(s) or None
    :type taskids: list, str, NoneType
    :param retry: number of retries for ES query
    :type retry: int

    :returns: hash of the requested task(s) metadata from ES (with
              numeric task ID(s) as hash kays)
    :rtype: dict
    """
    dkb_es = get_es_client()
    if not dkb_es:
        raise DataflowException("ES client is not initialized.")
    if not taskids:
        sys.stderr.write("(WARN) Invalid task id: %s\n" % taskids)
        return {}
    if not isinstance(taskids, list):
        taskids = [taskids]
    qname = 'task_metadata'
    q = es_queries.get(qname)
    if not q:
        raise DataflowException("Query not found: %s" % qname)
    try:
        q = q % {'taskids': json.dumps(taskids)}
    except KeyError, e:
        raise DataflowException("Unexpected query parameter: %s (%s)"
                                % (e, qname))
    except Exception:
        raise DataflowException("Invalid query parameter value: %s (%s)"
                                % (taskids, qname))
    try:
        q = json.loads(q)
    except Exception, e:
        raise DataflowException("Failed to parse query (%s)" % qname,
                                reason=e)

    kwargs = {}
    if es_params and es_params.get('index'):
        kwargs['index'] = es_params['index']
    if '_source' in q:
        kwargs['_source'] = q.pop('_source')
    kwargs['body'] = q
    kwargs['size'] = len(taskids)
    try:
        r = dkb_es.search(**kwargs)
    except ElasticsearchException, err:
        sys.stderr.write("(ERROR) ES search error (ids: %r): %s\n"
                         % (taskidis, err))
        kwargs_str = json.dumps(kwargs, indent=2)
        sys.stderr.write(("(DEBUG) ES query details:\n%s" % kwargs_str)
                         .replace('\n', '\n(DEBUG) ') + '\n')
        if retry > 0:
            sys.stderr.write("(INFO) Sleep 5 sec before retry...\n")
            time.sleep(5)
            return task_metadata(taskids, retry - 1)
        else:
            sys.stderr.write("(FATAL) Failed to get task metadata.\n")
            raise
    results = {}
    if r['hits']['hits']:
        for task in r['hits']['hits']:
            try:
                result = transform_task_info(task)
                results[int(result['taskid'])] = result
            except Exception, e:
                try:
                    taskid = task.get('_id')
                except Exception:
                    taskid = 'unknown'
                sys.stderr.write("(WARN) Failed to parse task data (id=%s):"
                                 " %s.\n" % (taskid, e))
                sys.stderr.write("(DEBUG) Task data:\n%s\n"
                                 % json.dumps(task, indent=2))
    return results


def transform_task_info(task):
    """ Transform Task document info taken from ES to expected format.

    .. note:: in there result document there are some to-be-obsolete fields,
              required due to some to-be-obsolete ways of how to define
              a production step:
              * mc_step
              * ctag_format_step

    :param task: original task info
    :type task: dict

    :returns: transformed document
    :rtype: dict
    """
    result = {}
    result['taskid'] = task['_id']
    result['hashtag'] = task['_source']['hashtag_list']
    result['mc_step'] = task['_source']['step_name']
    tags = task['_source']['taskname'].split('.')[-1]
    ctag = tags.split('_')[-1]
    formats = []
    try:
        datasets = task['inner_hits']['dataset']['hits']['hits']
    except KeyError:
        datasets = []
    for ds in datasets:
        if not ds['_source']['data_format'] in IGNORE_FORMATS:
            formats += ds['_source']['data_format']
    formats = list(set(formats))
    for f in formats:
        result['ctag_format_step'] = ':'.join([ctag, f])
        result['ami_tags_format_step'] = ':'.join([tags, f])
    return result


def process(stage, message):
    """ Single message processing.

    :param stage: ETL processing stage
    :type stage: pyDKB.dataflow.stage.ProcessorStage
    :param message: input message with data to be processed
    :type message: pyDKB.dataflow.communication.messages.JSONMessage

    :returns: True or False in case of failure
    :rtype: bool
    """
    data = message.content()
    mdata = task_metadata(data.get('taskid'))
    sys.stderr.write("(DEBUG) MDATA: %s\n" % mdata)
    if mdata:
        data.update(mdata.get(int(data.get('taskid')), {}))
        out_message = Message(messageType.JSON)(data)
    else:
        sys.stderr.write("(INFO) No information found (taskID: %s).\n"
                         % data.get('taskid', 'undefined'))
        out_message = Message(messageType.JSON)(data)
        out_message.incomplete(True)
    stage.output(out_message)
    return True


def main(args):
    """ Program body.

    :param args: command line arguments
    :type args: list
    """
    stage = ProcessorStage()
    stage.set_input_message_type(messageType.JSON)
    stage.set_output_message_type(messageType.JSON)

    cfg_file = os.path.join(base_dir, os.pardir, 'config', STAGE_ID + '.cfg')
    stage.set_default_arguments(config=cfg_file, ignore_on_skip=True)

    stage.process = process

    stage.configure(args)

    if not stage.ARGS.skip_process:
        init_es_client(stage.CONFIG['dkbES'])
        load_queries()

    exit_code = stage.run()

    if exit_code == 0:
        stage.stop

    exit(exit_code)


if __name__ == '__main__':
    main(sys.argv[1:])
