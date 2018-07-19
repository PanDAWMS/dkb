#!/bin/env python
"""
DKB Dataflow stage 025 (chicagoES).

Get additional metadata from Chicago ES:
 * cputime -> HS06

Authors:
  Marina Golosova (marina.golosova@cern.ch)
"""

import os
import sys
import traceback

import time

try:
    base_dir = os.path.dirname(__file__)
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
    from pyDKB.dataflow.stage import JSONProcessorStage
    from pyDKB.dataflow.messages import JSONMessage
    from pyDKB.dataflow.exceptions import DataflowException
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)

try:
    import elasticsearch
    from elasticsearch.exceptions import ElasticsearchException
except ImportError, err:
    sys.stderr.write("(FATAL) Failed to import elasticsearch module: %s\n"
                     % err)
    sys.exit(2)

chicago_es = None

chicago_hosts = [
    {'host': '192.170.227.31', 'port': 9200},
    {'host': '192.170.227.32', 'port': 9200},
    {'host': '192.170.227.33', 'port': 9200}
]

META_FIELDS = {
    'cputime': 'hs06'
}


def init_es_client():
    """ Initialize connection to Chicago ES. """
    global chicago_es
    chicago_es = elasticsearch.Elasticsearch(chicago_hosts)


def task_metadata(taskid, fields=[], retry=3):
    """ Get additional metadata for given task. """
    if not chicago_es:
        sys.stderr.write("(ERROR) Connection to Chicago ES is not"
                         " established.")
        return None
    if not taskid:
        sys.stderr.write("(WARN) Invalid task id: %s" % taskid)
        return {}
    kwargs = {
        'index': 'tasks_archive_*',
        'doc_type': 'task_data',
        'body': '{ "query": { "term": {"_id": "%s"} } }' % taskid,
        '_source': fields
    }
    try:
        r = chicago_es.search(**kwargs)
    except ElasticsearchException, err:
        sys.stderr.write("(ERROR) ES search error: %s\n" % err)
        if retry > 0:
            sys.stderr.write("(INFO) Sleep 5 sec before retry...\n")
            time.sleep(5)
            return task_metadata(taskid, fields, retry - 1)
        else:
            sys.stderr.write("(FATAL) Failed to get task metadata.\n")
            raise
    if not r['hits']['hits']:
        result = {}
    else:
        result = r['hits']['hits'][0]['_source']
    return result


def process(stage, message):
    """ Single message processing. """
    data = message.content()
    mdata = task_metadata(data.get('taskid'), META_FIELDS.keys())
    if mdata is None:
        return False
    for key in mdata:
        data[META_FIELDS[key]] = mdata.get(key)
    out_message = JSONMessage(data)
    stage.output(out_message)
    return True


def main(args):
    """ Program body. """
    stage = JSONProcessorStage()
    stage.process = process

    init_es_client()

    exit_code = 0
    exc_info = None
    try:
        stage.parse_args(args)
        stage.run()
    except (DataflowException, RuntimeError), err:
        if str(err):
            sys.stderr.write("(ERROR) %s\n" % err)
        exit_code = 2
    except Exception:
        exc_info = sys.exc_info()
        exit_code = 3
    finally:
        stage.stop()

    if exc_info:
        trace = traceback.format_exception(*exc_info)
        for line in trace:
            sys.stderr.write("(ERROR) %s" % line)

    exit(exit_code)


if __name__ == '__main__':
    main(sys.argv[1:])
