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
import datetime
import json

try:
    base_dir = os.path.dirname(__file__)
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
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
    from elasticsearch.exceptions import ElasticsearchException
except ImportError, err:
    sys.stderr.write("(ERROR) Failed to import elasticsearch module: %s\n"
                     % err)
finally:
    try:
        ElasticsearchException
    except NameError:
        ElasticsearchException = None


chicago_es = None

chicago_hosts = [
    {'host': '192.170.227.31', 'port': 9200},
    {'host': '192.170.227.32', 'port': 9200},
    {'host': '192.170.227.33', 'port': 9200}
]

META_FIELDS = {
    'cputime': 'hs06'
}

AGG_FIELDS = {'hs06sec_sum': 'toths06'}
JOB_STATUSES = ['finished', 'failed']

TASK_FINAL_STATES = ['done', 'finished', 'obsolete', 'failed', 'broken',
                     'aborted']


def init_es_client():
    """ Initialize connection to Chicago ES. """
    global chicago_es
    try:
        chicago_es = elasticsearch.Elasticsearch(chicago_hosts)
    except NameError:
        sys.stderr.write("(FATAL) Failed to initialize Elasticsearch client: "
                         "module not loaded.\n")
        raise DataflowException("Module not found: 'elasticsearch'")


def get_es_client():
    """ Get configured client connected to the Chicago ES. """
    if not chicago_es:
        init_es_client()
    return chicago_es


def task_metadata(taskid, fields=[], retry=3):
    """ Get additional metadata for given task.

    :param taskid: Task ID or None
    :type taskid: str, NoneType
    :param fields: requested ES fields; if empty list or nothing is
                   passed, all the fields available will be used
    :type fields: list
    :param retry: number of retries for ES query
    :type retry: int

    :returns: requested task metadata from ES or None if called before
              ES connection is established
    :rtype: dict, NoneType
    """
    chicago_es = get_es_client()
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
        sys.stderr.write("(ERROR) ES search error (id=%r): %s\n"
                         % (taskid, err))
        kwargs_str = json.dumps(kwargs, indent=2)
        sys.stderr.write(("(DEBUG) ES query details:\n%s" % kwargs_str)
                         .replace('\n', '\n(DEBUG) ') + '\n')
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


def get_indices_by_interval(start_time, end_time, prefix='jobs_archive_',
                            wildcard=False):
    """ Get list of Chicago ES indices for jobs between two dates.

    :param start_time: beginning of the interval
    :type start_time: datetime.datetime, NoneType
    :param end_time: ending of the interval
    :type end_time: datetime.datetime, NoneType
    :param prefix: prefix for the index names
    :type prefix: str
    :param wildcard: indicates if the index names should be appended with '*';
                     if interval between start and end date is longer than one
                     month, it will be set to True automatically (to reduce
                     number of indices in the result list)
    :type wildcard: bool

    :returns: indices for dates between specified times; if start or end
              time is not specified, default wildcard-appended index
              is returned
    :rtype: list
    """
    if not start_time or not end_time:
        return [prefix + '*']
    d_format = '%Y-%m-%d'
    delta = datetime.timedelta(days=1)
    if (end_time - start_time).days > 30:
        d_format = '%Y-%m'
        delta = datetime.timedelta(days=28)
        wildcard = True
    beg = start_time.date()
    end = end_time.date()
    cur = beg
    result = []
    while cur <= end + delta - datetime.timedelta(days=1):
        result += [prefix + cur.strftime(d_format) + '*' * wildcard]
        cur += delta
    return list(set(result))


def agg_query(taskid, agg_names):
    """ Create ES query for aggregation request.

    :param taskid: Task ID or None
    :type taskid: str, NoneType
    :param agg_names: code names of requested aggregations (available:
                     "hs06sec")
    :type agg_names: list

    :returns: ES query or None in case of failure
    :rtype: str, NoneType
    """
    if not taskid:
        sys.stderr.write("(WARN) Invalid task id: %s" % taskid)
        return None

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"taskid": taskid}},
                    {"terms": {"jobstatus": JOB_STATUSES}}
                ]
            }
        },
        "aggs": {}
    }
    aggs = query["aggs"]

    for agg in agg_names:
        idx = agg.rfind('_')
        field = agg[:idx]
        agg_type = agg[(idx + 1):]
        if agg == 'hs06sec_sum':
            aggs["status"] = {"terms": {"field": "jobstatus"}}
            aggs["status"]["aggs"] = {agg: {agg_type: {"field": field}}}
        else:
            sys.stderr.write("(ERROR) Unknown aggregation: '%s'.\n" % agg)

    if not query['aggs']:
        sys.stderr.write("(WARN) No aggregations done for Task ID: '%s'.\n"
                         % taskid)
        return {}

    return query


def agg_metadata(task_data, agg_names, retry=3, es_args=None):
    """ Get task metadata by task jobs metadata aggregation.

    The aggregation is done within buckets of jobs with different status.

    :param task_data: Task metadata, including: 'taskid', 'start_time',
                      'end_time', 'status'.
    :type task_data: dict
    :param agg_names: code names of requested aggregations (available:
                     "hs06sec")
    :type agg_names: list
    :param retry: number of retries for ES query
    :type retry: int
    :param es_args: ES search query parameters (used e.g. for retry)
    :type es_args: dict, NoneType

    :returns: requested task metadata from ES or None if called before
              ES connection is established
    :rtype: dict, NoneType
    """
    taskid = task_data.get('taskid')
    start_time = task_data.get('start_time')
    end_time = task_data.get('end_time')
    status = task_data.get('status')

    chicago_es = get_es_client()
    if not chicago_es:
        sys.stderr.write("(ERROR) Connection to Chicago ES is not"
                         " established.")
        return None

    if status not in TASK_FINAL_STATES:
        return {}

    if not es_args:
        dt_format = '%d-%m-%Y %H:%M:%S'
        beg = end = None
        if start_time:
            beg = datetime.datetime.strptime(start_time, dt_format)
            beg -= datetime.timedelta(days=1)
        if end_time:
            end = datetime.datetime.strptime(end_time, dt_format)
        es_args = {
            'index': get_indices_by_interval(beg, end, wildcard=True),
            'doc_type': 'jobs_data',
            'body': agg_query(taskid, agg_names),
            'size': 0
        }
    if not es_args['body']:
        return {}

    try:
        r = chicago_es.search(**es_args)
    except ElasticsearchException, err:
        sys.stderr.write("(ERROR) ES search error (id=%r): %s\n"
                         % (taskid, err))
        args_str = json.dumps(es_args, indent=2)
        sys.stderr.write(("(DEBUG) ES query details:\n%s" % args_str)
                         .replace('\n', '\n(DEBUG) ') + '\n')
        if retry > 0:
            sys.stderr.write("(INFO) Sleep 5 sec before retry...\n")
            time.sleep(5)
            return agg_metadata(task_data, agg_names, retry - 1, es_args)
        else:
            sys.stderr.write("(FATAL) Failed to get task aggregated"
                             " metadata.\n")
            raise

    if r['hits']['total']:
        result = r['aggregations']
    else:
        result = {}
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

    # Get task metadata (direct values)
    mdata = task_metadata(data.get('taskid'), META_FIELDS.keys())
    if mdata is None:
        return False
    for key in mdata:
        data[META_FIELDS[key]] = mdata.get(key)

    # Get metadata as aggregation by jobs
    mdata = agg_metadata(data, AGG_FIELDS.keys())
    if mdata is None:
        return False
    if mdata:
        buckets = mdata['status']['buckets']
        for f in AGG_FIELDS.keys():
            total = 0
            for b in buckets:
                status = b['key']
                val = b[f]['value']
                fname = '_'.join([AGG_FIELDS[f], status])
                data[fname] = val
                total += val
            fname = AGG_FIELDS[f]
            data[fname] = total
    out_message = Message(messageType.JSON)(data)
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

    stage.process = process

    exit_code = 0
    exc_info = None
    try:
        stage.configure(args)
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
