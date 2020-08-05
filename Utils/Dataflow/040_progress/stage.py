#!/bin/env python
"""
DKB Dataflow stage 040 (progress)

Generate task current progress documents for ES.

Authors:
  Golosova Marina (marina.golosova@cern.ch)
"""

import os
import sys

from datetime import datetime, timedelta

try:
    base_dir = os.path.dirname(__file__)
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
    from pyDKB.dataflow.stage import ProcessorStage
    from pyDKB.dataflow.communication.messages import JSONMessage
    from pyDKB.dataflow.exceptions import DataflowException
    from pyDKB.dataflow import messageType
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


DATE_FORMAT = "%d-%m-%Y %H:%M:%S"


def round_ts(timestamp, granularity):
    """ Round timestamp to specified granularity.

    :param timestamp: task timestamp (string in DATE_FORMAT)
    :type timestamp: str
    :param granularity: granularity of metadata snapshots.
                        Value is a number of seconds:
                         - N days (divisible by 86400);
                         - part of a day (aliquot of 86400).
                        Seconds will count from the beginning of day,
                        and days -- from ``datetime.min``
    :type granularity: int

    :return: rounded timestemp
    :rtype: datetime
    """
    dt = datetime.strptime(timestamp, DATE_FORMAT)
    td_gran = timedelta(seconds=granularity)
    if (td_gran.days and td_gran.seconds) \
            or not td_gran:
        raise DataflowException("Invalid progress granularity: %s (%s d,"
                                " %s s). Expected non-zero number divisible"
                                " by or aliquot of 86400 (N days or an aliqout"
                                " part of one day).")
    if td_gran.days:
        days = (dt - dt.min).days / td_gran.days * td_gran.days
        new_ts = dt.min + timedelta(days=days)
    else:
        s = (dt - dt.min).seconds / td_gran.seconds * td_gran.seconds
        new_ts = dt.min + timedelta(seconds=s)

    return new_ts


def progress_data(data, granularity=86400):
    """ Generate document with task progress metadata.

    :param data: task metadata
    :type data: dict
    :param granularity: granularity of metadata snapshots.
                        Value is a number of seconds:
                         - N days (divisible by 86400);
                         - part of a day (aliquot of 86400).
                        Seconds will count from the beginning of day,
                        and days -- from ``datetime.min``
    :type granularity: int

    :return: task current progress metadata
    :rtype: dict
    """
    result = {}
    fields = ['hashtag_list', 'ami_tags_format_step',
              'ctag_format_step']
    # Required fields
    result['taskid'] = data['taskid']
    rounded_ts = round_ts(data['task_timestamp'], granularity)
    result['date'] = rounded_ts.strftime(DATE_FORMAT)
    # "Optional" fields
    for f in fields:
        result[f] = data.get(f)
    result['processed_events'] = data.get('processed_events_v2')
    result['mc_step'] = data.get('step_name')
    # Service fields
    rounded_ts_sec = int((rounded_ts - datetime(1970, 1, 1)).total_seconds())
    rounded_ts_ms = rounded_ts_sec * 10**3
    result['_id'] = '%s_%s' % (rounded_ts_ms, result['taskid'])
    result['_type'] = 'task_progress'
    result['_index'] = 'progress'

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
    try:
        out_data = progress_data(data)
    except KeyError:
        stage.output_error("Invalid input message: %s" % data,
                           sys.exc_info())
    else:
        out_message = JSONMessage(out_data)
        stage.output(out_message)
    finally:
        stage.output(message)
    return True


def main(args):
    """ Program body. """
    stage = ProcessorStage()
    stage.set_input_message_type(messageType.JSON)
    stage.set_output_message_type(messageType.JSON)

    stage.process = process

    stage.configure(args)

    exit_code = stage.run()

    if exit_code == 0:
        stage.stop()

    exit(exit_code)


if __name__ == '__main__':
    main(sys.argv[1:])
