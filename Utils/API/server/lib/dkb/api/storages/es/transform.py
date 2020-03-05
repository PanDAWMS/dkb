"""
Transformation scenarios for ES data.
"""

import logging
import json

from api.storages.common import STEP_TYPES
from common import (ES_DATE_FORMAT,
                    PREFIX_AGGS)


# Common functions for ES respomse parsing
# --->

def steps_iterator(data):
    """ Gerenator for iterator over steps data.

    Recursively check all buckets within `steps` and `substeps`
    clauses of the ``data``.

    Input ``data`` are supposed to have outer key "steps" and may have
    nested keys "substeps" or "steps".

    :param data: full data to extract steps information from
    :type data: dict

    :returns: steps data in iterable representation.
              Each call of `next()` methor returns tuple:
              (``step_name``, ``step_data``)
    :rtype: iterable object
    """
    if data.get('steps'):
        # `data` contains information about steps
        # (first or recursive calls of the generator)
        buckets = data['steps'].get('buckets', None)
    elif data.get('substeps'):
        # `data` contains information about substeps
        # (recursive calls of the generator)
        buckets = data['substeps'].get('buckets', None)
    else:
        # `data` is data of a single step
        yield None, data
        raise StopIteration

    # Call `steps_iterator` for each bucket
    # (in case there are some sub-steps)
    for bucket in buckets:
        if isinstance(buckets, list):
            bucket_name = bucket.get('key', None)
        elif isinstance(buckets, dict):
            bucket_name = bucket
            bucket = buckets[bucket_name]
        for step_name, step in steps_iterator(bucket):
            step_name = ':'.join([bucket_name, step_name]) if step_name \
                        else bucket_name
            yield step_name, step


def get_single_agg_value(data, unit):
    """ Get single stat value from data.
    """
    if unit == 'total':
        val = data.get('doc_count', None)
    elif unit:
        while data.get(unit):
            data = data[unit]
        val = data.get('value', None)
    else:
        val = data.get('value', None)
    return val


def get_bucketed_agg_values(data, units=[]):
    """ Get values of stat units from data with 'buckets'.

    :param data: part of ES response containing statistic values
                 for a single item (e.g. processing step)
    :type data: dict
    :param units: statistics units (:py:func:`_agg_units`)

    :returns: simplified statistics representation, containing
              unit names as keys, and values -- as values
    :rtype: dict
    """
    if 'buckets' not in data:
        raise ValueError('get_bucketed_agg_values() expects `data` param to'
                         ' contain "buckets".')
    result = {}
    buckets = data['buckets']
    for bucket in buckets:
        if isinstance(buckets, list):
            bucket_name = bucket.get('key', None)
        elif isinstance(buckets, dict):
            bucket_name = i
            bucket = data[i]
        else:
            raise MethodException("Failed to parse ES response.")
        r = result[bucket_name] = {}
        r.update(get_agg_values(bucket, units))
        if set(r.keys()) == set(['total']):
            result[bucket_name] = r['total']
    return result


def get_agg_values(data, units=[]):
    """ Get value of statistics units from ES response.

    Input data contains items of one of the following views:
    ```
    1. {<unit_name>: {... {<unit_name>: <data>}...}}
    2. {<prefix>: {... {<prefix>: <data>}}}
    ```

    ``<Data>`` part may have one of the following views:
    ```
    1. {'value': <desired_value>}
    2. {'doc_count': <desired_value>}
    3. {'buckets': [{'key': <bucket_name>, 'doc_count': <n>, <sub-items>},
                    ...]}
    4. {'buckets': {<bucket_name>: {'doc_count': <n>, <sub-items>}, ...}}
    ```

    In ``<data>`` of types 3-4 ``<sub-items>`` are optional.
    For items of type 2 (with ``<prefix>``), ``<data>`` must be of type 2
    (with ``'doc_count'``) or 3-4 (with or without ``<sub-items>``).

    Output data contains items of one of the following simplified views:
    ```
    1. {<unit_name>: <desired_value>}
    2. {<prefix>: {<bucket_name>: {'total': <n>, <sub-items>}}}
    ```

    ``<Unit_name>`` in ``<items>`` is one of the passed ``units``,
    while in ``<sub-items>`` -- the one with stripped ``<prefix>__``.

    :param data: part of ES response containing statistic values
                 for a single item (e.g. processing step)
    :type data: dict
    :param units: statistics units (:py:func:`get_agg_units_query`)
    :type units: list

    :returns: simplified statistics representation, containing
              unit names as keys, and values -- as values
    :rtype: dict
    """
    prefixes = PREFIX_AGGS.keys()
    result = {}
    orig_data = data
    prefixed_units = {}
    clean_units = list(units)

    for unit in units:
        for p in prefixes:
            if unit == p:
                clean_units.remove(unit)
                prefixed_units[p] = prefixed_units.get(p, [])
                prefixed_units[p].append('total')
            if unit.startswith(p + '__'):
                clean_units.remove(unit)
                u = unit[(len(p) + 2):]
                prefixed_units[p] = prefixed_units.get(p, [])
                prefixed_units[p].append(u)
                break

    for p in prefixed_units:
        data = orig_data.get(p, {})
        while p in data:
            data = data.get(p, {})
        r = result[p] = {}
        logging.debug('Data:\n%s' % json.dumps(data, indent=2))
        if 'buckets' not in data:
            r.update(get_agg_values(data, prefixed_units[p]))
        else:
            r.update(get_bucketed_agg_values(data, prefixed_units[p]))
        logging.debug('Result:\n%s' % json.dumps(r, indent=2))

    if 'buckets' not in data:
        for unit in clean_units:
            result[unit] = get_single_agg_value(orig_data, unit)
        return result

    bucketed = get_bucketed_agg_values(data, clean_units)
    for b in bucketed:
        r = result.get(b, {})
        r.update(bucketed[b])
        if set(r.keys()) == set(['total']):
            result[b] = r['total']
    return result

# <---
# Common functions for ES respomse parsing

# =====

# Specific functions for different API methods
# --->

def construct_chain(chain_data):
    """ Reconstruct chain structure from the chain_data.

    Chain is a group of tasks where the first task is the root, and each next
    task has one of the previous ones as its parent (parent's output includes
    child's input).

    :param chain_data: chain_data of all tasks in the chain (in the form
                       corresponding outcome of ``method.chain_data()``)
    :type chain_data: list

    :return: constructed chain -- hash with keys of task IDs and values of the
             given task's child IDs:
             {
                 ...,
                 taskidN: [childN1_id, childN2_id, ...],
                 ...
             }
    :rtype: dict
    """
    chain = {}
    # Order data from longest to shortest lists. Processing [1, 2, 3] is faster
    # than processing [1], then [1, 2] and then [1, 2, 3].
    # TODO: check this more extensively.
    chain_data.sort(key=lambda cd: len(cd), reverse=True)
    for cd in chain_data:
        cd.reverse()
        child = False
        for tid in cd:
            if tid in chain:
                if child:
                    chain[tid].append(child)
                break
            else:
                chain[tid] = []
                if child:
                    chain[tid].append(child)
                child = tid
    return chain


def campaign_stat(stat_data, events_src=None):
    """ Transform ES response into user-friendly format.

    :param stat_data: ES response to 'campaign-stat' query
    :type stat_data: dict
    :param events_src: source of data for 'output' events.
                       Possible values:
                       * 'ds'   -- number of events in output datasets
                                   (default);
                       * 'task' -- number of processed events of 'done'
                                   and 'finished' tasks;
                       * 'all'  -- provide all possible values as hash.
    :type events_src: str

    :return: properly formatted response for ``method.campaign_stat()``
    :rtype: dict
    """
    r = {}
    data = {}
    events_src_values = ['ds', 'task', 'all']
    if not events_src:
        events_src = events_src_values[0]
    elif events_src not in events_src_values:
        raise ValueError('(events_src) unexpected value: %s' % events_src)
    r['_took_storage_ms'] = stat_data.pop('took', None)
    r['_total'] = stat_data.get('hits', {}).pop('total', None)
    r['_data'] = data

    data['tasks_processing_summary'] = {}
    data['overall_events_processing_summary'] = {}
    data['tasks_updated_24h'] = {}
    data['events_24h'] = {}
    data['last_update'] = stat_data.get('aggregations', {}) \
                                   .get('last_update', {}) \
                                   .get('value_as_string', None)
    data['date_format'] = ES_DATE_FORMAT

    steps = stat_data.get('aggregations', {}) \
                     .get('steps', {}) \
                     .get('buckets', [])
    for step in steps:
        # Events processing summary
        esp_o = {}
        esp_o['ds'] = step.get('output_events', {}) \
                          .get('output_events', {}) \
                          .get('value', None)
        esp_o['task'] = step.get('finished', {}) \
                            .get('processed_events', {}) \
                            .get('value', None)
        esp_o = esp_o[events_src] if events_src != 'all' else esp_o

        eps = {'input': step.get('input_events', {}).get('value', None),
               'output': esp_o,
               'ratio': None
               }
        try:
            eps['ratio'] = eps['output'] / eps['input']
        except TypeError:
            # Values are not numeric (None or dict)
            pass

        # Tasks processing: summary and updates
        tps = {}
        tps['total'] = step['doc_count']
        tps['start'] = step.get('start', {}) \
                           .get('value_as_string', None)
        tps['end'] = step.get('end', {}) \
                         .get('value_as_string', None)

        tu24h = {}
        e24h = {}
        statuses = step.get('status', {}) \
                       .get('buckets', [])
        for status in statuses:
            tps[status['key']] = status['doc_count']
            tu24h[status['key']] = {}
            tu24h[status['key']]['total'] = status.get('doc_count', None)
            tu24h[status['key']]['updated'] = status.get('updated_24h', {}) \
                                                    .get('doc_count', None)

            # Events in last 24 hours
            e24h_cur = {}
            e24h_cur['ds'] = status.get('updated_24h', {}) \
                                   .get('finished', {}) \
                                   .get('output', {}) \
                                   .get('events', {}) \
                                   .get('value', None)
            e24h_cur['task'] = status.get('updated_24h', {}) \
                                     .get('finished', {}) \
                                     .get('processed_events', {}) \
                                     .get('value', None)
            for key in e24h_cur.keys():
                try:
                    e24h[key] = e24h.get(key, 0) + e24h_cur[key]
                except TypeError:
                    # Current value is None
                    pass

        e24h = e24h.get(events_src, None) if events_src != 'all' else e24h
        if e24h == {}:
            e24h = None

        data['tasks_processing_summary'][step['key']] = tps
        data['overall_events_processing_summary'][step['key']] = eps
        data['tasks_updated_24h'][step['key']] = tu24h
        data['events_24h'][step['key']] = e24h
    return r


def step_stat(data, agg_units=[], step_type=None):
    """ Transform ES query response to required response format.

    :param data: ES response
    :type data: dict
    :param agg_units: list of aggregation units to look for
                      in the ES response
    :type agg_units: list
    :param step_type: 'step' or 'format_ctag'; defines which value
                      should be used for completion percents calculation.
    :type step_type: str

    :returns: prepared response data
    :rtype: dict
    """
    if not step_type:
        step_type = STEP_TYPES[0]

    # Depending on step type, different fields should be used for steps
    # completion calculation
    if step_type == 'step':
        events_field = 'total_events'
    elif step_type == 'ctag_format':
        events_field = 'processed_events'
    elif step_type not in STEP_TYPES:
        raise ValueError(step_type, "Unknown step type (expected one of: %s)"
                                    % STEP_TYPES)

    statuses = {0: 'StepNotStarted',
                0.1: 'StepProgressing',
                0.9: 'StepDone'
                }

    r = {}
    r['_took_storage_ms'] = data.pop('took')
    r['_total'] = data.get('hits', {}) \
                      .get('total', None)
    r['_data'] = []
    logging.debug('ES response data:\n%s' % json.dumps(data, indent=2))
    steps = steps_iterator(data.get('aggregations', {}))
    for name, step_data in steps:
        d = {'name': name}
        logging.debug('Step data (%s):\n%s' % (name, json.dumps(step_data,
                                                                indent=2)))
        step = get_agg_values(step_data, agg_units + ['total'])
        logging.debug('Parsed step data:\n%s' % json.dumps(step, indent=2))

        input_ds_data = step.pop('input', {})
        d['input_bytes'] = input_ds_data.get('input_bytes', None)
        d['input_not_removed_tasks'] = input_ds_data.get('total', None)

        output_ds_data = step.pop('output', {})
        d['output_bytes'] = output_ds_data.get('bytes', None)
        d['output_not_removed_tasks'] = output_ds_data.get('total', None)

        d['cpu_failed'] = step.pop('hs06_failed', None)
        d['total_tasks'] = step.pop('total', None)

        try:
            d['duration'] = step.pop('task_duration') / 86400.0 / 1000
        except KeyError, TypeError:
            d['duration'] = None

        # Calculate completion percentage and step status
        inp = step.get('input_events', 0)
        if not inp:
            d['step_status'] = 'Unknown'
            d['percent_done'] = 0.0
            d['percent_runnning'] = 0.0
            d['percent_pending'] = 0.0
            d['percent_not_started'] = 100.0
            d['finished_tasks'] = 0
            d['finished_bytes'] = 0
        else:
            # Completion
            d['percent_done'] = float(step[events_field]) / inp
            try:
                run = step['status']['running']
                running_events = run['input_events'] - run[events_field]
            except KeyError, TypeError:
                running_events = 0
            fin = step.get('status', {}).get('finished', {})
            done = step.get('status', {}).get('done', {})
            finished_events = \
                fin.get('input_events', 0) + done.get('input_events', 0) \
                - fin.get(events_field, 0) - done.get(events_field, 0)
            d['percent_running'] = \
                float(running_events) / step['input_events'] * 100
            d['percent_pending'] = \
                float(inp - step[events_field] - running_events
                      - finished_events) / inp * 100
            d['percent_pending'] = max(0, d['percent_pending'])
            d['finished_tasks'] = fin.get('total', 0) + done.get('total', 0)
            d['finished_bytes'] = fin.get('input', {}).get('input_bytes', 0) \
                + done.get('input', {}).get('input_bytes', 0)

            # Step status
            tres = statuses.keys()
            tres.sort()
            tres.reverse()
            d['step_status'] = statuses[tres[-1]]
            for t in tres:
                if d['percent_done'] > t:
                    d['step_status'] = statuses[t]
                    break

        del step['status']
        d.update(step)
        r['_data'].append(d)
    return r
