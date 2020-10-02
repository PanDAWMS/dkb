"""
Transformation scenarios for ES data.
"""

import logging
import json
from datetime import datetime

from api.common import STEP_TYPES
from common import (ES_DATE_FORMAT,
                    PREFIX_AGGS)


# Common functions for ES response parsing
# --->

def steps_iterator(data):
    """ Generator for iterator over steps data.

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
    raise NotImplementedError('steps_iterator')

    # TODO: implement new version or get rid of this function
    #       for steps are properly tagged in the nested scheme


def get_single_agg_value(data, unit):
    """ Get single stat value from data.

    Input data are supposed to be in one of the following forms:
    ```
    1. {<unit_name>: {... {<unit_name>: {'value': <desired_value>}}}}
    2. {'value': <desired_value>}
    3. {'doc_count': <desired_value>}
    ```

    (the 3rd variant is for special ``unit`` value 'total').

    :param data: part of ES response with value for a single agg unit
                 (without buckets)
    :type data: dict
    :param unit: aggregation unit name (without prefix)
                 (see :py:func:`common.get_agg_units_query`);
                 'total' (for the number of documents in given
                 aggregation)
    :type unit: str

    :return: unit value or None if can not extract value
    :rtype: object
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
    :param units: statistics units (:py:func:`common.get_agg_units_query`)

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
    :param units: statistics units (:py:func:`common.get_agg_units_query`)
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
# Common functions for ES response parsing

# =====

# Specific functions for different API methods
# --->


def task_steps_hist(data, start=None, end=None):
    """ Transform ES response for :py:func:`methods.task_steps_hist`.

    :param data: ES response
    :type data: dict

    :return: transformed data and method execution metadata
    :rtype: tuple(dict, dict)
    """
    rdata, metadata = {}, {}
    result = (rdata, metadata)
    rdata.update({'legend': [], 'data': {'x': [], 'y': []}})
    if not data:
        return result
    metadata['took_storage_ms'] = data.get('took')
    metadata['total'] = data.get('hits', {}).get('total')
    for step in data['aggregations']['steps']['buckets']:
        step_name = step['key']
        x = []
        y = []
        for bucket in step['chart_data']['buckets']:
            dt = datetime.fromtimestamp(bucket['key'] / 1000)
            if (not start or start <= dt) and (not end or end >= dt):
                x.append(dt.date())
                y.append(bucket['doc_count'])
        rdata['legend'].append(step_name)
        rdata['data']['x'].append(x)
        rdata['data']['y'].append(y)
    return result


def construct_chain(chain_data):
    """ Reconstruct chain structure from the chain_data.

    Chain is a group of tasks where the first task is the root, and each next
    task has one of the previous ones as its parent (parent's output includes
    child's input).

    :param chain_data: chain_data of all tasks in the chain (in the form
                       corresponding outcome of :py:func:`methods.chain_data`)
    :type chain_data: list

    :return: constructed chain in the format corresponding "data" part of the
             ``task/chain`` method (see :py:func:`api.handlers.task_chain`) and
             method execution metadata
    :rtype: tuple(dict, dict)
    """
    chain, metadata = {}, {}
    result = (chain, metadata)
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
    return result


def task_info(data):
    """ Transform ES response into list of dicts with task info.

    :param data: ES response
    :type data: dict

    :return: task and related datasets metadata in format required
             for :py:func:`api.handlers.task_kwsearch` and method execution
             metadata
    :rtype: tuple(list, dict)
    """
    rdata, metadata = [], {}
    result = (rdata, metadata)
    metadata['took_storage_ms'] = data['took']
    if not data['hits']['hits']:
        return result
    metadata['total'] = data['hits']['total']
    for hit in data['hits']['hits']:
        task = hit['_source']
        rdata.append(task)
    return result


def derivation_statistics(data, format):
    """ Transform ES response to be used in the API method response.

    Format of the returned values corresponds the format of a single
    element of returned data for method ``task/deriv`` (see
    :py:func:`api.handlers.task_deriv`).

    :param data: ES response
    :type data: dict
    :param format: data format to which ``data`` value is referred
    :type format: str

    :return: derivation efficiency data for given format and method
             execution metadata
    :rtype: tuple(dict, dict)
    """
    raise NotImplementedError('derivation_statistics')

    # TODO: reimplement according to what a new query response format
    #       (most likely it will be whole set of data, not for a single
    #       format)

    rdata, metadata = {}, {}
    result = (rdata, metadata)
    try:
        total = data['hits']['total']
        result_events = (data['aggregations']['output_datasets']['not_removed']
                         ['format']['sum_events']['value'])
        result_bytes = (data['aggregations']['output_datasets']['not_removed']
                        ['format']['sum_bytes']['value'])
        input_events = data['aggregations']['input_events']['value']
        input_bytes = data['aggregations']['input_bytes']['value']
        ratio = 0
        if input_bytes != 0:
            ratio = float(result_bytes) / float(input_bytes)
        events_ratio = 0
        if input_events != 0:
            events_ratio = float(result_events) / float(input_events)
        task_ids = [hit['_id'] for hit in data['hits']['hits']]
    except Exception:
        total = 0
        ratio = 0
        events_ratio = 0
        task_ids = []
    rdata.update({'output': format, 'tasks': total, 'task_ids': task_ids,
                  'ratio': ratio, 'events_ratio': events_ratio})
    return result


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

    :return: properly formatted response for ``campaign/stat`` method
             (see :py:func:`api.handlers.campaign_stat`) and method
             execution metadata
    :rtype: tuple(dict, dict)
    """
    raise NotImplementedError('campaign_stat')

    # TODO: check the code, it _might_ need to be changed.

    data, metadata = {}, {}
    result = (data, metadata)
    events_src_values = ['ds', 'task', 'all']
    if not events_src:
        events_src = events_src_values[0]
    elif events_src not in events_src_values:
        raise ValueError('(events_src) unexpected value: %s' % events_src)
    metadata['took_storage_ms'] = stat_data.pop('took', None)
    metadata['total'] = stat_data.get('hits', {}).pop('total', None)

    data['tasks_processing_summary'] = {}
    data['overall_events_processing_summary'] = {}
    data['tasks_updated_24h'] = {}
    data['events_daily_progress'] = {}
    data['last_update'] = stat_data.get('aggregations', {}) \
                                   .get('last_update', {}) \
                                   .get('value_as_string', None)
    data['date_format'] = ES_DATE_FORMAT

    steps = steps_iterator(stat_data.get('aggregations', {}))
    for step_name, step in steps:
        # Events processing summary
        esp_o = {}
        esp_o['ds'] = step.get('output_events', {}) \
                          .get('output_events', {}) \
                          .get('value', None)
        esp_o['task'] = step.get('finished', {}) \
                            .get('processed_events', {}) \
                            .get('value', None)
        esp_o = esp_o[events_src] if events_src != 'all' else esp_o

        eps = {'input': step.get('input_events', {})
                            .get('input_events', {})
                            .get('value', None),
               'output': esp_o,
               'ratio': None
               }
        try:
            eps['ratio'] = eps['output'] / eps['input']
        except TypeError:
            # Values are not numeric (None or dict)
            pass
        except ZeroDivisionError:
            # Number of input events is -- somehow -- zero
            pass

        # Tasks processing: summary and updates
        tps = {}
        tps['total'] = step['doc_count']
        tps['start'] = step.get('start', {}) \
                           .get('value_as_string', None)
        tps['end'] = step.get('end', {}) \
                         .get('value_as_string', None)

        tu24h = {}
        statuses = step.get('status', {}) \
                       .get('buckets', [])
        for status in statuses:
            tps[status['key']] = status['doc_count']
            tu24h[status['key']] = {}
            tu24h[status['key']]['total'] = status.get('doc_count', None)
            tu24h[status['key']]['updated'] = status.get('updated_24h', {}) \
                                                    .get('doc_count', None)

        events_daily = {}
        hist_data = step.get('finished', {}) \
                        .get('daily', {}) \
                        .get('buckets', {})
        for key in hist_data:
            e_cur = {}
            e_cur['ds'] = hist_data[key].get('output', {}) \
                                        .get('events', {}) \
                                        .get('value', None)
            e_cur['task'] = hist_data[key].get('processed_events', {}) \
                                          .get('value', None)
            events_daily[key] = \
                e_cur.get(events_src, None) if events_src != 'all' else e_cur
        if events_daily == {}:
            events_daily = None

        data['tasks_processing_summary'][step_name] = tps
        data['overall_events_processing_summary'][step_name] = eps
        data['tasks_updated_24h'][step_name] = tu24h
        data['events_daily_progress'][step_name] = events_daily
    return result


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

    :returns: prepared response data for ``step/stat`` method
              (see :py:func:`api.handlers.step_stat`) and method execution
              metadata
    :rtype: tuple(list, dict)
    """
    rdata, metadata = [], {}
    result = (rdata, metadata)

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
                10: 'StepProgressing',
                90: 'StepDone'
                }

    metadata['took_storage_ms'] = data.pop('took')
    metadata['total'] = data.get('hits', {}) \
                            .get('total', None)
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
            d['percent_done'] = float(step[events_field]) / inp * 100
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

        # Adjust 'percent done' value to avoid getting '100%' when it's not
        # (due to a roundoff)
        if d['percent_done'] > 99.99 and \
                d.get(events_field, 0) < d.get('input_events', 0):
            d['percent_done'] = 99.99

        rdata.append(d)
    return result
