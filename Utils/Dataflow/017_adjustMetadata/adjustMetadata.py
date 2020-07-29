#!/bin/env python
"""
DKB Dataflow stage 017 (adjustMetadata)

Transform task metadata (from ProdSys) to fit ES scheme.

Authors:
  Marina Golosova (marina.golosova@cern.ch)
  Vasilii Aulov (vasilii.aulov@cern.ch)
"""

import os
import sys

import re

try:
    base_dir = os.path.dirname(__file__)
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
    from pyDKB.dataflow.stage import ProcessorStage
    from pyDKB.dataflow.communication.messages import JSONMessage
    from pyDKB.dataflow import messageType
    from pyDKB.dataflow.exceptions import DataflowException
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


PHYS_CATEGORIES_HASHTAGS_MAP = {
    'BPhysics': ['charmonium', 'jpsi', 'bs', 'bd', 'bminus', 'bplus',
                 'charm', 'bottom', 'bottomonium', 'b0'],
    'BTag': ['btagging'],
    'Diboson': ['diboson', 'zz', 'ww', 'wz', 'wwbb', 'wwll'],
    'DrellYan': ['drellyan', 'dy'],
    'Exotic': ['exotic', 'monojet', 'blackhole', 'technicolor',
               'randallsundrum', 'wprime', 'zprime', 'magneticmonopole',
               'extradimensions', 'warpeded', 'randallsundrum',
               'contactinteraction', 'seesaw'],
    'GammaJets': ['photon', 'diphoton'],
    'Higgs': ['whiggs', 'zhiggs', 'mh125', 'higgs', 'vbf', 'smhiggs',
              'bsmhiggs', 'chargedhiggs'],
    'Minbias': ['minbias'],
    'Multijet': ['dijet', 'multijet', 'qcd'],
    'Performance': ['performance'],
    'SingleParticle': ['singleparticle'],
    'SingleTop': ['singletop'],
    'SUSY': ['bino', 'susy', 'pmssm', 'leptosusy', 'rpv', 'mssm'],
    'Triboson': ['triplegaugecoupling', 'triboson', 'zzw', 'www'],
    'TTbar': ['ttbar'],
    'TTbarX': ['ttw', 'ttz', 'ttv', 'ttvv', '4top', 'ttww'],
    'Upgrade': ['upgrad'],
    'Wjets': ['w'],
    'Zjets': ['z']}


PHYS_CATEGORIES_PHYS_SHORT_MAP = {
    'Exotic': ['4topci'],
    'TTbarX': ['ttbb', 'ttgamma', '3top'],
    'Higgs': ['h125', 'xhh'],
    'BPhysics': ['upsilon'],
    'Wjets': ['_wenu_'],
    'Multijet': ['jets'],
    'SUSY': ['tanb'],
    'TTbar': ['ttbar', '_tt_'],
    'SingleTop': ['singletop', '_wt', '_wwbb']}


def get_category(row):
    """
    Each task can be associated with a number of Physics Categories.
    1) search category in hashtags list
    2) if not found in hashtags, then search category in phys_short
       field of tasknames
    :param row
    :return:
    """
    hashtags = row.get('hashtag_list')
    taskname = row.get('taskname')
    match = {}
    categories = []
    for phys_category in PHYS_CATEGORIES_HASHTAGS_MAP:
        current_map = [x.strip(' ').lower()
                       for x in PHYS_CATEGORIES_HASHTAGS_MAP[phys_category]]
        if hashtags is not None:
            match[phys_category] = len([x for x in hashtags
                                        if x in current_map])
    categories = [cat for cat in match if match[cat] > 0]
    if not categories and taskname:
        phys_short = taskname.split('.')[2].lower()
        for phys_category in PHYS_CATEGORIES_PHYS_SHORT_MAP:
            for s in PHYS_CATEGORIES_PHYS_SHORT_MAP[phys_category]:
                if re.search(s, phys_short) is not None:
                    categories.append(phys_category)
                    break
    if not categories:
        categories = ["Uncategorized"]
    return categories


def input_events(data):
    """ Calculate derived value 'input_events'.

    For EVNT tasks:
     * n_files_to_be_used  -> n_files_to_be_used *
                                * n_events_per_job / (n_files_per_job|1)
     * !n_files_to_be_used -> processed_events

    For other tasks:
     * min(requested_events, primary_input_events)

    :param data: task metadata
    :type data: dict

    :return: derived value 'input_events'
             (None if input data do not provide enough information)
    :rtype: int, NoneType
    """
    result = None
    if data.get('step_name', '').lower() == 'evgen':
        try:
            to_be_used = int(data.get('n_files_to_be_used'))
        except TypeError:
            result = data.get('processed_events')
        else:
            try:
                files_per_job = int(data.get('n_files_per_job'))
            except TypeError:
                files_per_job = 1
            try:
                events_per_job = int(data.get('n_events_per_job'))
            except TypeError:
                events_per_job = 0
            result = to_be_used * events_per_job / files_per_job
    else:
        try:
            requested = int(data.get('requested_events'))
        except TypeError:
            requested = -1
        try:
            ds_events = int(data.get('primary_input_events'))
        except TypeError:
            ds_events = requested
        if requested < 0:
            result = ds_events
        elif ds_events < 0:
            result = requested
        else:
            result = min(requested, ds_events)
    if result < 0:
        result = None
    return result


def input_events_v2(data):
    """ Calculate derived value 'input_events_v2'.

    For a first task in a chain:
     * total_req_events (if defined);
     * else: primary_input_events (if defined);
     * else: requested_events.

    For other tasks:
     * parent_total_events (if parent is not derivation);
     * else: requested_events.

    For details please contact Mikhail Borodin <Mikhail.Borodin@cern.ch>.

    :param data: task metadata
    :type data: dict

    :return: derived value 'input_events_v2'
             (None if input data do not provide enough information)
    :rtype: int, NoneType
    """
    result = None

    # First task in chain
    if data['taskid'] == data['chain_id']:
        if data.get('total_req_events'):
            result = data['total_req_events']
        elif data.get('primary_input_events'):
            result = data['primary_input_events']
        else:
            result = data.get('requested_events')
    # Other tasks
    else:
        if data.get('parent_task_name') is not None and \
                '.deriv.' not in data.get('parent_task_name', ''):
            result = data.get('parent_total_events')
        else:
            result = data.get('requested_events')

    return result


def processed_events_v2(data):
    """ Calculate number of processed events (v2).

    V1 is calculated at Stage 009; V2 is suggested by Mikhail Borodin
    <Mikhail.Borodin@cern.ch> to take into account wider range of possible
    situations.

    :param data: task metadata
    :type data: dict

    :return: derived value 'input_events_v2'
             (None if input data do not provide enough information)
    :rtype: int, NoneType
    """
    result = None
    if data.get('input_events_v2') and \
            data.get('processed_events') and \
            data.get('requested_events'):
        result = int(data['input_events_v2']
                     * data['processed_events'] / data['requested_events'])
    # For EVNT tasks 'requested_events' is None, so we use 'total_events'
    # (just as in v1)
    elif data.get('step_name', '').lower() == 'evgen':
        result = data['total_events']

    return result


def transform_chain_data(data):
    """ Transform chain_data into proper task metadata fields.

    New/updated fields:
      - chain_data:          task IDs of all tasks in chain prior to (and
                             including) the current one (array of integers);
      - chain_id:            first task ID in the chain (chain's root);
      - parent_taskname:     name of the parent task;
      - parent_total_events: total_events parameter of the parent task.

    :param data: data to be updated, must contain proper chain_data
                 (string of numbers separated by commas) or taskid
    :type data: dict

    :return: True if update was completely successful, False otherwise
    :rtype: bool
    """
    if type(data) is not dict:
        sys.stderr.write('(WARN) Function transform_chain_data() received '
                         'non-dict data: %s. Skipping.\n' % str(data))
        return False
    chain_data = data.get('chain_data')
    if not chain_data:
        taskid = data.get('taskid')
        if not taskid:
            sys.stderr.write('(WARN) Function transform_chain_data() '
                             'received data with empty chain_data and '
                             'without taskid: %s. Skipping.\n' % str(data))
            return False
        sys.stderr.write('(WARN) Task %s: chain_data field is empty.'
                         'Setting chain_id=%s, chain_data=[%s].\n'
                         % (taskid, taskid, taskid))
        data['chain_id'] = taskid
        data['chain_data'] = [taskid]
        return False
    chain_items = chain_data.split(',')
    try:
        parent_id, parent_name, parent_events = chain_items[-2].split(':')
        chain_items[-2] = parent_id
        data['parent_taskname'] = parent_name
        data['parent_total_events'] = parent_events
    except IndexError:
        # No parent (chain root)
        pass
    except ValueError:
        # Invalid parent data
        sys.stderr.write('(WARN) Invalid parent task data (expected format:'
                         ' "p_tid:p_name:p_total_events"): %s (tid: %s).\n'
                         % (chain_items[-2], data.get('taskid')))
    try:
        chain_data = [int(i) for i in chain_items]
        data['chain_id'] = chain_data[0]
        data['chain_data'] = chain_data
    except ValueError, err:
        sys.stderr.write('(WARN) Invalid chain_data item: %s (tid: %s).\n'
                         % (err, data.get('taskid')))
        taskid = data.get('taskid')
        if not taskid:
            sys.stderr.write('(WARN) Task id is missed; skip'
                             ' transform_chain_data().')
            return False
        sys.stderr.write('(INFO) Setting chain_id=%s, chain_data=[%s]'
                         ' (tid: %s).\n'
                         % (taskid, taskid, taskid))
        data['chain_id'] = taskid
        data['chain_data'] = [taskid]
        return False
    return True


def ami_tags_chain(data):
    """ Get AMI tags chain from task name.

    :param data: task metadata
    :type data: dict

    :returns: AMI tags chain,
              None if input data do not provide enough information
    :rtype: str, NoneType
    """
    taskname = data.get('taskname')
    try:
        ami_tags = taskname.split('.')[-1]
    except AttributeError:
        # `taskname` is None or something else, but not a string
        ami_tags = None
    return ami_tags


def generate_step_names(data):
    """ Add fields with name of step to which task belongs.

    There are different ways to tell one step from another:
    - MC production step name (already exists as 'step_name' field);
    - current AMI tag + output data format;
    - chain of AMI tags + output data format.

    The latter is supposed to be the most universal, but initially only the
    first one was used, then for some cases the second was invented, and...
    ...and there's no good way to make things as they are supposed to be
    all at once. So we need all the possible namings.

    :param data: task metadata (will be altered in place)
    :type data: dict

    :returns: None
    :rtype: NoneType
    """
    ignore_formats = ['LOG']
    output_formats = data.get('output_formats', [])
    if not isinstance(output_formats, list):
        output_formats = [output_formats]
    ctag = data.get('ctag')
    tags = data.get('ami_tags')
    data['ctag_format_step'] = []
    data['ami_tags_format_step'] = []
    for data_format in output_formats:
        if data_format in ignore_formats:
            continue
        if ctag:
            data['ctag_format_step'].append(':'.join([ctag, data_format]))
        if tags:
            data['ami_tags_format_step'].append(':'.join([tags, data_format]))
    if not data['ctag_format_step']:
        del data['ctag_format_step']
    if not data['ami_tags_format_step']:
        del data['ami_tags_format_step']


def process(stage, message):
    """ Single message processing. """
    data = message.content()

    if type(data) is not dict:
        sys.stderr.write('(WARN) Message contains non-dict data: %r. '
                         'Skipping.\n' % data)
        return False
    # Missing taskid indicates that the message is, most likely, incorrect.
    if 'taskid' not in data:
        sys.stderr.write('(WARN) Message contains no taskid: %r. '
                         'Skipping.\n' % data)
        return False

    # 1. Unify hashtag_list
    hashtags = data.get('hashtag_list')
    if hashtags:
        hashtags = hashtags.lower().split(',')
        data['hashtag_list'] = [x.strip() for x in hashtags]
    # 2. Detect physics category
    data['phys_category'] = get_category(data)
    # 3. Unify output_formats
    output_formats = data.get('output_formats')
    if output_formats:
        data['output_formats'] = output_formats.split('.')
    # 4. Produce derived value 'input_events'
    inp_events = input_events(data)
    if inp_events:
        data['input_events'] = inp_events
    # 5. Save chain_data as array of integers, extract chain_id from it
    transform_chain_data(data)
    # 6. Calculate 'input_events_v2'
    inp_events = input_events_v2(data)
    if inp_events is not None:
        data['input_events_v2'] = inp_events
    # 7. Calculate 'processed_events_v2'
    pr_events = processed_events_v2(data)
    if pr_events is not None:
        data['processed_events_v2'] = pr_events
    # 8. AMI tags chain
    ami_tags = ami_tags_chain(data)
    if ami_tags:
        data['ami_tags'] = ami_tags
    # 9. Step names
    generate_step_names(data)

    out_message = JSONMessage(data)
    stage.output(out_message)
    return True


def main(args):
    """ Program body. """
    stage = ProcessorStage()
    stage.set_input_message_type(messageType.JSON)
    stage.set_output_message_type(messageType.JSON)

    stage.process = process

    stage.configure(args)
    error_code = stage.run()

    if error_code == 0:
        stage.stop()

    exit(error_code)


if __name__ == '__main__':
    main(sys.argv[1:])
