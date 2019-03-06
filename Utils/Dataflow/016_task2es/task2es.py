#!/bin/env python
"""
DKB Dataflow stage 016 (task2es)

Transform task metadata (from ProdSys) to fit ES scheme.

Authors:
  Marina Golosova (marina.golosova@cern.ch)
"""

import os
import sys
import traceback

import re

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


def add_es_index_info(data):
    """ Update data with required for ES indexing info.

    Add fields:
      _id => taskid
      _type => 'task'

    Return value:
      False -- update failed, skip the record
      True  -- update successful
    """
    if type(data) is not dict:
        return False
    if not data.get('taskid'):
        return False
    data['_id'] = data['taskid']
    data['_type'] = 'task'
    return True


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
    PHYS_CATEGORIES_MAP = {
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
    match = {}
    categories = []
    for phys_category in PHYS_CATEGORIES_MAP:
        current_map = [x.strip(' ').lower()
                       for x in PHYS_CATEGORIES_MAP[phys_category]]
        if hashtags is not None:
            match[phys_category] = len([x for x in hashtags
                                        if x in current_map])
    categories = [cat for cat in match if match[cat] > 0]
    if not categories and taskname:
        phys_short = taskname.split('.')[2].lower()
        if re.search('singletop', phys_short) is not None:
            categories.append("SingleTop")
        if re.search('ttbar', phys_short) is not None:
            categories.append("TTbar")
        if re.search('jets', phys_short) is not None:
            categories.append("Multijet")
        if re.search('h125', phys_short) is not None:
            categories.append("Higgs")
        if re.search('ttbb', phys_short) is not None:
            categories.append("TTbarX")
        if re.search('ttgamma', phys_short) is not None:
            categories.append("TTbarX")
        if re.search('_tt_', phys_short) is not None:
            categories.append("TTbar")
        if re.search('upsilon', phys_short) is not None:
            categories.append("BPhysics")
        if re.search('tanb', phys_short) is not None:
            categories.append("SUSY")
        if re.search('4topci', phys_short) is not None:
            categories.append("Exotic")
        if re.search('xhh', phys_short) is not None:
            categories.append("Higgs")
        if re.search('3top', phys_short) is not None:
            categories.append("TTbarX")
        if re.search('_wt', phys_short) is not None:
            categories.append("SingleTop")
        if re.search('_wwbb', phys_short) is not None:
            categories.append("SingleTop")
        if re.search('_wenu_', phys_short) is not None:
            categories.append("Wjets")
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


def transform_chain_data(data):
    """ Transform chain_data into array of integers and get chain_id from it.

    chain_id is the taskid of the task chain's root.

    :param data: data to be updated, must contain taskid and proper chain_data
                 (string of numbers separated by commas).
    :type data: dict

    :return: True if update was successful, False otherwise (chain_id and
             chain_data will be updated so that the task is considered
             independent)
    :rtype: bool
    """
    if type(data) is not dict:
        return False
    taskid = data.get('taskid')
    chain_data = data.get('chain_data')
    if not chain_data or not chain_data.replace(',', '').isdigit():
        sys.stderr.write('(WARN) Task %s: cannot transform chain_data "%s", '
                         'it seems to be incorrect. Setting chain_id=%s, '
                         'chain_data=[%s].\n'
                         % (taskid, chain_data, taskid, taskid))
        data['chain_id'] = taskid
        data['chain_data'] = [taskid]
        return False
    chain_data = [int(i) for i in chain_data.split(',')]
    data['chain_id'] = chain_data[0]
    data['chain_data'] = chain_data
    return True


def process(stage, message):
    """ Single message processing. """
    data = message.content()

    # 1. Add the fields for ES indexing
    if not add_es_index_info(data):
        sys.stderr.write("(WARN) Skip message (not enough info"
                         " for ES indexing).\n")
        return True
    # 2. Unify hashtag_list
    hashtags = data.get('hashtag_list')
    if hashtags:
        hashtags = hashtags.lower().split(',')
        data['hashtag_list'] = [x.strip() for x in hashtags]
    # 3. Detect physics category
    data['phys_category'] = get_category(data)
    # 4. Unify output_formats
    output_formats = data.get('output_formats')
    if output_formats:
        data['output_formats'] = output_formats.split('.')
    # 5. Produce derived value 'input_events'
    inp_events = input_events(data)
    if inp_events:
        data['input_events'] = inp_events
    # 6. Save chain_data as array of integers, extract chain_id from it
    transform_chain_data(data)

    out_message = JSONMessage(data)
    stage.output(out_message)
    return True


def main(args):
    """ Program body. """
    stage = JSONProcessorStage()
    stage.process = process

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
