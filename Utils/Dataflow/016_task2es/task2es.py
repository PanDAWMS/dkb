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
            match[phys_category] = len([x for x in hashtags.lower().split(',')
                                        if x.strip(' ') in current_map])
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


def process(stage, message):
    """ Single message processing. """
    data = message.content()
    data['phys_category'] = get_category(data)
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
