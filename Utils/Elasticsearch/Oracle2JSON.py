#!/bin/env python
import json
import argparse
import ConfigParser
import re
import DButils
import sys
from datetime import datetime, timedelta
from collections import defaultdict

try:
    import cx_Oracle
except:
    print "****ERROR : DButils. Cannot import cx_Oracle"
    pass

# Policies
PLAIN_POLICY='PLAIN'
SQUASH_POLICY='SQUASH'

def connectDEFT_DSN(dsn):
    connect = cx_Oracle.connect(dsn)
    cursor = connect.cursor()

    return connect, cursor

def main():
    """
    --config <config file> --mode <PLAIN|SQUASH>
    :return:
    """
    args = parsingArguments()
    global conf
    conf = args.config
    global mode
    mode = args.mode

    # read initial configuration
    config = ConfigParser.ConfigParser()
    try:
        config.read(conf)
        # unchangeable data
        dsn = config.get("oracle", "dsn")
        initial_date = config.get("timestamps", "initial")
        step = config.get("timestamps", "step")
        step_seconds = interval_seconds(step)
        final_date = config.get("timestamps", "final")
        if final_date == '':
            final_date = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
        queries_cfg = config.items("queries")
        queries = {}
        for (qname, file) in queries_cfg:
            queries[qname] = {'file': file}
        # changeable data
        offset_date = config.get("timestamps", "offset")
        if offset_date == '':
            offset_date = initial_date
    except IOError:
        sys.stderr.write('Could not read config file %s\n' % conf)

    conn, cursor = connectDEFT_DSN(dsn)
    process(conn, offset_date, final_date, step_seconds, queries)


def plain(conn, queries, offset_date, end_date):
    tasks = query_executor(conn, queries['tasks']['file'], offset_date, end_date)
    for task in tasks:
        task['phys_category'] = get_category(task)
        # send NDJSON string to STDOUT
        sys.stdout.write(json.dumps(task) + '\n')


def squash(conn, queries, offset_date, end_date):
    tasks = query_executor(conn, queries['tasks']['file'], offset_date, end_date)
    datasets = query_executor(conn, queries['datasets']['file'], offset_date, end_date)
    join_results(tasks, squash_records(datasets))

def squash_records(rec):
    """
    a single-pass iterator (for a generator) restructuring list of datasets:
    from view:
    [
       {"taskid": 1, "type": "input", "datasetname": "First_in"},
       {"taskid": 1, "type": "input", "datasetname": "Second_in"},
       {"taskid": 1, "type": "input", "datasetname": "Third_in"},
       {"taskid": 1, "type": "output", "datasetname": "First_out"},
       {"taskid": 1, "type": "output", "datasetname": "Second_out"},
       {"taskid": 1, "type": "output", "datasetname": "Third_out"},
       {"taskid": 2, "type": "input", "datasetname": "First_in"},
       {"taskid": 2, "type": "output", "datasetname": "First_out"},
       {"taskid": 2, "type": "output", "datasetname": "Second_out"},
    ...]
     to
    [
       {"taskid": 1,
        "input": ["First_in", "Second_in", "Third_in"],
        "output": ["First_out", "Second_out", "Third_out"]
       },
       {"taskid": 2,
        "input": ["First_in"],
        "output": ["First_out","Second_out"]
       },
    ...]
    """
    grouper = defaultdict(lambda: defaultdict(list))
    for d in rec:
        grouper[d['taskid']][d['type']].append(d['datasetname'])
    return [dict(taskid=k, **v) for k, v in grouper.items()]


def join_results(tasks, datasets):
    d = defaultdict(dict)
    for l in (tasks, datasets):
        for elem in l:
            d[elem['taskid']].update(elem)
            sys.stdout.write(json.dumps(d[elem['taskid']]) + '\n')

def process(conn, offset_date, final_date, step_seconds, queries):
    while (datetime.strptime(offset_date, "%d-%m-%Y %H:%M:%S") < datetime.strptime(final_date, "%d-%m-%Y %H:%M:%S")):
        end_date = (datetime.strptime(offset_date, "%d-%m-%Y %H:%M:%S") +
                    timedelta(seconds=step_seconds)).strftime("%d-%m-%Y %H:%M:%S")
        if end_date > final_date:
            end_date = final_date
        sys.stderr.write("(TRACE) %s: Run queries for interval from %s to %s\n"
                         % (datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
                            offset_date, end_date))
        if mode == SQUASH_POLICY:
            squash(conn, queries, offset_date, end_date)
        elif mode == PLAIN_POLICY:
            plain(conn, queries, offset_date, end_date)
        update_offset(end_date)
        offset_date = end_date

def get_initial_date():
    """
     TODO:
        Procedure to obtain the min(timestamp) from t_production_task
        SELECT min(timestamp) from t_production_task;
    """
    return None

def query_executor(conn, sql_file, offset_date, end_date):
    """
    Execution of query with offset from file
    """
    try:
        file_handler = open(sql_file)
        query = file_handler.read().rstrip().rstrip(';') % (offset_date, end_date)
        return DButils.ResultIter(conn, query, 1000, True)
    except IOError:
        sys.stderr.write('File open error. No such file (%s).\n' % sql_file)
        sys.exit(2)


def get_offset():
    config = ConfigParser.ConfigParser()
    config.read(conf)
    return config.get("timestamps", "offset")

def update_offset(new_offset):
    """
    Updating offset value in configuration file
    """
    config = ConfigParser.RawConfigParser()
    config.optionxform = str
    config.read(conf)
    config.set('timestamps', 'offset', new_offset)
    with open(conf, 'w') as configfile:
        config.write(configfile)

def interval_seconds(step):
    """
    Convert human-readable interval into seconds.

    If no suffix present, take step as seconds.
    """
    try:
        return int(step)
    except ValueError:
        pass
    if len(step) < 2:
        raise ValueError("Failed to decode interval: %s" % step)
    suffix = { 'd': 86400, 'h': 3600, 'm': 60, 's': 1}
    val = step[:-1]
    try:
        mul = suffix[step[-1]]
        return int(val) * mul
    except ValueError:
        raise ValueError("Failed to decode numeric part of the interval: %s"
                         % step)
    except KeyError:
        raise ValueError("Failes to decode index of the interval: %s" %step)

def get_category(row):
    """
    Each task can be associated with a number of Physics Categories.
    1) search category in hashtags list
    2) if not found in hashtags, then search category in phys_short field of tasknames
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
        current_map = [x.strip(' ').lower() for x in PHYS_CATEGORIES_MAP[phys_category]]
        if hashtags is not None:
            match[phys_category] = len([x for x in hashtags.lower().split(',') if x.strip(' ') in current_map])
    categories = [cat for cat in match if match[cat] > 0]
    if len(categories) > 0:
        return categories
    else:
        phys_short = taskname.split('.')[2].lower()
        if re.search('singletop', phys_short) is not None: categories.append("SingleTop")
        if re.search('ttbar', phys_short) is not None: categories.append("TTbar")
        if re.search('jets', phys_short) is not None: categories.append("Multijet")
        if re.search('h125', phys_short) is not None: categories.append("Higgs")
        if re.search('ttbb', phys_short) is not None: categories.append("TTbarX")
        if re.search('ttgamma', phys_short) is not None: categories.append("TTbarX")
        if re.search('_tt_', phys_short) is not None: categories.append("TTbar")
        if re.search('upsilon', phys_short) is not None: categories.append("BPhysics")
        if re.search('tanb', phys_short) is not None: categories.append("SUSY")
        if re.search('4topci', phys_short) is not None: categories.append("Exotic")
        if re.search('xhh', phys_short) is not None: categories.append("Higgs")
        if re.search('3top', phys_short) is not None: categories.append("TTbarX")
        if re.search('_wt', phys_short) is not None: categories.append("SingleTop")
        if re.search('_wwbb', phys_short) is not None: categories.append("SingleTop")
        if re.search('_wenu_', phys_short) is not None: categories.append("Wjets")
        return categories
    return "Uncategorized"

def parsingArguments():
    parser = argparse.ArgumentParser(description='Process command line arguments.')
    parser.add_argument('--config', help='Configuration file path')
    parser.add_argument('--mode', help='Mode of execution: PLAIN | SQUASH',
                        choices=[PLAIN_POLICY, SQUASH_POLICY])
    return parser.parse_args()

if  __name__ == '__main__':
    main()
