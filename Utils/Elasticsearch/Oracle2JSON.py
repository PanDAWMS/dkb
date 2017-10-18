#!/bin/env python
import json
import argparse
import ConfigParser
import re
import DButils
import sys
from datetime import datetime, timedelta

try:
    import cx_Oracle
except:
    print "****ERROR : DButils. Cannot import cx_Oracle"
    pass

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
        step_hours = int(config.get("timestamps", "step_hours"))
        final_date = config.get("timestamps", "final")
        tasks_sql_file = config.get("queries", "tasks")
        datasets_sql_file = config.get("queries", "datasets")

        # changeable data
        offset_date = config.get("timestamps", "offset")
        if offset_date == '':
            offset_date = initial_date
    except IOError:
        sys.stderr.write('Could not read config file %s' % conf)

    conn, cursor = connectDEFT_DSN(dsn)

    if mode == 'PLAIN':

        while (datetime.strptime(offset_date, "%d-%m-%Y %H:%M:%S") < datetime.strptime(final_date, "%d-%m-%Y %H:%M:%S")):
            # get offset from configuration file
            offset_date = get_offset()
            end_date = (datetime.strptime(offset_date, "%d-%m-%Y %H:%M:%S") +
                        timedelta(hours=step_hours)).strftime("%d-%m-%Y %H:%M:%S")
            # get all tasks for time range (offset date + 24 hours)
            tasks = query_executor(conn, tasks_sql_file, offset_date, end_date)
            # set end_date as current offset in configuration file for next step
            update_offset(end_date)
            # joining datasets to tasks
            ndjson_string = ''
            for task in tasks:
                task['phys_category'] = get_category(task)
                ndjson_string += json.dumps(task) + '\n'
            # send NDJSON string to STDOUT
            sys.stdout.write(ndjson_string)

    elif mode == 'SQUASH':

        while (datetime.strptime(offset_date, "%d-%m-%Y %H:%M:%S") < datetime.strptime(final_date, "%d-%m-%Y %H:%M:%S")):
            # get offset from configuration file
            offset_date = get_offset()
            end_date = (datetime.strptime(offset_date, "%d-%m-%Y %H:%M:%S") +
                        timedelta(hours=step_hours)).strftime("%d-%m-%Y %H:%M:%S")
            # get all tasks for time range (offset date + 24 hours)
            tasks = query_executor(conn, tasks_sql_file, offset_date, end_date)
            # get I/O datasets for time range (offset date + 24 hours)
            datasets = query_executor(conn, datasets_sql_file, offset_date, end_date)
            # set end_date as current offset in configuration file for next step
            update_offset(end_date)
            ndjson_string = ''
            for idx, task in enumerate(tasks):
                task['phys_category'] = get_category(task)
                task['input_datasets'] = []
                task['output_datasets'] = []
                for ds in datasets:
                    if ds['taskid'] == task['taskid']:
                        if ds['type'] == 'input':
                            task['input_datasets'].append(ds['datasetname'])
                        elif ds['type'] == 'output':
                            task['output_datasets'].append(ds['datasetname'])
                ndjson_string += json.dumps(task) + '\n'
                if (idx % 50 == 0):
                    sys.stdout.write(ndjson_string)
                    ndjson_string = ''
            sys.stdout.write(ndjson_string)

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
        sys.stderr.write('File open error. No such file.')


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
    parser.add_argument('--mode', help='Mode of execution: PLAIN | SQUASH')
    return parser.parse_args()

if  __name__ == '__main__':
    main()
