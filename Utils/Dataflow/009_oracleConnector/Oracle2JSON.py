#!/bin/env python
import os
import json
import argparse
import ConfigParser
import re
import sys
from datetime import datetime, timedelta
import time
from collections import defaultdict

from OffsetStorage import FileOffsetStorage
from dbConnection import OracleConnection

# Policies
PLAIN_POLICY = 'PLAIN'
SQUASH_POLICY = 'SQUASH'

# Global variables
# ---
# Config file
conf = None
# Operating mode
mode = None
# Output stream
OUT = os.fdopen(sys.stdout.fileno(), 'w', 0)
# ---


def main():
    """ Main program cycle.

    USAGE:
       ./Oracle2JSON.py --config <config file> --mode <PLAIN|SQUASH>

    TODO:
       Obtain the min(timestamp) from t_production_task
         in case of no `initial_date` specified.
       Query: SELECT min(timestamp) from t_production_task;
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
        step = config.get("timestamps", "step")
        step_seconds = interval_seconds(step)
        final_date = str2date(config.get("timestamps", "final"))
        queries_cfg = config.items("queries")
        queries = {}
        for (qname, file) in queries_cfg:
            queries[qname] = {'file': file}
    except (IOError, ConfigParser.Error), e:
        sys.stderr.write('Failed to read config file (%s): %s\n'
                         % (conf, e))
        sys.exit(1)

    # Offset storage initialization
    offset_storage = init_offset_storage(config)
    if not offset_storage:
        sys.exit(2)

    conn = OracleConnection(dsn)
    if not conn.establish():
        sys.stderr.write("(ERROR) Failed to connect to Oracle. Exiting.\n")
        sys.exit(3)

    if not conn.save_queries(queries):
        sys.stderr.write("(ERROR) Queries seem to be misconfigured. Exiting.\n")
        sys.exit(1)

    process(conn, offset_storage, final_date, step_seconds)

def init_offset_storage(config):
    """ Get configured (or default) offset file.

    If file does not exist, create file.
    If the directory for that file does not exist, raise exception.

    :param config: stage configuration
    :type config: ConfigParser.ConfigParser
    :return: offset storage
    :rtype: FileOffsetStorage
    """
    config_file = conf
    offset_storage = None

    if not isinstance(config, ConfigParser.ConfigParser):
        raise ValueError("get_offset_file: ConfigParser object is expected;"
                         " got %s" % type(config))

    try:
        offset_file = config.get("logging", "offset_file")
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        sys.stderr.write('(INFO) Config: "offset_file" not found in section'
                         ' "logging". Using default value.\n')
        offset_file = '.offset'

    if not os.path.isabs(offset_file):
        config_dir = os.path.dirname(os.path.abspath(config_file))
        offset_file = os.path.join(config_dir, offset_file)

    try:
        offset_storage = FileOffsetStorage(offset_file)
        if not offset_storage.get():
            initial_date = config.get("timestamps", "initial")
            current_offset = str2date(initial_date)
            commit_offset(offset_storage, current_offset)
    except IOError, err:
        sys.stderr.write("(ERROR) %s\n" % err)

    return offset_storage


def commit_offset(offset_storage, new_offset):
    """ Save current offset to the storage.

    :param offset_storage: the offset storage
    :param new_offset: offset to commit
    :type offset_storage: OffsetStorage
    :type new_offset: datetime.datetime
    """
    timestamp = time.mktime(new_offset.timetuple())
    offset_storage.commit(timestamp)


def get_offset(offset_storage):
    """ Get current offset from the offset storage.

    :param offset_storage: the offset storage
    :type offset_storage: OffsetStorage
    :return: current offset
    :rtype: datetime.datetime
    """
    result = offset_storage.get()
    if result is not None:
        timestamp = float(result)
        result = datetime.fromtimestamp(timestamp)
    return result


def plain(conn, queries, start_date, end_date):
    """ Execute 'tasks' query.

    The only acceptable value for `queries` is ['tasks'].

    :param conn: open connection to Oracle
    :param queries: names of queries to execute
    :param start_date: start date
    :param end_date: end date
    :type conn: OracleConnection
    :type queries: list
    :type start_date: datetime.datetime
    :type end_date: datetime.datetime
    """
    if not conn.execute_saved(queries[0], start_date=start_date,
                              end_date=end_date):
        raise StopIteration
    tasks = conn.results(queries[0], 1000, True)
    for task in tasks:
        yield task


def squash(conn, queries, start_date, end_date):
    """ Execute queries 'tasks' and 'datesets' and squash the results.

    The only acceptable value for `queries` is ['tasks', 'datasets'].

    :param conn: open connection to Oracle
    :param queries: names of queries to execute
    :param start_date: start date
    :param end_date: end date
    :type conn: OracleConnection
    :type queries: list
    :type start_date: datetime.datetime
    :type end_date: datetime.datetime
    """
    if not conn.execute_saved(queries[0], start_date=start_date,
                              end_date=end_date) \
            or not conn.execute_saved(queries[1], start_date=start_date,
                                      end_date=end_date):
        raise StopIteration
    tasks = conn.results(queries[0], 1000, True)
    datasets = conn.results(queries[1], 1000, True)
    return join_results(tasks, squash_records(datasets))


def squash_records(rec):
    """ Squash multiple records with same 'taskid' value into one.

    The squashing is performed via joining values of 'datasetname'
      parameter into a lists of dataset names: one list for every
      'type' value.

    Original structure:
    [
       {"taskid": 1, "type": "output", "datasetname": "First_out"},
       {"taskid": 1, "type": "output", "datasetname": "Second_out"},
       {"taskid": 1, "type": "output", "datasetname": "Third_out"},
       {"taskid": 2, "type": "output", "datasetname": "First_out"},
       {"taskid": 2, "type": "output", "datasetname": "Second_out"},
    ...]

    Result structure:
    [
       {"taskid": 1,
        "output": ["First_out", "Second_out", "Third_out"]
       },
       {"taskid": 2,
        "output": ["First_out","Second_out"]
       },
    ...]

    :param rec: original records
    :type rec: iterable object
    """
    key_fields = ['taskid']
    fold_fields = [('type', 'datasetname')]
    result = {}
    for d in rec:
        for key_field in key_fields:
            if not d.get(key_field):
                # There must not be empty key fields (or how is it "key");
                # but we better explicitly skip it. Just in case.
                continue
            if result.get(key_field) and result[key_field] != d[key_field]:
                # Squashing is over for given set of values of the key fields
                yield result
                result = {}
                break

        # Fold original record
        # {'taskid': 1, 'type': 'output', 'datasetname': DSNAME} ->
        #  -> {'taskid': 1, 'output': DSNAME}
        for fkey, fval in fold_fields:
            if fkey in d:
                new_key = d.pop(fkey)
                new_val = d.pop(fval, None)
                d[new_key] = new_val

        for f in d:
            if f in key_fields:
                result[f] = d[f]
                continue
            if d[f]:
                if result.get(f):
                    if type(result[f]) != list:
                        result[f] = [result[f]]
                    result[f].append(d[f])
                else:
                    result[f] = d[f]

    if result:
        yield result

def join_results(tasks, datasets):
    """ Join results of two queries by 'taskid'.

    :param tasks: result of query 'tasks'
    :param datasets: result of query 'datasets'
    :type tasks: iterable object
    :type datasets: iterable object
    """
    d = defaultdict(dict)
    buffers = (tasks, datasets)
    join_buffer = {}
    req_n = len(buffers)
    for l in buffers:
        for elem in l:
            tid = elem['taskid']
            d[tid].update(elem)
            n = join_buffer.get(tid, 0) + 1
            if n == req_n:
                yield d[tid]
                del join_buffer[tid]
                del d[tid]
            else:
                join_buffer[tid] = n

    # Flush records stuck in the buffer (not joined)
    for tid in d:
        yield d[tid]


def process(conn, offset_storage, final_date_cfg, step_seconds):
    """ Run the source connector process: extract data from ext. source.

    :param conn: open connection to Oracle
    :param offset_storage: offset storage
    :param final_date_cfg: final date from the namin config file
    :param step_seconds: interval for single process iteration
    :type conn: OracleConnection
    :type offset_storage: OffsetStorage
    :type final_date_cfg: datetime.datetime
    """
    if final_date_cfg:
        final_date = final_date_cfg
    else:
        final_date = datetime.now()
    break_loop = False
    offset_date = get_offset(offset_storage)
    while (not break_loop and offset_date < final_date):
        end_date = offset_date + timedelta(seconds=step_seconds)
        if end_date > final_date:
            end_date = final_date
            break_loop = True
        sys.stderr.write("(TRACE) %s: Run queries for interval from %s to %s\n"
                         % (date2str(datetime.now()), date2str(offset_date),
                            date2str(end_date)))
        if mode == SQUASH_POLICY:
            records = squash(conn, ['tasks', 'datasets'], offset_date, end_date)
        elif mode == PLAIN_POLICY:
            records = plain(conn, ['tasks'], offset_date, end_date)
        for r in records:
            r['phys_category'] = get_category(r)
            OUT.write(json.dumps(r) + '\n')
        offset_date = end_date
        commit_offset(offset_storage, offset_date)
        if not final_date_cfg:
            final_date = datetime.now()


def interval_seconds(step):
    """ Convert human-readable interval into seconds.

    If no suffix present, take step as seconds.
    :param step: human-readable time interval (3600, 1h, 1d, 15m....)
    :type step: str|int
    """
    try:
        return int(step)
    except ValueError:
        pass
    if len(step) < 2:
        raise ValueError("Failed to decode interval: %s" % step)
    suffix = {'d': 86400, 'h': 3600, 'm': 60, 's': 1}
    val = step[:-1]
    try:
        mul = suffix[step[-1]]
        return int(val) * mul
    except ValueError:
        raise ValueError("Failed to decode numeric part of the interval: %s"
                         % step)
    except KeyError:
        raise ValueError("Failes to decode index of the interval: %s" % step)


def str2date(str_date):
    """ Convert string (%d-%m-%Y %H:%M:%S) to datetime object. """
    if not str_date:
        return None
    return datetime.strptime(str_date, "%d-%m-%Y %H:%M:%S")


def date2str(date):
    """ Convert datetime object to string (%d-%m-%Y %H:%M:%S). """
    if not date:
        return None
    return datetime.strftime(date, "%d-%m-%Y %H:%M:%S")


def get_category(row):
    """ Categorize task.

    Each task can be associated with a number of Physics Categories.
    1) search category in hashtags list
    2) if not found in hashtags, then search category in phys_short
       field of taskname
    :param row: task metadata
    :type row: dict
    :return: detected categories for the task
    :rtype: list
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


def parsingArguments():
    """ Parse command line arguments.

    :return: parsed arguments
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(description='Process command line'
                                     'arguments.')
    parser.add_argument('--config', help='Configuration file path',
                        type=str, required=True)
    parser.add_argument('--mode', help='Mode of execution: PLAIN | SQUASH',
                        choices=[PLAIN_POLICY, SQUASH_POLICY])
    args = parser.parse_args()
    if not os.access(args.config, os.F_OK):
        sys.stderr.write("argument --config: '%s' file not exists\n"
                         % args.config)
        sys.exit(1)
    if not os.access(args.config, os.R_OK | os.W_OK):
        sys.stderr.write("argument --config: '%s' read/write access failed\n"
                         % args.config)
        sys.exit(1)
    return parser.parse_args()


if __name__ == '__main__':
    main()
