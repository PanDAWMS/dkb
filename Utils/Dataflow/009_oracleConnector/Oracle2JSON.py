#!/bin/env python
import os
import json
import argparse
import ConfigParser
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
    global mode
    mode = args.mode

    # read initial configuration
    config = read_config(args.config)
    if config is None:
        sys.exit(1)

    # Offset storage initialization
    offset_storage = init_offset_storage(config)
    if not offset_storage:
        sys.exit(2)

    conn = OracleConnection(config['dsn'])
    if not conn.establish():
        sys.stderr.write("(ERROR) Failed to connect to Oracle. Exiting.\n")
        sys.exit(3)

    if not conn.save_queries(config['queries']):
        sys.stderr.write("(ERROR) Queries seem to be misconfigured."
                         " Exiting.\n")
        sys.exit(1)

    process(conn, offset_storage, config['final_date'], config['step_seconds'])


def read_config(config_file):
    """ Read configuration file.

    Returns None in case of failure.

    :param config_file: config file name
    :type config_file: string
    :return: configuration parameters
    :rtype: dict|NoneType
    """
    result = {'__path__': os.path.dirname(os.path.abspath(config_file))}
    config = ConfigParser.SafeConfigParser()
    try:
        config.read(config_file)
        # Required parameters (with no defaults)
        result['dsn'] = config.get("oracle", "dsn")
        step = config.get("timestamps", "step")
        result['step_seconds'] = interval_seconds(step)
        queries_cfg = config.items("queries")
        queries = {}
        for (qname, f) in queries_cfg:
            queries[qname] = {'file': config_path(f, result)}
        result['queries'] = queries
    except (IOError, ConfigParser.Error), e:
        sys.stderr.write('Failed to read config file (%s): %s\n'
                         % (config_file, e))
        return None

    # Optional parameters
    result['final_date'] = str2date(config_get(config, "timestamps", "final"))
    result['initial_date'] = str2date(
        config_get(config, "timestamps", "initial",
                   datetime.utcfromtimestamp(0)))
    result['offset_file'] = config_path(config_get(config, "logging",
                                                   "offset_file", ".offset"),
                                        result)

    return result


def config_get(config, section, param, default=None):
    """ Get $param value from $section of $config with $default value.

    If the parameter (or even section) is not presented in the config,
      returns default value instead of throwing an exception (as
      `config.get()` does).

    :param config: config to read from
    :param section: config section name
    :param param: config parameter name
    :param default: default value
    :type config: ConfigParser object
    :type section: string
    :type param: string
    :param default: object (any type)
    :return: param value from config or default value
    :rtype: object
    """
    if not isinstance(config, ConfigParser.ConfigParser):
        raise TypeError("config_get() expects first parameter to be"
                        " an instance of 'ConfigParser' (get '%s')."
                        % config.__class__.__name__)
    if type(section) != str and type(section) != unicode:
        raise TypeError("config_get() expects second parameter to be"
                        " string (get '%s')."
                        % section.__class__.__name__)
    if type(param) != str and type(param) != unicode:
        raise TypeError("config_get() expects third parameter to be"
                        " string (get '%s')."
                        % section.__class__.__name__)

    try:
        result = config.get(section, param)
    except ConfigParser.Error:
        if default is not None:
            sys.stderr.write("(INFO) Config: parameter '%s' (section '%s')"
                             " is not configured. Using default value: %s\n"
                             % (param, section, default))
        result = default

    return result


def config_path(rel_path, config):
    """ Turn relative (to config dir) path to absolute.

    :param rel_path: relative (or absolute) path
    :param config: stage configuration
    :type rel_path: str
    :type config: dict
    :return: absolute path
    :rtype: str
    """
    if os.path.isabs(rel_path):
        abs_path = rel_path
    else:
        config_dir = config['__path__']
        abs_path = os.path.join(config_dir, rel_path)
    return abs_path


def init_offset_storage(config):
    """ Get configured (or default) offset file.

    If file does not exist, create file.
    If the directory for that file does not exist, raise exception.

    If the configured initial date is greater than a stored offset
      (or if no offset found) it becomes the initial offset value.

    :param config: stage configuration
    :type config: dict
    :return: offset storage
    :rtype: FileOffsetStorage
    """
    offset_storage = None

    if not isinstance(config, dict):
        raise TypeError("get_offset_file: dict object is expected;"
                        " got %s" % type(config))

    offset_file = config['offset_file']
    initial_date = config['initial_date']

    try:
        offset_storage = FileOffsetStorage(offset_file)
        current_offset = get_offset(offset_storage)
        if not current_offset:
            sys.stderr.write('(INFO) No stored offset found.'
                             ' Using initial date.\n')
            current_offset = initial_date
        elif current_offset < initial_date:
            sys.stderr.write('(INFO) Stored offset is less then configured'
                             ' initial date. Using initial date instead.\n')
            current_offset = initial_date
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
            records = squash(conn, ['tasks', 'datasets'], offset_date,
                             end_date)
        elif mode == PLAIN_POLICY:
            records = plain(conn, ['tasks'], offset_date, end_date)
        for r in records:
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
    """ Convert string (%d-%m-%Y %H:%M:%S) to datetime object.

    If passed parameter is of type datetime.datetime, it is returned
    as is.

    :param str_date: string to convert
    :type str_date: string | datetime.datetime
    :return: converted value
    :rtype: datetime.datetime
    """
    if not str_date:
        return None
    if isinstance(str_date, datetime):
        return str_date
    return datetime.strptime(str_date, "%d-%m-%Y %H:%M:%S")


def date2str(date):
    """ Convert datetime object to string (%d-%m-%Y %H:%M:%S). """
    if not date:
        return None
    return datetime.strftime(date, "%d-%m-%Y %H:%M:%S")


def parsingArguments():
    """ Parse command line arguments.

    :return: parsed arguments
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(description='DKB Dataflow Oracle'
                                     ' connector stage.')
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
