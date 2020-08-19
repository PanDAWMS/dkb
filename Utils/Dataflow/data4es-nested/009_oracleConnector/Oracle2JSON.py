#!/bin/env python
import os
import json
import argparse
import ConfigParser
import sys
from datetime import datetime, timedelta
import time
import pytz
from collections import defaultdict

from OffsetStorage import FileOffsetStorage
from dbConnection import OracleConnection


base_dir = os.path.dirname(__file__)

# Queries location
QUERY_DIR = os.path.join(base_dir, 'query')

# Policies
PLAIN_POLICY = 'PLAIN'
SQUASH_POLICY = 'SQUASH'

# Global variables
# ---
# Output stream
OUT = os.fdopen(sys.stdout.fileno(), 'w', 0)

# Timezone of the data timestamps in the source DB
# (used to adjust 'now' to the proper value)
OFFSET_TZ = None
# Delay
# (used to adjust 'now' to avoid pulling of "unstable" data)
OFFSET_DELAY = 0
# ---


def main():
    """ Main program cycle.

    USAGE:
       ./Oracle2JSON.py --config <config file>

    TODO:
       Obtain the min(timestamp) from t_production_task
         in case of no `initial_date` specified.
       Query: SELECT min(timestamp) from t_production_task;
    """
    args = parsingArguments()

    # read initial configuration
    config = read_config(args.config)
    log_config(config)
    if config is None:
        sys.exit(1)

    # Offset storage initialization
    offset_storage = init_offset_storage(config)
    if not offset_storage:
        sys.exit(2)

    # Offset timezone initialization
    init_offset_tz(config)
    # Offset delay initialization
    init_offset_delay(config)

    conn = OracleConnection(config['__dsn'])
    if not conn.establish():
        sys.stderr.write("(ERROR) Failed to connect to Oracle. Exiting.\n")
        sys.exit(3)

    if not conn.save_queries(config['queries']):
        sys.stderr.write("(ERROR) Queries seem to be misconfigured."
                         " Exiting.\n")
        sys.exit(1)

    process(conn, offset_storage, config)


def log_config(config):
    """ Log stage configuration.

    :param config: stage configuration
    :type config: dict, NoneType
    """
    if config is None:
        sys.stderr.write("(ERROR) Stage is not configured.\n")
    if not isinstance(config, dict):
        sys.stderr.write("(ERROR) Stage is misconfigured.\n")

    sys.stderr.write("(INFO) Stage 009 configuration (%s):\n"
                     % config['__file__'])

    key_len = len(max(config.keys(), key=len))
    pattern = "(INFO)  %%-%ds : '%%s'\n" % key_len
    sys.stderr.write("(INFO) ---\n")
    for p in config:
        if p.startswith('__'):
            continue
        sys.stderr.write(pattern % (p, config[p]))
    sys.stderr.write("(INFO) ---\n")


def read_config(config_file):
    """ Read configuration file.

    Returns None in case of failure.

    :param config_file: config file name
    :type config_file: string
    :return: configuration parameters
    :rtype: dict|NoneType
    """
    result = {'__path__': os.path.dirname(os.path.abspath(config_file))}
    result['__file__'] = os.path.join(result['__path__'], config_file)
    config = ConfigParser.SafeConfigParser()
    try:
        config.read(config_file)
        # Required parameters (with no defaults)
        result['__dsn'] = config.get('oracle', 'dsn')
        step = config.get('timestamps', 'step')
        result['step_seconds'] = interval_seconds(step)
        if result['step_seconds'] == 0:
            raise ConfigParser.Error("'timestamps.step': unacceptable value")
        queries_in_use = config.get('queries', 'use').split(',')
        qtype = config.get('queries', 'type')
        queries = {}
        for qname in queries_in_use:
            queries[qname] = {'file': query_path(qname, qtype)}
        if 'queries.params' in config.sections():
            qparams = {}
            for (param, val) in config.items('queries.params'):
                qparams[param] = val
            for q in queries:
                # As for now we have a common set of parameters for all
                # queries, we reuse same `dict` instead of copying it.
                # But if we need different parameters for different queries,
                # it must be changed to `dict(qparams)` or smth like this.
                queries[q]['params'] = qparams
        result['queries'] = queries
    except (IOError, ConfigParser.Error), e:
        sys.stderr.write('Failed to read config file (%s): %s\n'
                         % (config_file, e))
        return None

    # Optional parameters
    result['timestamp_tz'] = config_get(config, 'timestamps', 'tz',
                                        time.tzname[0])
    delay = config_get(config, 'timestamps', 'delay', '0')
    result['delay'] = interval_seconds(delay)
    if result['delay'] < 0:
        # Delay is used when right border of the data taking interval is
        # 'now'.
        # It does not make much sense to ask for data from the future.
        sys.stderr.write('(WARN) Delay is less than 0, setting it to 0.\n')
        result['delay'] = 0
    if result['step_seconds'] > 0:
        default_initial = datetime.utcfromtimestamp(0)
        default_final = None
    else:
        default_initial = offset_now(result['timestamp_tz'], result['delay'])
        default_final = datetime.utcfromtimestamp(0)

    result['final_date'] = str2date(config_get(config, 'timestamps', 'final',
                                               default_final))
    result['initial_date'] = str2date(config_get(config, 'timestamps',
                                                 'initial', default_initial))
    result['offset_file'] = config_path(config_get(config, 'logging',
                                                   'offset_file', '.offset'),
                                        result)
    result['mode'] = config_get(config, 'process', 'mode', 'SQUASH')

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
    if type(section) != str and \
            (sys.version_info.major > 2 or type(section) != unicode):
        raise TypeError("config_get() expects second parameter to be"
                        " string (get '%s')."
                        % section.__class__.__name__)
    if type(param) != str and \
            (sys.version_info.major > 2 or type(param) != unicode):
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

    if result == '' and default is not None:
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


def query_path(qname, qtype=''):
    """ Construct path to file with SQL query.

    :param qname: query name
    :type qname: str
    :param qtype: query type
    :type qtype: str

    :return: path to the query file
    :rtype: str
    """
    result = QUERY_DIR
    if qtype:
        result = os.path.join(result, qtype)
    return os.path.join(result, qname)


def init_offset_tz(config):
    """ Initialize OFFSET_TZ. """
    global OFFSET_TZ
    OFFSET_TZ = pytz.timezone(config['timestamp_tz'])


def init_offset_delay(config):
    """ Initialize OFFSET_DELAY. """
    global OFFSET_DELAY
    OFFSET_DELAY = config['delay']


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
    reverse = config['step_seconds'] < 0

    try:
        offset_storage = FileOffsetStorage(offset_file)
        current_offset = get_offset(offset_storage)
        if not current_offset:
            sys.stderr.write("(INFO) No stored offset found."
                             " Using initial date.\n")
            current_offset = initial_date
        elif not reverse and current_offset < initial_date \
                or reverse and current_offset > initial_date:
            sys.stderr.write("(INFO) Stored offset is %s then configured"
                             " initial date (%s data loading). Using initial"
                             " date instead.\n"
                             % ('greater' if reverse else 'less',
                                'reverse' if reverse else 'normal'))
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


def offset_now(tz=None, delay=None):
    """ Get offset value corresponding to the current moment of time.

    :param tz: time zone name. If not specified, default (globally set)
               time zone is used.
    :type tz: str, NoneType
    :param delay: delay in seconds. If not specified, default (globally set)
                  value is used.
    :type delay: int, NoneType

    :return: offset value (naive datetime, adjusted to OFFSET_TZ with
             OFFSET_DELAY)
    :rtype: datetime.datetime
    """
    TZ = OFFSET_TZ
    if tz:
        TZ = pytz.timezone(tz)
    dl = OFFSET_DELAY
    if delay is not None:
        dl = delay
    return datetime.now(TZ).replace(tzinfo=None) - timedelta(seconds=dl)


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
    for buf in buffers:
        for elem in buf:
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


def process(conn, offset_storage, config):
    """ Run the source connector process: extract data from ext. source.

    :param conn: open connection to Oracle
    :param offset_storage: offset storage
    :param config: stage configuration
    :type conn: OracleConnection
    :type offset_storage: OffsetStorage
    :type config: dict
    """
    mode = config['mode']
    reverse = config['step_seconds'] < 0
    final_date = config['final_date']
    step_seconds = config['step_seconds']
    offset_date = get_offset(offset_storage)
    if not reverse and not final_date:
        # In case of normal data load 'final_date' may be None:
        # it means we need to adjust it to current timestamp
        # before every check
        final_date = offset_now()
    full_interval = {'l': min(final_date, offset_date),
                     'r': max(final_date, offset_date)}
    break_loop = False
    while full_interval['l'] <= offset_date <= full_interval['r']:
        new_offset = offset_date + timedelta(seconds=step_seconds)
        if not full_interval['l'] < new_offset < full_interval['r']:
            # Get outside the configured full interval:
            # need to adjust current interval
            new_offset = full_interval['l'] if reverse else full_interval['r']
            # and break the loop before next iteration
            break_loop = True
        if new_offset == offset_date:
            break
        start_date = min(offset_date, new_offset)
        end_date = max(offset_date, new_offset)
        sys.stderr.write("(TRACE) %s: Run queries for interval from %s to %s"
                         " (%s)\n" % (date2str(datetime.now()),
                                      date2str(start_date), date2str(end_date),
                                      OFFSET_TZ.zone))
        if mode == SQUASH_POLICY:
            records = squash(conn, ['tasks', 'datasets'], start_date,
                             end_date)
        elif mode == PLAIN_POLICY:
            records = plain(conn, ['tasks'], start_date, end_date)
        for r in records:
            OUT.write(json.dumps(r) + '\n')
        offset_date = new_offset
        commit_offset(offset_storage, offset_date)
        if not config['final_date']:
            full_interval['r'] = offset_now()
        if break_loop:
            break


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
        raise ValueError("Failed to decode index of the interval: %s" % step)


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
    parser = argparse.ArgumentParser(description="DKB Dataflow Oracle"
                                     " connector stage.")
    parser.add_argument('--config', help="Configuration file path",
                        type=str, required=True)
    args = parser.parse_args()
    if not os.access(args.config, os.F_OK):
        sys.stderr.write("argument --config: '%s' file not exists\n"
                         % args.config)
        sys.exit(1)
    if not os.access(args.config, os.R_OK):
        sys.stderr.write("argument --config: '%s' read access failed\n"
                         % args.config)
        sys.exit(1)
    return parser.parse_args()


if __name__ == '__main__':
    main()
