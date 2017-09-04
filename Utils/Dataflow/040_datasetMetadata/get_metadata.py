#!/usr/bin/env python

# Retrieves metadata from Impala for datasets, mentioned in the JSON document

# ./get_metadata.py -h
# usage: get_metadata.py [-h] [-t [TYPE]] [-H [HOST]] [-P [PORT]] [-d [DB]]
#                       [-O [{f,i}]]
#                       JSON-FILE [JSON-FILE ...]
#
# Reads JSON-file with information about papers and look for metadata for
# mentioned datasets in Impala.
#
# positional arguments:
#  JSON-FILE             Source JSON-file.
#
# optional arguments:
#  -h, --help            show this help message and exit
#  -t [TYPE], --source-type [TYPE]
#                        Source document (from which the datasets are taken)
#                        naming type. Possible values: GID - GlanceID (default)
#  -H [HOST], --host [HOST]
#                        Impala host name.
#  -P [PORT], --port [PORT]
#                        Impala port number.
#  -d [DB], --database [DB]
#                        Impala database name.
#  -m [MODE], --mode [MODE]
#                        Processing mode. Possible values: f (file), m (map-reduce)
#  --hdfs                Take input files from HDFS.
#  -O [{f,i}], --output [{f,i}]
#                        Where to store results:
#                         f -- files
#                         i -- Impala tables
#                         s -- stdout
#                         h -- HDFS (works only with --hdfs)

import argparse

import json
import sys
from sys import stderr
import os
import os.path
import subprocess

from impala.dbapi import connect
from thrift.transport.TTransport import TTransportException

SELECT = """ifnull(PS1.name, PS2.name) as `name`,
       ifnull(PS1.task_id, PS2.taskid) as `tid`,
       ifnull(PS1.task_pid, PS2.parent_tid) as `chain_tid`,
       ifnull(PS1.phys_group, PS2.phys_group) as `phys_group`,
       ifnull(PS1.events, PS2.events) as `events`,
       if(PS1.`files` is not NULL and PS1.`files`=0, PS2.`files`, PS1.`files`) as `files`,
       ifnull(PS1.status, PS2.status) as `status`,
       if(PS2.`timestamp` is not NULL and PS1.`timestamp` < PS2.`timestamp`, PS1.`timestamp`, PS2.`timestamp`) as `timestamp`,
       PS2.pr_id as `pr_id`,
       PS2.campaign as `campaign`,
       PS2.ddm_erase_timestamp as `ddm_erase_timestamp`,
       PS1.vuid as `vuid`,
       PS1.grid_exec as `grid_exec`,
       PS1.se as `se`,
       PS1.file_size_mb as `file_size_mb`
  FROM
  T_PRODUCTIONDATASETS_EXEC as PS1
  FULL OUTER JOIN
  T_PRODUCTION_DATASET as PS2
  ON PS2.name = PS1.name"""

dataType = {'montecarlo': 'MC', 'realdata': 'RealData'}

IGNORE_PROJECTS = ('evind', 'valid', 'user', 'group')

ARGS = ''

def main(argv):
  global ARGS
  parser = argparse.ArgumentParser(description=u'Reads JSON-file with information about papers and look for metadata for mentioned datasets in Impala.')
  parser.add_argument('input', metavar=u'JSON-FILE', type=argparse.FileType('r'), nargs='*',
                       help=u'Source JSON-file.')
  parser.add_argument('-t', '--source-type', action='store', type=str, nargs='?',
                       help='''Source document (from which the datasets are taken) naming type.
  Possible values:
    GID - GlanceID (default)''',
                       default='GID',
                       const='GID',
                       metavar='TYPE',
                       choices=['GID'],
                       dest='type')
  
  parser.add_argument('--hdfs', action='store', type=bool, nargs='?',
                        help=u'Source files are stored in HDFS; if no JSON-FILE specified, filenames will come to STDIN',
                        default=False,
                        const=True,
                        metavar='HDFS',
                        dest='hdfs'
                        )
  parser.add_argument('-H', '--host', action='store', type=str, nargs='?',
                       help=u'Impala host name.',
                       default='nosql-three.zoo.computing.kiae.ru',
                       const='nosql-three.zoo.computing.kiae.ru',
                       metavar='HOST',
                       dest='host'
                       )
  parser.add_argument('-P', '--port', action='store', type=str, nargs='?',
                       help=u'Impala port number.',
                       default='21050',
                       const='21050',
                       metavar='PORT',
                       dest='port'
                       )
  parser.add_argument('-d', '--database', action='store', type=str, nargs='?',
                       help=u'Impala database name.',
                       default='dkb',
                       const='dkb',
                       metavar='DB',
                       dest='db'
                       )
  parser.add_argument('-m', '--mode', action='store', type=str, nargs='?',
                       help=u'Processing mode: (f)ile or (m)ap-reduce.',
                       default='f',
                       const='m',
                       metavar='MODE',
                       choices=['f', 'm'],
                       dest='mode'
                       )
  parser.add_argument('-O', '--output', action='store', type=str, nargs='?',
                       help=u'Where to output results: (f)iles, (i)mpala tables, (s)tdout, (h)dfs (with --hdfs option only)',
                       default='f',
                       const='f',
                       choices=['f', 'i', 's', 'h'],
                       dest='out'
                       )
  
  ARGS = parser.parse_args(argv)

  try:
    impala = connect(host=ARGS.host, port=ARGS.port, database=ARGS.db, auth_mechanism="GSSAPI")
    DKB = impala.cursor()
  except TTransportException, e:
    stderr.write("Can not connect to Impala. Error message:")
    stderr.write(e.message + "\n")
    exit(1)

  if ARGS.hdfs:
    ARGS.input = hdfsFiles(ARGS.input, ARGS.out == 'h')
  elif ARGS.mode == 'm':
    ARGS.input = hdfsFiles(ARGS.input, False)
    ARGS.out = 's'

  headers = True

  for infile in ARGS.input:
    if os.stat(infile.name).st_size == 0: continue
    stderr.write('FILE: {0}\n'.format(infile.name))
    try:
      DocData = json.load(infile)
    except ValueError, e:
      stderr.write("(ERROR) Failed to read JSON from %s: %s\n" % (infile.name, e.message))
      continue
    if ARGS.out in ('f', 'h'):
      outname = infile.name[:infile.name.rfind('.')] + '.csv'
    elif ARGS.out == 's':
      outname = sys.stdout
    else:
      outname = None
    GlanceID = os.path.basename(infile.name)
    GlanceID = GlanceID[:GlanceID.find('.')]
    loadMetadata(DocData, outname, DKB, {'glanceid': GlanceID, 'datatype': None}, headers)
    # As we are writing all output to one stream,
    # we don't need to repeat headers
    if ARGS.out == 's':
      headers = False

def hdfsFiles(filenames, upload=False):
  if not filenames: filenames = sys.stdin
  for f in filenames:
    f = f.strip()
    if not f: continue
    cmd = "hadoop fs -get %s " % f
    os.system(cmd)
    name = f.split('/')[-1]
    with open(name, 'r') as infile:
      yield infile
    os.remove(name)
    if upload:
      hdfspath = f[:f.rfind('/')]
      csvname = name.replace('.json', '.csv')
      if not os.path.isfile(csvname): continue
      cmd = "hadoop fs -put -f %s %s" % (csvname, hdfspath)
      os.system(cmd)
      os.remove(csvname)


def loadMetadata(data, outfile, db, extra={}, headers=True):
  global ARGS
  if not ( data.get('datasets') or ( data.get('datasetIDs') and data.get('campaigns') )):
    stderr.write("No dataset names or IDs found.\n")
    return 0

  if data.get('datasets'):
    loadByNames(data['datasets'], outfile, db, extra, headers)
  if data.get('datasetIDs') and data.get('campaigns'):
    projects = campaign2project(data['campaigns'], db)
    dsids = []
    for tab in data['datasetIDs']:
      dsids += data['datasetIDs'][tab].split()
    loadByDSIDs(projects, dsids, outfile, db, extra, headers)

def loadByNames(datasets, outfile, db, extra={}, headers=True):
  global ARGS
  for t in dataType:
    if not datasets.get(t, None): continue
    extra['datatype'] = t
    s = 'SELECT distinct '
    s += extra_string(extra)
    s += SELECT
    w = 'WHERE isnull(PS1.name,PS2.name) LIKE "' + '%" OR isnull(PS1.name,PS2.name) LIKE "'.join(datasets[t]) + '%"' if datasets[t] else ''
    query = '{select} {where}'.format(select=s, where=w)
    loadQuery(query, outfile, ARGS, db, headers)

def loadByDSIDs(projects, datasets, outfile, db, extra={}, headers=True):
  global ARGS

  s = 'SELECT distinct '
  s += extra_string(extra)
  s += SELECT

  dsnames = []
  for p in projects:
    for i in datasets:
      dsnames += ['^{project}\.0*?{DSID}\..*$'.format(project=p, DSID=i)]

  w = 'WHERE isnull(PS1.name,PS2.name) RLIKE "' + '" OR isnull(PS1.name,PS2.name) RLIKE "'.join(dsnames) + '"' if dsnames else ''
  query = '{select} {where}'.format(select=s, where=w)
  loadQuery(query, outfile, ARGS, db)

def loadQuery(query, outfile, ARGS, db, headers=True):
  if outfile:
    loadQuery2File(query, outfile, ARGS, headers)
  else:
    loadQuery2DB(query, db)

def loadQuery2File(query, outfile, ARGS, headers=True):
  with open('query.sql', 'w') as qfile:
    qfile.write(query)
    qfile.close()
    m = {'host': ARGS.host, 'db': ARGS.db}
    if outfile == sys.stdout: m['output'] = ''
    else: m['output'] = '-o ' + outfile
    m['headers'] = "--print_header" if headers else ""
    run = 'PYTHON_EGG_CACHE=/tmp/.python-eggs impala-shell -k -i {host} -d {db} -B -f query.sql --output_delimiter="," {headers} {output}'.format(**m)
    try:
      subprocess.check_call(run, shell=True)
    except subprocess.CalledProcessError, e:
      stderr.write("(ERROR) Failed to execute Impala request (return code: %d).\nCommand: %s\n" % (e.returncode, e.cmd))

def loadQuery2DB(query, db):
    q = 'INSERT INTO dkb_temp.datasets {query}'.format(query=query)
    db.execute(q)

def campaign2project(campaigns, db):
# Get projects from Impala for given campaigns
  if not campaigns:
    return []

  not_like = 'AND lower(`project`) NOT LIKE "%' + '%" AND lower(`project`) NOT LIKE "%'.join(IGNORE_PROJECTS) + '%"'  if IGNORE_PROJECTS else ''
  campaign_in = "','".join(campaigns).lower()
  query = '''SELECT DISTINCT project FROM T_PRODUCTION_TASK
where (lower(`campaign`) in ('{campaign_in}')
or lower(`subcampaign`) in ('{campaign_in}'))
{not_like}'''.format(campaign_in=campaign_in, not_like=not_like)
  db.execute(query)
  r = []
  for row in db.fetchall():
    r += [row[0]]
  return r

def extra_string(extra={}):
  keys = extra.keys()
  keys.sort()
  s = ''
  for e in keys:
    val = extra[e]
    if not checkExtra(e, val):
      stderr.write("(WARN) Replace unacceptable value with NULL.\n")
      val = None

    try: val = int(val)
    except (ValueError, TypeError): pass

    if type(val) == str: val = '"{val}"'.format(val=val)
    elif type(val) == int: val = '{val}'.format(val=val)
    elif val == None: val = 'NULL'

    s += '''{val} as `{name}`,
'''.format(val=val, name=e)
  return s

def checkExtra(key, val):
  checkParameters = {
      'glanceid': {
          'type': int
          }
  }
  if key in checkParameters:
    p = checkParameters[key]
    try: p['type'](val)
    except ValueError:
      stderr.write("(WARN) Unacceptable value for key '%s': '%s' (expected type: %s).\n" % (key, val, p['type']))
      return False
  return True

if __name__ == '__main__':
  main(sys.argv[1:])
