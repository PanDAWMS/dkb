#!/usr/bin/env python
#
# By Eygene A. Ryabinkin, 2017.
# Public domain, but try to respect the authorship.

import base64
import cStringIO
from datetime import datetime, timedelta
import math
import os
import sys


def perror(e_code, message):
    sys.stderr.write(message + "\n")
    sys.exit(e_code)

def pout(e_code, message):
    sys.stdout.write(message + "\n")
    sys.exit(e_code)


try:
    import cx_Oracle as ora
except ImportError as e:
    perror(1, "Failed to import cx_Oracle: %s" % (e))

try:
    import argparse as argp
except ImportError as e:
    perror(1, "Failed to import argparse (Python < 2.7?): %s" % (e))

try:
    import pycurl
except ImportError as e:
    perror(1, "Failed to import py-cURL: %s" % (e))


def tdiff_to_sec(td):
    """ Converts timediff to the number of seconds (Python <= 2.6) """
    return ((td.microseconds/float(10**3) + \
      (td.seconds + td.days * 24 * 3600) * float(10**3))/10**3)


def arr_to_s(arr, delim = ", "):
    """ Array stringifier """

    return delim.join(i for i in arr)


def do_es_queries(es_params, query, iterations):
    db = 'es'

    handle = pycurl.Curl()
    headers = {
      'Authorization': 'Basic ' + base64.b64encode("%(user)s:%(password)s" % (es_params))
    }

    handle.setopt(pycurl.HTTPHEADER, ["%s: %s" % item for item in headers.items()])
    handle.setopt(pycurl.URL, es_params['url'])
    handle.setopt(pycurl.POST, 1)
    stmt = queries[db][query]['json']
    handle.setopt(pycurl.POSTFIELDS, stmt)

    timings = []
    for i in xrange(0, iterations):
        start = datetime.now()

        response = cStringIO.StringIO()
	handle.setopt(pycurl.WRITEFUNCTION, response.write)
        handle.perform()
	result = handle.getinfo(pycurl.HTTP_CODE)
	assert(result is 200)

	retval = response.getvalue()

        td = datetime.now() - start
        timings.append(tdiff_to_sec(td))

    handle.close()

    return timings



def do_oracle_queries(db_params, query, iterations):
    db = 'ora'

    handle = ora.connect("""%(user)s/%(password)s@(
  DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=%(host)s)(PORT=%(port)s))(LOAD_BALANCE=on)(ENABLE=BROKEN)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=adcr.cern.ch)(INSTANCE_NAME=ADCR1))
)
""" % (db_params))

    session = handle.cursor()
    stmt = queries[db][query]['sql']

    timings = []
    for i in xrange(0, iterations):
        start = datetime.now()

        session.execute(stmt)

        numrows = 0
        while True:
            rows = session.fetchmany(session.arraysize)
            if not len(rows):
                break
            numrows += len(rows)

        td = datetime.now() - start
        timings.append(tdiff_to_sec(td))

    session.close()
    handle.close()

    return timings


def gaussian_fit(data, nsigma):
    """
    Performs gaussian fitting of the provided data

    Arguments:
    - data: array of input values
    - nsigma: number of dispersion intervals to consider

    Return values: quadriple of
    - mean;
    - standard deviation;
    - input values that fit into nsigma * std_dev
    - input values that are outside nsigma * std_dev

    """

    n = len(data)
    assert(n > 0)

    mean = 0.0
    for x in data: mean += float(x)
    mean /= n

    std_dev = 0.0
    for x in data: std_dev += (float(x - mean))**2
    std_dev = math.sqrt(std_dev / float(1 if n == 1 else n - 1))

    return (mean, std_dev,
      filter(lambda x: math.fabs(x - mean) < nsigma * std_dev, data),
      filter(lambda x: math.fabs(x - mean) >= nsigma * std_dev, data))



## Hashtag => category mappers

"""
TODO:

Original query contained '+' in a single statement,
{{{
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(charmonium|jpsi|bs|bd|bminus|bplus|charm|bottom|bottomonium|b0)+(*)')
        THEN 'BPhysics'
}}}
Understand, if its omission affects something.  Should not,
but it is 04:55 at the morning, so I am not 100% sure.
"""
 
hashtag2category = (
  ('BPhysics', [
     'charmonium', 'jpsi', 'bs', 'bd', 'bminus', 'bplus', 'charm'
   , 'bottom', 'bottomonium', 'b0'
  ])
 ,('BTag', [
    'btagging'
  ])
 ,('Diboson', [
    'diboson', 'zz', 'ww', 'wz', 'wwbb', 'wwll'
  ])
 ,('DrellYan', [
    'drellyan', 'dy'
  ])
 ,('Exotic', [
    'exotic', 'monojet', 'blackhole', 'technicolor', 'randallsundrum'
  , 'wprime', 'zprime', 'magneticmonopole', 'extradimensions', 'warpeded'
  , 'contactinteraction', 'seesaw'
  ])
 ,('GammaJets', [
    'photon', 'diphoton'
  ])
 ,('Higgs', [
    'vbf', 'higgs', 'mh125', 'zhiggs', 'whiggs', 'bsmhiggs', 'chargedhiggs'
  , 'bsmhiggs', 'smhiggs'
  ])
 ,('Minbias', [
    'minBias', 'minbias'
  ])
 ,('Multijet', [
    'dijet', 'multijet', 'qcd'
  ])
 ,('Performance', [
    'performance'
  ])
 ,('SingleParticle', [
    'singleparticle'
  ])
 ,('SingleTop', [
    'singletop'
  ])
 ,('SUSY', [
    'bino', 'susy', 'pmssm', 'leptosusy', 'rpv', 'mssm'
  ])
 ,('Triboson', [
    'triboson', 'triplegaugecoupling', 'zzw', 'www'
  ])
 ,('TTbar', [
    'ttbar'
  ])
 ,('TTbarX', [
    'ttw', 'ttz', 'ttv', 'ttvv', '4top'
  ])
 ,('Upgrade', [
    'upgrad'
  ])
 ,('Wjets', [
    'w'
  ])
 ,('Zjets', [
    'z'
  ])
)



# XXX: add DB quoting!


def likes_REGEXP_LIKE(tags, var):
    """
    Constructs switch-type REGEXP_LIKE with '(match1|match2|...)'

    Arguments:
    - list of tags to switch on
    - var: name of the variable carrying hashtag list in DB

    """
    return ("REGEXP_LIKE(%s, " % (var) + \
      "'" + \
      (tags[0] if len(tags) == 1 else "(%s)" % ("|".join(tags))) + \
      "')")


def likes_REGEXP_LIKE_i(tags, var):
    """
    Constructs switch-type REGEXP_LIKE -- '(match1|match2|...)'

    Case-insensitiveness is reached via regexp modifier.

    Arguments:
    - list of tags to switch on
    - var: name of the variable carrying hashtag list in DB

    """
    return ("REGEXP_LIKE(%s, " % (var) + \
      "'" + \
      (tags[0] if len(tags) == 1 else "(%s)" % ("|".join(tags))) + \
      "', 'i')")


def likes_PURE_LIKE(tags, var):
    """
    Implements switch-type REGEXP_LIKE with bare LIKE

    Arguments:
    - list of tags to switch on
    - var: name of the variable carrying hashtag list in DB

    REGEXP_LIKE vs LIKE: see
      http://www.orafaq.com/usenet/comp.databases.oracle.misc/2007/04/18/0291.htm
    for a discussion on why the latter is a whole lot faster.

    """
    return ("(" + (" OR ".join([
      "%s LIKE '%%%s%%'" % (var, t) for t in tags
    ])) + ")")


def ht_mapper_WHEN_THEN(ht2cat, indent, htlist_name, _lambda):
    """
    Produces REGEXP_LIKE query part for SQL

    Arguments:
     - ht2cat: ordered dictionary for hashtag => category mapping
     - indent: prefix for each generated line
     - htlist_name: name of DB variable/expression with list of hashtags
     - _lambda: actual workhorse function that produces the body of WHEN

    _lambda's signature is (tags, htlist_name).

    """
    # XXX: add DB quoting!
    when_then = \
      lambda (cat, tags): (\
        indent + \
        "WHEN " + _lambda(tags, htlist_name) + "\n" + \
	indent + \
	"THEN '%s'" % cat)
    return "\n".join(map(when_then, ht2cat))


##
## Configuration stuff
##

config_keys = {
  'ora_params': ('user', 'password', 'host', 'port')
, 'es_params': ('url', 'user', 'password')
}


def read_config(filename, config_keys):
    """
    Processes configuration file

    Arguments:
    - filename: name of the file to process
    - config_keys: tuple-valued hash with mandatory topic => (keys)
      configuration.

    See config_help() for the description of configuration file format.

    Returns hash keyed by topic, each value is the hash itself;
    it is keyed by "key" and valued -- by "value" (surprise!).

    Items not listed in config_keys are currently silently ignored.

    """

    retval = {}
    for k in config_keys.keys():
        retval[k] = {}

    lineno = 0
    with open(filename, 'r') as f:
        for line in f:
	   lineno += 1
	   l = line.strip()
	   if not len(l) or l[0] == '#':
	       continue
	   parts = l.split(':', 1)
	   tk = parts[0].split('.', 1)
	   if len(parts) != 2 or len(tk) != 2:
	       perror(1, "%s, %d: " % (filename, lineno) + \
	         "line not in 'topic.key: value' form")
	   topic = tk[0]
	   key = tk[1].rstrip()
	   value = parts[1].lstrip()
	   if topic not in config_keys:
	       continue
	   if key not in config_keys[topic]:
	       continue
           retval[topic][key] = value

    missing_items = []
    for k in config_keys.keys():
        mandatory = set(config_keys[k])
	existing = set(retval[k].keys())
        if not mandatory in existing:
	    missing_items.extend(["%s.%s" % (k, i) \
	      for i in (mandatory - existing)])

    if len(missing_items):
        perror(1, "Configuration file %s, " % (filename) + \
	  "missing the following items: " + arr_to_s(missing_items))

    return retval


def config_help(config_keys):
    sys.stdout.write("""Configuration file help
=======================

File syntax:
 - leading and trailing whitespace is stripped.
 - each line has format 'topic.key: value',
   whitespace around ':' is stripped too;
 - empty, whitespace-only lines and everything
   beginning with '#' is ignored.

Mandatory items:
""")
    for key, value in config_keys.items():
        print(" - topic '%s': %s" % (key, arr_to_s(value)))


#
# Queries
#

queries = {
  'ora': {}
, 'es': {}
}

# TODO: verify that we have same sets of keys for queries
# TODO: and kinds.
kinds = {
  'ora': 'sql'
, 'es': 'json'
}


queries['ora']['mc16-tasks-ORIG'] = {
  'help': 'MC16 campaign, full-blown regexp and stuff from M. Grigorieva'
, 'sql': """
WITH mc16a_tasks AS (
    SELECT
      task.taskid,
      task.step_id
    FROM
      ATLAS_DEFT.t_production_task task
    WHERE
      LOWER(task.campaign) = 'mc16'
      AND LOWER(task.subcampaign) = 'mc16a'
),
task_hashtags AS (
    SELECT
      tasks.taskid,
      s_t.step_name,
      LISTAGG(hashtag.hashtag, ', ')
    WITHIN GROUP (ORDER BY tasks.taskid) AS hashtag_list
    FROM
      mc16a_tasks tasks,
      ATLAS_DEFT.t_ht_to_task ht_t,
      ATLAS_DEFT.t_hashtag hashtag,
      ATLAS_DEFT.t_production_step s,
      ATLAS_DEFT.t_step_template s_t
    WHERE
      tasks.taskid = ht_t.taskid
      AND hashtag.ht_id = ht_t.ht_id
      AND tasks.step_id = s.step_id
      AND s.step_t_id = s_t.step_t_id
    GROUP BY
      tasks.taskid,
      s_t.step_name
),
phys_categories AS (
    SELECT
      taskid,
      step_name,
      hashtag_list,
      CASE
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(charmonium|jpsi|bs|bd|bminus|bplus|charm|bottom|bottomonium|b0)+(*)')
        THEN 'BPhysics'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)btagging(*)')
        THEN 'BTag'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(diboson|zz|ww|wz|wwbb|wwll)(*)')
        THEN 'Diboson'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(drellyan|dy)(*)')
        THEN 'DrellYan'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(exotic|monojet|blackhole|technicolor|randallsundrum|wprime|zprime|magneticmonopole|extradimensions|warpeded|contactinteraction|seesaw)(*)')
        THEN 'Exotic'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(photon|diphoton)(*)')
        THEN 'GammaJets'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(vbf|higgs|mh125|zhiggs|whiggs|bsmhiggs|chargedhiggs|bsmhiggs|smhiggs)(*)')
        THEN 'Higgs'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(minBias|minbias)(*)')
        THEN 'Minbias'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(dijet|multijet|qcd)(*)')
        THEN 'Multijet'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(performance)(*)')
        THEN 'Performance'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(singleparticle)(*)')
        THEN 'SingleParticle'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(singletop)(*)')
        THEN 'SingleTop'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(bino|susy|pmssm|leptosusy|rpv|mssm)(*)')
        THEN 'SUSY'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(triboson|triplegaugecoupling|zzw|www)(*)')
        THEN 'Triboson'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(ttbar)(*)')
        THEN 'TTbar'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(ttw|ttz|ttv|ttvv|4top)(*)')
        THEN 'TTbarX'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(upgrad)(*)')
        THEN 'Upgrade'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(w)(*)')
        THEN 'Wjets'
        WHEN REGEXP_LIKE(lower(hashtag_list), '(*)(z)(*)')
        THEN 'Zjets'
        ELSE 'Uncategorized'
      END AS phys_category
    FROM task_hashtags
    WHERE
      REGEXP_LIKE(lower(hashtag_list), '(*)(mc16a|mc16a_cp|mc16a_trig|mc16a_hpc|mc16a_pc)(*)')
),
result AS (
    SELECT
      task.taskid,
      task.step_name,
      task.phys_category,
      jd.nevents     AS requested_events,
      jd.neventsused AS processed_events
    FROM
      phys_categories task,
      ATLAS_PANDA.jedi_datasets jd
    WHERE
      task.taskid = jd.jeditaskid
      AND jd.masterid IS NULL
      AND jd.type IN ('input')
)
SELECT
  phys_category,
  step_name,
  sum(processed_events) AS processed,
  sum(requested_events) AS requested
FROM
  result
GROUP BY
  phys_category,
  step_name
ORDER BY
  phys_category,
  step_name
"""
}

queries['ora']['mc16-tasks'] = {
  'help': 'MC16 campaign, full-blown regexp'
, 'sql': """
WITH mc16a_tasks AS (
    SELECT
      task.taskid,
      task.step_id
    FROM
      ATLAS_DEFT.t_production_task task
    WHERE
      LOWER(task.campaign) = 'mc16'
      AND LOWER(task.subcampaign) = 'mc16a'
),
task_hashtags AS (
    SELECT
      tasks.taskid,
      s_t.step_name,
      LISTAGG(lower(hashtag.hashtag), ', ')
    WITHIN GROUP (ORDER BY tasks.taskid) AS hashtag_list
    FROM
      mc16a_tasks tasks,
      ATLAS_DEFT.t_ht_to_task ht_t,
      ATLAS_DEFT.t_hashtag hashtag,
      ATLAS_DEFT.t_production_step s,
      ATLAS_DEFT.t_step_template s_t
    WHERE
      tasks.taskid = ht_t.taskid
      AND hashtag.ht_id = ht_t.ht_id
      AND tasks.step_id = s.step_id
      AND s.step_t_id = s_t.step_t_id
    GROUP BY
      tasks.taskid,
      s_t.step_name
),
phys_categories AS (
    SELECT
      taskid,
      step_name,
      CASE
%(categoryWhenThen)s
        ELSE 'Uncategorized'
      END AS phys_category
    FROM task_hashtags
    WHERE
      REGEXP_LIKE(hashtag_list, '(*)(mc16a|mc16a_cp|mc16a_trig|mc16a_hpc|mc16a_pc)(*)')
),
result AS (
    SELECT
      task.taskid,
      task.step_name,
      task.phys_category,
      jd.nevents     AS requested_events,
      jd.neventsused AS processed_events
    FROM
      phys_categories task,
      ATLAS_PANDA.jedi_datasets jd
    WHERE
      task.taskid = jd.jeditaskid
      AND jd.masterid IS NULL
      AND jd.type IN ('input')
)
SELECT
  phys_category,
  step_name,
  sum(processed_events) AS processed,
  sum(requested_events) AS requested
FROM
  result
GROUP BY
  phys_category,
  step_name
ORDER BY
  phys_category,
  step_name
""" % ({'categoryWhenThen':
        ht_mapper_WHEN_THEN(hashtag2category, "        ",
	"hashtag_list", likes_REGEXP_LIKE)
      })
}

queries['ora']['mc16-tasks-LIKE-ONLY'] = {
  'help': 'MC16 campaign, optimized with LIKE'
, 'sql': """
WITH mc16a_tasks AS (
    SELECT
      task.taskid,
      task.step_id
    FROM
      ATLAS_DEFT.t_production_task task
    WHERE
      LOWER(task.campaign) = 'mc16'
      AND LOWER(task.subcampaign) = 'mc16a'
),
task_hashtags AS (
    SELECT
      tasks.taskid,
      s_t.step_name,
      LISTAGG(lower(hashtag.hashtag), ', ')
    WITHIN GROUP (ORDER BY tasks.taskid) AS hashtag_list
    FROM
      mc16a_tasks tasks,
      ATLAS_DEFT.t_ht_to_task ht_t,
      ATLAS_DEFT.t_hashtag hashtag,
      ATLAS_DEFT.t_production_step s,
      ATLAS_DEFT.t_step_template s_t
    WHERE
      tasks.taskid = ht_t.taskid
      AND hashtag.ht_id = ht_t.ht_id
      AND tasks.step_id = s.step_id
      AND s.step_t_id = s_t.step_t_id
    GROUP BY
      tasks.taskid,
      s_t.step_name
),
phys_categories AS (
    SELECT
      taskid,
      step_name,
      CASE
%(categoryWhenThen)s
        ELSE 'Uncategorized'
      END AS phys_category
    FROM task_hashtags
    WHERE
      REGEXP_LIKE(hashtag_list, '(*)(mc16a|mc16a_cp|mc16a_trig|mc16a_hpc|mc16a_pc)(*)')
),
result AS (
    SELECT
      task.taskid,
      task.step_name,
      task.phys_category,
      jd.nevents     AS requested_events,
      jd.neventsused AS processed_events
    FROM
      phys_categories task,
      ATLAS_PANDA.jedi_datasets jd
    WHERE
      task.taskid = jd.jeditaskid
      AND jd.masterid IS NULL
      AND jd.type IN ('input')
)
SELECT
  phys_category,
  step_name,
  sum(processed_events) AS processed,
  sum(requested_events) AS requested
FROM
  result
GROUP BY
  phys_category,
  step_name
ORDER BY
  phys_category,
  step_name
""" % ({'categoryWhenThen':
        ht_mapper_WHEN_THEN(hashtag2category, "        ",
	"hashtag_list", likes_PURE_LIKE)
      })
}


queries['es']['mc16-tasks'] = {
  'help': 'MC16 campaign'
, 'json': """
{
  "size": 0,
  "query": {
    "bool": {
      "must": [
        { "term": { "subcampaign.keyword": "MC16a" } }
      ],
      "should": [
        { "term": { "hashtag_list": "MC16a" } },
        { "term": { "hashtag_list": "MC16a_CP" } },
        { "term": { "hashtag_list": "MC16a_TRIG" } },
        { "term": { "hashtag_list": "MC16a_HPC" } },
        { "term": { "hashtag_list": "MC16a_PC" } }
      ]
    }
  },
  "aggs": {
    "category": {
      "terms": { "field": "phys_category" },
      "aggs": {
        "step": {
          "terms": { "field": "step_name.keyword" },
          "aggs": {
            "requested": {
              "sum": { "field": "requested_events" }
            },
            "processed": {
              "sum": { "field": "processed_events" }
            }
          }
        }
      }
    }
  }
}
"""
}


queries['ora']['keywords-1--3-joins'] = {
  'help': 'Keyword search query #1, 3 JOINS'
, 'sql': """
SELECT t.*
FROM t_production_task t
  LEFT JOIN
  ATLAS_DEFT.t_ht_to_task ht_t
  ON ht_t.taskid = t.taskid
  LEFT JOIN
  ATLAS_DEFT.t_hashtag hashtag
  ON hashtag.ht_id = ht_t.ht_id
  LEFT JOIN t_task tt
  ON t.taskid = tt.taskid
WHERE
  hashtag.hashtag = 'MC16a_CP'
  AND t.phys_group = 'MCGN'
  AND t.project = 'mc16_13TeV'
  AND to_char(NVL(substr(regexp_substr(jedi_task_parameters, '"\-\-geometryVersion=\\"default:[a-zA-Z0-9_\-]+[^\""]'),
        regexp_instr(regexp_substr(jedi_task_parameters, '"\-\-geometryVersion=\\"default:[a-zA-Z0-9_\-]+[^\""]'),
        '(:)+', 1, 1, 1)), '')) = 'ATLAS-R2-2016-01-00-01_VALIDATION'
  AND to_char(NVL(substr(regexp_substr(jedi_task_parameters, '"\-\-conditionsTag \\"default:[a-zA-Z0-9_\-]+[^\""]'),
        regexp_instr(regexp_substr(jedi_task_parameters, '"\-\-conditionsTag \\"default:[a-zA-Z0-9_\-]+[^\""]'),
        '(:)+', 1, 1, 1)), '')) = 'OFLCOND-MC16-SDR-14'
  AND rownum <= 2000
"""
}

queries['es']['keywords-1'] = {
  'help': 'Keyword search query #1'
, 'json': """
{
  "query": {
    "query_string": {
      "query": "'MC16a_CP' AND 'Atlas-21.0.1' AND 'MCGN' AND 'mc16_13TeV' AND 'ATLAS-R2-2016-01-00-01_VALIDATION' AND 'OFLCOND-MC16-SDR-14'",
      "analyze_wildcard": true
    }
  },
  "from": 0,
  "size": 2000
}
"""
}


queries['ora']['keywords-2--no-joins'] = {
  'help': 'Keyword search query #2, no JOINS'
, 'sql': """
SELECT *
FROM t_production_task
WHERE
  subcampaign = 'MC16a'
  AND phys_group = 'MCGN'
  AND project = 'mc16_13TeV'
  AND rownum <= 2000
"""
}

queries['es']['keywords-2'] = {
  'help': 'Keyword search query #2'
, 'json': """
{
  "query": {
    "query_string": {
      "query": "'MC16a' AND 'MCGN' AND 'mc16_13TeV'",
      "analyze_wildcard": true
    }
  },
  "from": 0,
  "size": 2000
}
"""
}


def main(queries):
    """ Main entry point of this badness """

    aparser = argp.ArgumentParser(description="Oracle/ES " + \
      "testing utility for HENP DM project")
    aparser.add_argument('--conf', dest = 'conf',
      default = 'henp-perfcomp.conf',
      help = 'Configuration file (specify "help" to get some)')
    aparser.add_argument('--type', dest = 'type',
      default = 'es', choices = ['es', 'ora'],
      help = "database type")
    aparser.add_argument('--query', dest = 'query',
      default = 'mc16-tasks',
      help = 'DB-specific query type ("help" provides full list)')
    aparser.add_argument('--iters', dest = 'iters',
      type = int, default = '42',
      help = 'Query repetitions to collect statistical data')
    aparser.add_argument('--explain', dest = 'explain',
      action = 'store_true', default = False,
      help = 'Just show queries to be performed')
    aparser.add_argument('--dump', dest = 'dump',
      action = 'store_true', default = False,
      help = 'Dump obtained timings')

    args = aparser.parse_args()

    if args.query == "help":
	if os.isatty(1):
          pout(0, "Query types for '%s': %s" % (args.type,
            arr_to_s(queries[args.type].keys())))
	else:
	  pout(0, arr_to_s(queries[args.type].keys(), " "))

    if args.query not in queries[args.type]:
        perror(1, "Query type '%s' isn't recognized: " % (args.query) + \
	  "allowed ones are %s" % (arr_to_s(queries[args.type].keys())))

    if args.conf == "help":
        config_help(config_keys)
	sys.exit(0)

    try:
        conf = read_config(args.conf, config_keys)
    except EnvironmentError as e:
        perror(1, "Configuration file %s: %s" % (args.conf, e))

    if args.explain:
	kind = kinds[args.type]
	qry = queries[args.type][args.query]
        pout(0, "Lingo is '%s', query '%s' is:\n{{{%s\n}}}" % \
	  (kind, qry['help'], qry[kind].rstrip("\n")))


    db_cfg = conf["%s_params" % (args.type)]
    if args.type == "ora":
        timings = do_oracle_queries(db_cfg, args.query,
          args.iters)
    else:
        timings = do_es_queries(db_cfg, args.query,
	  args.iters)


    (mean, std_dev, fit, doesnt_fit) = gaussian_fit(timings, 3)
    print("mean = %s, std_dev = %s (%s %%), fit/notfit = %d/%d" % \
      (mean, std_dev, 100.0 * std_dev / mean, len(fit), len(doesnt_fit)))

    if args.dump:
        for i in xrange(0, len(timings)):
	    print("%d\t%f" % (i + 1, timings[i]))

    sys.exit(0)


if __name__ == '__main__':
    main(queries)
