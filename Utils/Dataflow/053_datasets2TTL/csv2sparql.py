#!/usr/bin/env python

# Prepares CSV data for upload to Virtuoso

# csv2sparql.py <options> <filename> 
#
# -t, --type        Source type: GID|...
# -g, --graph       Virtuoso DB graph name
# -o, --ontology    Virtuoso ontology prefix
# -L, --link-file   Where to store linking queries
# -O, --output      Where to store prepared triples

import argparse, warnings

import sys, csv, re
from datetime import datetime

### DEFAULTS AND CONFIGURATIONS ###

# Default CSV header
CSV_HEADER = ["datatype", "glanceid", "name", "tid", "chain_tid", "phys_group", "events", "files", "status", "timestamp", "pr_id", "campaign", "ddm_erase_timestamp", "vuid", "grid_exec", "se", "file_size_mb"]

# ONTOLOGY --->
# TODO: make things more friendly, perhaps dict of 
#       { <codename>: {'prop': <property>, 'type': <type>}}
# TODO: And maybe one day we'll teach it reading the *.owl files. 
#       THAT would be really nice.

# Dataset properties
# Ordinary numeric/string properties: <dataset> <#property> Value
OWL_PARAMS_NUMSTR = {
    #numeric
           'DSID': 'hasDatasetID',
           'events': 'hasEvents',
           'files': 'hasFiles',
           'datasetSize': 'hasDatasetSize',  # hasFiles * hasFileSizeMB
           'file_size_mb': 'hasFileSizeMB',
    #string
           'name': 'hasDatasetName',
           'timestamp': 'hasTimestamp',
           'AMItags': 'hasAMITag',
           'status': 'hasStatus',
           'physKeyword': 'hasPhysKeyword'
}

# Only numeric properties
OWL_PARAMS_NUM = {
    'DSID': 'hasDatasetID',
           'events': 'hasEvents',
           'files': 'hasFiles',
           'datasetSize': 'hasDatasetSize',  # hasFiles * hasFileSizeMB
           'file_size_mb': 'hasFileSizeMB'
}

# Object properties: <dataset> <#property> <DBobject>
OWL_PARAMS_OBJ = {
    'campaign': 'hasCampaign',
           'dataFormat': 'hasDataSampleFormat',
           'datatype': 'hasDataSampleType', # #Container|#MC|#RealData
           'generator': 'hasGenerator',
           'physGroup': 'hasPhysGroup',
           'prodStep': 'hasProductionStep',
           'project': 'hasProject',
           'usedIn': 'usedIn'
}
# <--- ONTOLOGY

# Known generator names
GENERATORS = ['acermc', 'alpgen',
              'charybdis', 'comphep',
              'evtgen',                  # <--- There might also be smth like 'EG'
              'gg2vv',                        ## but how do we supposed to know
              'herwig', 'herwig\+\+', 'hijing', ## if it is 'EG' or 'powhEG', or any
              'isajet',                       ## other word?
              'jimmy',
              'pythia8', 'pythia6', 'pythiab', 'pythia', 'py8',
              'madgraph5', 'madgraph', 'mcatnlo', 'mc@nlo',
              'photos', 'powheg',
              'sherpa',
              'tauola'
           ]
# Known synonyms from the list above.
# Basic name is a key, and value is a list of synonyms.
GEN_SYN = { 'mc@nlo': ['mcatnlo'],
            'pythia8': ['py8']
         }

# Regular expressions to handle dataset name
GENERATOR = re.compile('(?P<generator>' + '|'.join(GENERATORS) + ')', flags=re.I)
DSNAME_data = re.compile('^(?P<project>data[0-9_a-zA-Z]*)\.(?P<DSID>[0-9]*)\.(?P<streamName>[^.]*)\.(?P<prodStep>[^.]*)\.(?P<datatype>[^.]*)\.(?P<AMItag>[^./]*)(?P<container>/)?$')
DSNAME_mc = re.compile('^(?P<project>mc[0-9_a-zA-Z]*)\.(?P<DSID>[0-9]*)\.(?P<phys_short>[^.]*)\.(?P<prodStep>[^.]*)\.(?P<datatype>[^.]*)\.(?P<AMItag>[^./]*)(?P<container>/)?$')

class CloseFileException(Exception):
  pass

def add_sparql(line, linkfile, triple_map):
  # Adds part of linking query, to link dataset, mentioned in this CSV line,
  # and its document.

  gid = line.get('glanceid')
  name = line.get('name')
  if not gid or not name: return False

  # Create linking statements
  triple_map['GlanceID'] = gid
  triple_map['obj'] = "datasets/" + line['name']
  linkquery = '''( <{graph}/{obj}> {GlanceID} )'''.format(**triple_map)
  linkfile.write(linkquery)
  return True



def check_synonyms(word, syns):
  # Checks if given word (words) have any default form, in which they are to
  # be processed further, and returns transformed (if needed) value.

  if type(syns) != dict: return word
  if type(word) != list:
    for t in syns:
      if word in syns[t]: return t
  else:
    for i in range(0, len(word)):
      for t in syns:
        if word[i] in syns[t]: word[i] = t
  return word


def add_ttl(line, outfile, triple_map):
  # Transforms single CSV line to a set of triples, writing them to file.
  # Transformation rules are basically defined by global variables,
  # but also have some specifics in the code itself.
  # TODO: teach it to work with any data, not only dataset metadata.

  # Get values from dataset name
  name = line.get('name')
  if not name:
    sys.stderr.write('Can`t find dataset name in CSV file\n')
    return False

  m = re.match(DSNAME_data, name)
  if m:
    line['datatype'] = 'RealData'
  else:
    m = re.match(DSNAME_mc, name)
    if m:
      line['datatype'] = 'MC'

  if m:
    if m.group('container'):
      line['datatype'] = 'Container'
  else:
    sys.stderr.write('Can`t analyze dataset name: {name}\n'.format(name=name))
    return False

  line['DSID'] = m.group('DSID')
  line['dataFormat'] = m.group('datatype').split('_')[0].upper() if m.group('datatype') else None
  line['project'] = 'project:' + m.group('project').split(':')[0]  if m.group('project') else None
  line['prodStep'] = m.group('prodStep').lower()   if m.group('prodStep') else None
  line['AMItag'] = m.group('AMItag')
  try:
    if m.group('phys_short'):
      line['generator'] = re.findall(GENERATOR, m.group('phys_short').lower())
      line['generator'] = check_synonyms(line['generator'], GEN_SYN)

      kwds = m.group('phys_short').replace('_', '__').replace('no__filter', 'no_filter').split('__')
      line['physKeyword'] = []
      for kw in kwds:
        if re.match(GENERATOR, kw): continue
        line['physKeyword'] += [kw]
  except IndexError:
    pass


  # Refine timestamp
  t = None if (line['timestamp'] in ('NULL', '\N', None)) else int(line['timestamp']) / 100
  if t:
    line['timestamp'] = datetime.fromtimestamp(t).strftime("%Y-%m-%dT%H:%M:%S")

  # Prepare triples
  # Add object...
  triple_map['obj'] = "datasets/" + line['name']
  triple = "<{graph}/{obj}> a <{ontology}#DataSample> .\n".format(**triple_map)
  outfile.write(triple)
  # ...and its roperties
  for p in line:
    if not line[p] or line[p] in ('NULL', '\N'): continue
    value = '{value}'
    val = line[p]; prop = ''
    if type(val) != list: val = [val]
    if p in OWL_PARAMS_NUMSTR.keys():
      val = str(val).strip('[]')  # "'a', 'b', 'c'" 4strings OR "1, 2, 3" 4ints
      if p in OWL_PARAMS_NUM.keys():
        val = val.replace("'", '') # As numeric values could be read as strings
      prop = OWL_PARAMS_NUMSTR[p]
    elif p in OWL_PARAMS_OBJ.keys():
      prop += OWL_PARAMS_OBJ[p]
      v1 = []
      for v in val:
        if not line[p] or line[p] in ('NULL', '\N'): continue
        # TODO: CHECK, if such an object exists!
        v = v.lower() if type(v) == str else v
        v1 += ['<{ontology}#' + str(v) + '>']
      val = str(v1).strip('[]').replace("'", '') # "<ont#val>, <ont#val>"
      value = val
    else:
      warnings.warn('Skipping unknown dataset property: {p}'.format(p=p), Warning)
      continue

    triple = "<{graph}/{obj}> <{ontology}#{property}> " + value + " .\n"

    triple_map['property'] = prop
    triple_map['value'] = val
    outfile.write(triple.format(**triple_map))

  return True


def LinkFiles(basename, ext='', beg='', end=''):
  # Provides mechanism of switching to the next file.
  # 1) Chooses new file name (adding '.N.' before extention '.sparql')
  #    TODO: make it work properly with any extention
  # 2) Closes prev. file and opens new one.
  # 3) Add same "beginning" and "ending" to the files, so all you need is to
  #    write the substantial part
  n = 0
  if ext and ext[0] != '.': ext = '.' + ext
  if type(basename) == file:
    f = basename
    basename = f.name.split('.')
    if len(basename) > 2 and basename[-1] == ext:
      n = basename[-2]
      try:
        n = int(n)
        dot = f.name.rfind('.', 0, -len(ext))
        basename = f.name[: dot if dot > 0 else None]
      except ValueError, TypeError:
        n = 0
        dot = f.name.rfind('.')
        basename = f.name[:dot if dot > 0 else None]
    else:
      dot = f.name.rfind('.')
      basename = f.name[:dot if dot > 0 else None]
    if not f.closed:
      if f.tell() == 0: f.write(beg)
      n += 1
      try:
        yield f
        f.write(end)
        f.close()
      except CloseFileException:
        f.write(end)
        f.close()
        yield None

  while True:
    if basename == '/dev/stdout':
      new_name = basename
    else:
      new_name = '{basename}{n}{ext}'.format(basename=basename, n='.' + str(n) if n > 0 else '', ext=ext)
    sys.stderr.write("(INFO) LinkFiles: another call ({nn})".format(nn=new_name))
    n += 1
    try:
      f = open(new_name, 'w', 0)
      f.write(beg)
    except IOError as e:
      sys.stderr.write("I/O error({0}): {1}\n".format(e.errno, e.strerror))
      break
    except GeneratorExit as e:
      sys.stderr.write("Generator Exit")
      raise
    except:
      sys.stderr.write("Unexpected error: {0}\n".format(sys.exc_info()[0]))
      raise

    try:
      yield f
      f.write(end)
      f.close()
    except CloseFileException:
      f.write(end)
      f.close()
      yield None
  yield None


def csv2ttl(csvfile, outfile, linkfile, args=None):
  # Converts single CSV file to TTL and SPARQL files

  spamreader = csv.reader(iter(csvfile.readline, ''), delimiter=',')

  if args.processing_mode:
    # Use default headers in case of mapreduce mode
    headers = CSV_HEADER
  else:
    # Read first line and check if it is a header line
    # and memorise file basename (without extention)
    fname = csvfile.name[:csvfile.name.rfind('.')]
    headers = spamreader.next()
    if 'name' not in headers or \
       'glanceid' not in headers:
      csvfile.seek(0)
      headers = CSV_HEADER
      warnings.warn("{0}: Can`t find header in CSV file. Use default header.".format(csvfile.name), Warning)

  triple_map = {'graph': args.graph, 'ontology': args.ontology}

  if args.mode in ('link', None):
    # To create links we need opening/close statements
    linkquery_beg = '''WITH <{graph}>
INSERT {{
?dataset <{ontology}#usedIn> ?doc .
}}
WHERE {{
values (?dataset ?GlanceID) {{'''.format(**triple_map)

    linkquery_end = '''}}
?doc a <{ontology}#SupportingDocument> .
?doc <{ontology}#hasGLANCE_ID> ?GlanceID .
}};
'''.format(**triple_map)

    if args.processing_mode == 's':
    # For stream processing we want to put the whole query in one string
    # and mark the end of line (with last '\n')
    #                 and the end of input string processing (with '\0')
      linkquery_beg = linkquery_beg.replace("\n", " ")
      linkquery_end = linkquery_end.replace("\n", " ") + "\n\0"

    close_link = False
    if args.processing_mode in ('m', 's'):
      linkfile = '/dev/stdout'    # not <stdout>, as it can be closed acsidentally
    if not linkfile: linkfile = fname
    if type(linkfile) != file: close_link = True
    linkfiles = LinkFiles(linkfile, '.sparql', linkquery_beg, linkquery_end)
    linkfile = linkfiles.next()

    linkquery_flag = False

  if args.mode in ('ttl', None):
    close_out = False

    # There's no real need to use LinkFiles here,
    # but as it handles extention and file opening -- why not?
    if args.processing_mode in ('m', 's'):
      outfile = '/dev/stdout'    # not <stdout>, as it can be closed acsidentally
    if not outfile: outfile = fname
    if type(outfile) != file: close_out = True
    outfiles = LinkFiles(outfile, '.ttl')
    outfile = outfiles.next()

    triples_flag = False

  nlfiles = 1
  for csvstr in spamreader:
    line = dict(zip(headers, csvstr))
    if args.mode in ('ttl', None): 
      triples_flag |= add_ttl(line, outfile, triple_map)
      triple_map = {'graph': args.graph, 'ontology': args.ontology}
      if args.processing_mode == 's':
        outfile.write("\0") # the mark saying current string is fully processed
    if args.mode in ('link', None):
      linkquery_flag += add_sparql(line, linkfile, triple_map)
      if args.processing_mode != 's':
        linkfile.write("\n") # having thousands of query pieces in one line
                             # is just not beautiful
      triple_map = {'graph': args.graph, 'ontology': args.ontology}

      # We need to finalize the linking query every ~ 6000 link objects
      # to keep query files within ~1M (which was good enough for uploading)
      # TODO: define exact treshold. I believe it depends actually on
      #       the number of objects, not filesize.
      if linkquery_flag >= args.N * nlfiles:
        linkfile = linkfiles.next()
        nlfiles += 1
        warnings.warn('Link query is too long. Continue in a new file: {0}'.format(linkfile.name), Warning)

  if args.mode in ('ttl', None):
    if not triples_flag:
      outfile.truncate()
    if close_out:
      outfile.close()     # As it was opened in this function for this CSV file

  if args.mode in ('link', None):
    if not linkquery_flag:
      linkfile.truncate()
    if close_link:
      linkfiles.throw(CloseFileException)    # So that linkfiles can properly finalize file
      linkfile = None       # As it was passed as None, it should be returned as None

    linkfiles.close()

  return linkfile

def main(argv):
  parser = argparse.ArgumentParser(description=u'Reads CSV-file with information about datasets and produces files with triples and Dataset-Document linking statements.')
  parser.add_argument('csv', metavar=u'CSV-FILE', type=argparse.FileType('r'), nargs='*',
                      help=u'Source CSV-file.')
  parser.add_argument('-g', '--graph', action='store', type=str, nargs='?',
                      help='Virtuoso DB graph name (default: %(default)s)',
                      default='http://nosql.tpu.ru:8890/DAV/ATLAS',
                      const='http://nosql.tpu.ru:8890/DAV/ATLAS',
                      metavar='GRAPH',
                      dest='graph'
                     )
  parser.add_argument('-O', '--ontology', action='store', type=str, nargs='?',
                      help='Virtuoso ontology prefix (default: %(default)s)',
                      default='http://nosql.tpu.ru/ontology/ATLAS',
                      const='http://nosql.tpu.ru/ontology/ATLAS',
                      metavar='ONT',
                      dest='ontology'
                     )
  parser.add_argument('-l', '--link-file', action='store', type=argparse.FileType('w'), nargs='?',
                      help=u'Name of the file to store Dataset-Document linking statements (default: <CSV-FILE without CSV>.sparql). WARNING: please use custom names with ".sparql" extention.',
                      metavar='FILE',
                      dest='linkfile'
                     )
  parser.add_argument('-N', '--link-number', action='store', type=int, nargs='?',
                      help=u'Maximum number of links in one link query file.',
                      const='6000',
                      default=6000,
                      dest='N'
                     )
  parser.add_argument('-o', '--output', action='store', type=argparse.FileType('w'), nargs='?',
                      help=u'Name of the file to store triples (default: <CSV-FILE without CSV>.ttl).',
                      metavar='FILE',
                      dest='outfile'
                     )
  parser.add_argument('-m', '--mode', action='store', nargs='?',
                      help=u'''VALUES: m -- run in Hadoop MapReduce mode (as mapper).
s -- run in a Kafka Streams mode (as processor).
Ignore options: -o|--output (stdout), -l|--link-file (stdout) , -N|--link-number (1);
Default mode (-T|-L): TTL.''',
                      default=None,
                      dest='processing_mode',
                      choices=['m', 's', None]
                     )
  parser.add_argument('-T', '--ttl', action='store_const',
                      help=u'Generate TTL file only.',
                      const='ttl',
                      default=None,
                      dest='mode'
                     )
  parser.add_argument('-L', '--link', action='store_const',
                      help=u'Generate link (SPARQL) file only.',
                      const='link',
                      default=None,
                      dest='mode'
                     )


  args = parser.parse_args(argv)
  linkfile = args.linkfile
  if not args.csv:
    if args.processing_mode in ('m', 's'):
      args.csv = [sys.stdin]
    else:
      sys.stderr.write('(ERROR) No input CSV file presented.')
      exit(1)

  if args.processing_mode in ('m', 's') and not args.mode:
    args.mode = 'ttl'

  if args.processing_mode == 's':
    args.N = 1

  for infile in args.csv:
    linkfile = csv2ttl(infile, args.outfile, linkfile, args)
    infile.close()

if __name__ == '__main__':
  main(sys.argv[1:])
