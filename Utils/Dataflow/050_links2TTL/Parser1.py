#!/usr/bin/env python

import argparse
import sys
import json
import uuid

GUIDlog = None
GUIDdict = {"p": {}, "s": {}}

GRAPH = "http://nosql.tpu.ru:8890/DAV/ATLAS"
ONTOLOGY = "http://nosql.tpu.ru/ontology/ATLAS"

# ---
# Service functions ->
# ---

def TTL_escape(s):
  """
  Escapes special characters for TTL
  """
  if type(s) == str:
    s = s.encode("string-escape")
  elif type(s) == unicode:
    s = s.encode("unicode-escape")
    s = str.replace(s, "'", r"\'")
  return s

def getGUID(t, key):
  """
  Get already assigned to the document GUID or generate a new one.
  @param t -- type of the document ("p", "s")
  """
  if not key:
    return None

  d = GUIDdict
  s = GUIDlog
  if d and s:
    # Look for the existing GUID to ensure that
    # same document has the same GUID every time it passes the processor
    # NOTE: works only if there's only single instance of this script will be run,
    #       always on the same machine etc
    if key in d.values():
      for position, i in enumerate(d.values()):
        if i == item["id"]:
          GUID = list(d.keys())[position]
    else:
      # Not checking the uniqueness of the GUID here, as
      # for UUID4 we have a miserable chance to have a collision and
      # NOTE: if we need the check, we must do it globally
      #       (to ensure that wherever we run this script it is aware
      #        of all the GUIDs currently in use)
      GUID = uuid.uuid4()
      update_history(t, str(GUID), str(key))
  else:
    GUID = uuid.uuid4()

  return GUID


def getUOID(GUID):
  """
  Retruns object indentificator for TTL.
  """
  if not GUID:
    return None

  obj = "document/%s" % GUID
  UOID = "<%s/%s>" % (GRAPH, obj)
  return UOID

# ---
# <- Service functions
# ---

# ---
# GUID history storage finctions ->
#
# Used to ensure that same document has same GUID however it was processed
# multiple times.
#
# Currently the storage is local (text files)
#
# ---

def update_history(t, key, val):
  """
  Updates global variables and writes new data to the local history storage.
  @param t = {"p"|"s"}
  """
  d = GUIDdict
  s = GUIDlog
  if d and s:
    d[t][key] = val
    try:
      s.write(t + " " + key + " " + val + "\n")
    except (AttributeError, ValueError) as e:
      sys.stderr.write("Failed to update history storage %s: %s\n" % (s, e.message))
    return True
  return False


def load_history():
  """
  Loads historical data from log-files.
  Log files contains following information:
  {{{
  <type> <UUID4> <docID>
  <type> <UUID4> <docID>
  ...
  }}},
  where <type> = {p|s}
  """
  global GUIDlog
  global GUIDdict

  if GUIDlog:
    for columns in (raw.strip().split() for raw in GUIDlog):
      GUIDdict[columns[0]][column[1]] = columns[2]
  else:
    sys.stderr.write("(WARN) No local GUID log storage specified. New GUIDs will be generated.\n")

# ---
# <- GUID history storage finctions
# ---

# ---
# TTL statement functions ->
# ---

def linkTTL(doc, notes):
  if not notes:
    return None
  if type(notes) != list:
    raise ValueError("linkTTL() expects 2nd parameter to be of type %s, got %s" % (list, type(notes)))
  result = ""
  docUOID = getUOID(doc)
  for note in notes:
    noteUOID = getUOID(note)
    attrs = {'noteUOID': noteUOID,
                 'docUOID': docUOID,
                 'ontology': ONTOLOGY
                  }
    isBasedOnTriplet = "{docUOID} <{ontology}#isBasedOn> {noteUOID} .\n".format(**attrs)
    result += isBasedOnTriplet

  return result


def notes2TTL(notes):
  """
  Transforms list of JSON (dict) with SupportingNotes metadata in TTL statements.
  Returns tuple of ( ttl_statements, [noteGUID1, ...] )
  """
  if not notes:
    return None, None
  if type(notes) != list:
    raise ValueError("notes2TTL expects parameter of type %s, got %s\n" % (list, type(notes)))
  result = ""
  GUIDS = []
  for note in notes:
    if type(note) != dict:
      sys.stderr.write("Skipping note (not dict): %s\n" % note)
      continue
    noteGUID = getGUID("s", note["id"])
    GUIDS += [noteGUID]
    noteSubject = getUOID(noteGUID)
    noteAttrs = {'noteSubject': noteSubject,
                 'ontology': ONTOLOGY,
                 'graph': GRAPH,
                 'id': note["id"],
                 'label': TTL_escape(note["label"]),
                 #                 'label': note["label"].translate(str.maketrans({"'":  r"\'"})),
                 'url': note["url"]
                }
    noteTriplet = "{noteSubject} a <{ontology}#SupportingDocument> .\n".format(**noteAttrs)
    result += noteTriplet
    noteTriplets = '''{noteSubject} <{ontology}#hasGLANCE_ID> {id} .
{noteSubject} <{ontology}#hasLabel> '{label}' .
{noteSubject} <{ontology}#hasURL> '{url}' .\n'''.format(**noteAttrs)
    result += noteTriplets

  return result, GUIDS


def doc2TTL(item):
  """
  Transforms JSON (dict) with Paper metadata in TTL statements.
  Returns tuple of ( ttl_statements, docGUID )
  """
  if type(item) != dict:
    raise ValueError("doc2TTL expects parameter of type %s, got %s\n" % (dict, type(item)))

  result = ""
  docGUID = getGUID("p", item.get("id"))
  docSubject = getUOID(docGUID)
  OWLPARAMS = {'docSubject': docSubject,
               'graph': GRAPH,
               'ontology': ONTOLOGY,
               'id': item["id"],
               'short_title': TTL_escape(item["short_title"]),
               'full_title': TTL_escape(item["full_title"]),
               #               'short_title': item["short_title"].translate(str.maketrans({"\\": r"\\", "'":  r"\'"})),
               #               'full_title': item["full_title"].translate(str.maketrans({"\\": r"\\", "'":  r"\'"})),
               'ref_code': item["ref_code"]
              }
  triplet = "{docSubject} a <{ontology}#Paper> .\n".format(**OWLPARAMS)
  triplets = '''{docSubject} <{ontology}#hasGLANCE_ID> {id} .
{docSubject} <{ontology}#hasShortTitle> '{short_title}' .
{docSubject} <{ontology}#hasFullTitle> '{full_title}' .
{docSubject} <{ontology}#hasRefCode> '{ref_code}' .\n'''.format(**OWLPARAMS)
  result += triplet
  result += triplets
  return result, docGUID

# ---
# <- TTL statement functions
# ---

# ---
# Input/Output functions ->
# ---

def get_items(fds):
  """
  Returns generator object.
  Every call of method next() returns JSON (dict) object
  with information about a document (paper).
  """
  if not fds:
    sys.stderr.write("No input file descriptors specified. Exiting.\n")
    return
  if type(fds) != list:
    sys.stderr.write("(ERROR) get_items(): expected patameter of type %s, get %s.\n" % (list, type(fds)))
    return

  for data_file in fds:
    if type(data_file) != file:
      sys.stderr.write("(ERROR) get_items(): expected list of %s, get %s: %s\nSkipping file.\n" % (file, type(data_file)))
      continue
    if data_file != sys.stdin:
      try:
        data = json.load(data_file)
      except ValueError as e:
        sys.stderr.write("Failed to parse JSON (%s): %s\nSkipping file.\n" % (data_file.name, e.message))
        continue
    else:
      data = iter(data_file.readline, '')
    for item in data:
      if type(item) != dict:
        try:
          item = json.loads(item)
        except ValueError as e:
          sys.stderr.write("Failed to parse JSON: %s\nSkipping line.\n" % e.message)
          continue
      yield item


def outputTTL(fd, *TTL):
  """
  Outputs produced TTL statements to a given file descriptor.
  """
  if type(fd) != file:
    raise ValueError("outputTTL() expects 1st parameter to be of type %s, got %s" % (file, type(fd)))
  try:
    for ttl in TTL:
      if ttl:
        fd.write(ttl)
        fd.flush()
  except IOError as e:
    sys.stderr.write("Failed to write TTL to %s: %s\n" % (fd.name, e.message))
    return False
  return True

# ---
# <- Input/Output functions
# ---

def define_globals(args):
  global GUIDlog
  GUIDlog = args.GUIDlog

  global GRAPH
  GRAPH = args.GRAPH

  global ONTOLOGY
  ONTOLOGY = args.ONTOLOGY

def main(argv):
  """
  The body of the program.
  """

  # Parsing command-line arguments
  parser = argparse.ArgumentParser(description=u'Converts Paper and SupportingDocuments basic metadata from JSON format to TTL.')
  parser.add_argument('infiles', metavar=u'JSON-FILE', type=argparse.FileType('r'), nargs='*',
                      help=u'Source JSON file.')
  parser.add_argument('-g', '--graph', action='store', type=str, nargs='?',
                      help='Virtuoso DB graph name (default: %(default)s)',
                      default=GRAPH,
                      const=GRAPH,
                      metavar='GRAPH',
                      dest='GRAPH'
                     )
  parser.add_argument('-O', '--ontology', action='store', type=str, nargs='?',
                      help='Virtuoso ontology prefix (default: %(default)s)',
                      default=ONTOLOGY,
                      const=ONTOLOGY,
                      metavar='ONT',
                      dest='ONTOLOGY'
                     )
  parser.add_argument('-o', '--output', action='store', type=argparse.FileType('w'), nargs='?',
                      help=u'Name of the file to store triples (default: <CSV-FILE without CSV>.ttl).',
                      metavar='OUTFILE',
                      dest='outfile'
                     )
  parser.add_argument('-m', '--mode', action='store', nargs='?',
                      help=u'''VALUES:
f -- works with files (default)
s -- run in a Kafka Streams mode (as processor).
Ignore options: -o|--output (use STDOUT)
''',
                      default='f',
                      dest='processing_mode',
                      choices=['f', 's']
                     )
  parser.add_argument('-d', '--delimiter', action='store', nargs='?',
                      help=u'EOP marker for Kafka mode (default: \0)',
                      default='',
                      dest='EOPmarker'
                     )
  parser.add_argument('-l', '--guid-log', action='store', type=argparse.FileType('w+'), nargs='?',
                      help=u'File to store already assigned GUID for documents.',
                      metavar='GUID-FILE',
                      dest='GUIDlog',
                      default=None,
                      const=None
                     )

  args = parser.parse_args(argv)
  if args.processing_mode == 'f':
    if not args.infiles:
      sys.stderr.write('(INFO) No input JSON file presented. Switching to streaming mode.\n')
      args.processing_mode = 's'
    if not args.outfile:
      sys.stderr.write('(INFO) No output file specified. Write to stdout\n')
      args.outfile = sys.stdout
  if args.processing_mode == 's':
    args.infiles = [sys.stdin]
    args.outfile = sys.stdout
    if not args.EOPmarker:
      args.EOPmarker = '\0'

  define_globals(args)

  # Main actions
  load_history()
  items = get_items(args.infiles)
  for item in items:
    doc_ttl, docGUID = doc2TTL(item)
    notes_ttl, noteGUIDS = notes2TTL(item.get("supporting_notes"))
    link_ttl = linkTTL(docGUID, noteGUIDS)
    if not outputTTL(args.outfile, doc_ttl, notes_ttl, link_ttl, args.EOPmarker):
      break


if __name__ == "__main__":
  main(sys.argv[1:])
