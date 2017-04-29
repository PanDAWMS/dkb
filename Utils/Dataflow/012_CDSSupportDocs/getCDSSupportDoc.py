#!/usr/bin/env python
'''
author: Maria Grigorieva
maria.grigorieva@cern.ch

refactored by: Golosova Marina
golosova.marina@gmail.com

Execute on local machine with installed invenio-client;
invenio-client also requires phantomjs (>=1.9.8 due to default SSL protocol)

input metadata: 
  - list of papers & supporting documents [JSON] from GLANCE API  

output metadata:
  1) CDS Records in JSON format
     <GLANCE_ID>.json

'''

import json
from urlparse import urlparse
import sys, getopt

sys.path.append("../")
from pyDKB.dataflow import CDSInvenioConnector, KerberizedCDSInvenioConnector

import warnings
from requests.packages.urllib3.exceptions import InsecurePlatformWarning


counter = 0

def usage():
   msg='''
USAGE
  ./getCDSPapers.py <options>

OPTIONS
  -l, --login     LOGIN   CERN account login
  -p, --password  PASSWD  CERN account password
  -k, --kerberos          Use kerberos authorization

  -m, --mode      MODE    operating mode:
                            f|file   -- default mode: read from file,
                                                      output to files
                            s|stream -- stream mode: read from STDIN,
                                                     output to STDOUT

  -h, --help              Show this message and exit
'''
   sys.stderr.write(msg)

def search_notes(cds, notes):
   '''
   NOTES is a JSON (dict) array with supporting documents information.
   Returns dict: { (str) note_id : (str|NoneType) note_metadata}
   '''
   if notes == None: return {}
   if type(notes) != list: return None
   results={}
   for note in notes:
      results[note.get("id")] = search_note(cds,note)

   return results

def search_note(cds, note):
   '''
   NOTE is a JSON node (dict) with a single supporting document information.
   Returns (str|NoneType) note_metadata
   '''
   global counter
   if type(note) != dict: return None
   url = note.get("url",None)
   if not url: return None
   url = url.replace('\\','')
   parsed = urlparse(url)
   if (parsed.netloc == 'cds.cern.ch' or parsed.netloc == 'cdsweb.cern.ch'):
      sys.stderr.write(parsed.path+"\n")
      if (parsed.path[-1:] == '/'):
         recid = parsed.path[:-1].split('/')[-1]
      else:
         recid = parsed.path.split('/')[-1]
      counter += 1
      sys.stderr.write(str(counter) + ' : ' + str(recid) + "\n")

      # metadata from CDS Invenio in json format
      results = cds.search(recid=recid, of="recjson")

      try:
         json.loads(results)
      except ValueError:
         if "You are not authorized to perform this action" in results:
            sys.stderr.write("This Supporting Document is not available for your user.\n")
         elif "Sign in with a CERN account, a Federation account or a public service account" in results:
            sys.stderr.write("This Supporting Document is not available for unauthenticated user. Specify login/password or use Kerberos authentication.\n")
         else:
            sys.stderr.write("JSON decoding failed.\n")
         results=None

   else:
      results = None

   return results

def input_file_handle(filename, cds):
   '''
   Handles input file.
   '''
   try:
      fname = 'Input/list_of_papers.json'
      data_file = open(fname)
   except IOError, e:
      sys.stderr.write("ERROR: %s: %s\n" % (fname, e.strerror))
      sys.exit(e.errno)
   else:
      with data_file:
         data = json.load(data_file)

   for item in data:
      sys.stderr.write(item["id"]+"\n")
      results = search_notes(cds, item.get("supporting_notes",None))
      if not results: continue
      if type(results) != dict: continue
      for note_id in results:
         if not results[note_id]: continue
         file = open("SupportingDocuments/%s.json" % note_id, "w")
         file.write(results[note_id])
         file.close()

   sys.stderr.write("done!\n")

def input_stream_handle(stream,cds):
   '''
   Handles input stream.
   '''
   if type(stream) != file:
      sys.stderr.write("ERROR: input_stream_handle: expected <file>, got %s.\n" % type(stream))
      return False
   if stream.closed:
      sys.stderr.write("ERROR: input_stream_handle: <file> is already closed.\n")
      return False

   instream = iter(stream.readline, '')

   for raw_item in instream:
      try:
         item = json.loads(raw_item)
      except ValueError:
         sys.stderr.write("WARNING: can't decode input line as JSON. Skipping.\n")
         continue

      results = search_notes(cds, item.get("supporting_notes",None))
      if not results: continue
      for note_id in results:
         if results[note_id]:
           sys.stdout.write(results[note_id]+"\n")

      # Shell we mark the "end-of-processing" even when no data found?..
      sys.stdout.write("\0")
      sys.stdout.flush()

def main(argv):
   login = ''
   password = ''
   kerberos = False
   mode = 'f'
   try:
      opts, args = getopt.getopt(argv, "hl:p:km:",["login=","password=","kerberos","mode="])
   except getopt.GetoptError:
      usage()
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         usage()
         sys.exit()
      elif opt in ("-l", "--login"):
         login = arg
      elif opt in ("-p", "--password"):
         password = arg
      elif opt in ("-k", "--kerberos"):
         kerberos = True
      elif opt in ("-m", "--mode"):
         mode = arg

   if not login and not kerberos:
      sys.stderr.write("WARNING: no authentication method will be used.\n")

   warnings.simplefilter("once",InsecurePlatformWarning)

   if kerberos: Connector=KerberizedCDSInvenioConnector
   else:        Connector=CDSInvenioConnector

   with Connector(login,password) as cds:

      if mode in ("f","file"):
         input_file_handle('Input/list_of_papers.json',cds)

      elif mode in ("s", "stream"):
         input_stream_handle(sys.stdin,cds)

      else:
         sys.stderr.write("Wrong value for MODE parameter: %s\n" % mode)
         usage()
         exit(2)

if __name__ == "__main__":
   main(sys.argv[1:])
