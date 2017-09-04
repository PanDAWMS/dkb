#!/usr/bin/env python
'''
author: Maria Grigorieva
maria.grigorieva@cern.ch

refactored by: Marina Golosova
golosova.marina@gmail.com

Execute on atlswing@aipanda070.cern.ch

input metadata:
  - list of papers & supporting documents [JSON] from GLANCE API

output metadata:
  - CDS Papers in JSON format

'''

from invenio_client import InvenioConnector
from invenio_client.contrib.cds import CDSInvenioConnector
import json
import sys, getopt
import uuid
import os

def usage():
  msg = '''
USAGE
  ./getCDSPapers.py <options>

OPTIONS
  -l, --login     LOGIN   CERN account login
  -p, --password  PASSWD  CERN account password
  -m, --mode      MODE    operating mode:
                            f|file   -- default mode: read from file,
                                                      output to files
                            s|stream -- stream mode: read from STDIN,
                                                     output to STDOUT

  -h, --help              Show this message and exit
'''
  sys.stderr.write(msg)

def search_paper(cds, paper_info):
   '''
   Performing CDS search by given paper info.
   Search parameters:
        aas - advanced search ("0" means no, "1" means yes).  Whether
                     search was called from within the advanced search
                     interface.
        p1 - first pattern to search for in the advanced search
                     interface.  Much like 'p'.

        f1 - first field to search within in the advanced search
             interface.  Much like 'f'.

        m1 - first matching type in the advanced search interface.
             ("a" all of the words, "o" any of the words, "e" exact
             phrase, "p" partial phrase, "r" regular expression).

        op1 - first operator, to join the first and the second unit
              in the advanced search interface.  ("a" add, "o" or,
              "n" not).
        p2 - second pattern to search for in the advanced search
                     interface.  Much like 'p'.

        f2 - second field to search within in the advanced search
             interface.  Much like 'f'.

        m2 - second matching type in the advanced search interface.
             ("a" all of the words, "o" any of the words, "e" exact
             phrase, "p" partial phrase, "r" regular expression).

        op2 - second operator, to join the second and the third unit
             in the advanced search interface.  ("a" add, "o" or,
             "n" not).
        of - output format (e.g. "hb").
        cc - current collection (e.g. "ATLAS").  The collection the
                     user started to search/browse from.
   '''
   sys.stderr.write(paper_info["id"] + "\n")
   #results = cds.search(cc="ATLAS", aas=1, m1="e", op1="a", p1=paper_info["full_title"], f1="title", m2="a", op2="a", p2="ARTICLE, ATLAS_Papers", f2="collection", m3="a", p3=paper_info["ref_code"], f3="report_number", of="recjson")
   results = cds.search(cc="ATLAS", aas=1, m1="p", p1=paper_info["ref_code"], f1="reportnumber", m2="a", op2="a", p2="ARTICLE, ATLAS_Papers", f2="collection", of="recjson")
   try:
      res = json.loads(results)
      sys.stderr.write("count = " + str(len(res)) + "\n")
      for item in res:
         item['glance_id'] = paper_info["id"]
      return res
   except ValueError:
      sys.stderr.write("Decoding JSON has failed\n")
      return None

#   return results

def collection_verification(collection):
   if len(collection) > 0 and type(collection[0]) is dict and collection[0]['primary'] in ('ARTICLE', 'ATLAS_Papers'):
      return True
   elif type(collection) is list:
      try:
         if collection[0]['primary'] in ('ARTICLE', 'ATLAS_Papers'):
            return True
      except (IndexError, TypeError, ValueError):
         return False


def main(argv):
   login = ''
   password = ''

   try:
      opts, args = getopt.getopt(argv, "hl:p:m:",["login=","password=","mode="])
   except getopt.GetoptError:
      usage()
      sys.exit(2)

   # Default parameters
   mode = "file"

   for opt, arg in opts:
      if opt == '-h':
         usage()
         sys.exit()
      elif opt in ("-l", "--login"):
         login = arg
      elif opt in ("-p", "--password"):
         password = arg
      elif opt in ("-m", "--mode"):
         mode = arg

   cds = CDSInvenioConnector(login, password)

   if mode in ('f','file'):
      with open('list_of_papers_formatted.json') as data_file:
         data = json.load(data_file)
         for item in data:
            results = search_paper(cds, item)
            if not results: continue
            for res in results:
               if (collection_verification(res['collection']) == True):
                  unique_filename = 'Output_1/' + str(uuid.uuid4()) + '.json'
                  while os.path.isfile(unique_filename):
                     unique_filename = 'Output_1/' + str(uuid.uuid4()) + '.json'
                  file = open(unique_filename, 'w')
                  file.write(json.dumps(results))
                  file.write("\n\0")
                  file.close()
            sys.stderr.write("done!\n")

   elif mode in ('s','stream'):
      instream = iter(sys.stdin.readline, '')
      for data_str in instream:
         data = json.loads(data_str)
         results = search_paper(cds, data)
         if not results: continue
         for res in results:
            if (collection_verification(res['collection']) == True):
               sys.stdout.write(res + "\n\0")
               sys.stdout.flush()

   else:
      sys.stderr.write("Wrong value for MODE parameter: {m}\n".format(m=mode))
      usage()
      exit(2)

if __name__ == "__main__":
   main(sys.argv[1:])
