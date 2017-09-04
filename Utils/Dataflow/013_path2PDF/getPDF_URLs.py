'''
  author: Maria Grigorieva
  maria.grigorieva@cern.ch

  Get Internal Notes full-text URLs
  Executed on local machine with Invenio Client

  input metadata:
    - list of Papers & Supporting Documents from GLANCE API

  output metadata:
    - list of urls in JSON-format
    
'''
import sys, getopt
import json
import requests
import os
from urlparse import urlparse
from invenio_client import InvenioConnector
from invenio_client.contrib.cds import CDSInvenioConnector
import json

def main(argv):
   login = ''
   password = ''
   try:
      opts, args = getopt.getopt(argv, "hl:p:", ["login=", "password="])
   except getopt.GetoptError:
      print '-l <login> -p <password>'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print '-l <login> -p <password>'
         sys.exit()
      elif opt in ("-l", "--login"):
         login = arg
      elif opt in ("-p", "--password"):
         password = arg
   

   cookie_path = '/data/atlswing-home/ssocookie.txt'
   cds = CDSInvenioConnector(login, password)
   
   with open('Input/list_of_papers.json') as data_file:
       data = json.load(data_file)
   
   js_list = []

   with open('SupportingDocumentsURLS.json', 'w') as outfile:
     outfile.write("[")


     last = len(data) - 1

     for i, item in enumerate(data):
         print item["id"]
         if "supporting_notes" in item:
            for note in item["supporting_notes"]:
                parsed = urlparse(note["url"])
                if (parsed.netloc == 'cds.cern.ch' or
                    parsed.netloc == 'cdsweb.cern.ch'):
                   recid = parsed.path.split('/')[2]
                   print recid
                   results = cds.get_record(recid)
                   if len(results) > 0:
                        try:
                            urls = results[0]["8564_u"]
                            for item in urls:
                                if (item.split('.')[-1] == 'pdf'):
                                   url = item
                            js          = {}
                            js["recid"] = recid
                            js["url"]   = url
                            js["id"]    = note["id"]
                            # js_list.append(js)
                            json.dump(js, outfile)
                            if i != last:
                              outfile.write(",")
                            elif i == last:
                              outfile.write("]")
                        except Exception:
                          print "broken mark record..."

   # with open('SupportingDocumentsURLS.json', 'w') as outfile:
   #    json.dump(js_list, outfile)
   
if __name__ == "__main__":
   main(sys.argv[1:])
