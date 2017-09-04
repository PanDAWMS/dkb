'''
  author: Maria Grigorieva
  maria.grigorieva@cern.ch

  Get Internal Notes full-text PDF, using SSO authentication
  Execiuted on mgrigori@lxplus.cern.ch

  User must be registered in Kerberos 
  kinit

  input metadata:
    - list of PDF urls in JSON-format

  output data:
    - PDF documents
'''

import sys, getopt
import json
import requests
import os
from urlparse import urlparse

def main(argv):
   cookie_path = '/data/atlswing-home/ssocookie.txt'
   with open('urls.json') as data_file:
       data = json.load(data_file)

   for item in data:
      os.system('cern-get-sso-cookie --krb -r -u %s -o %s' % (item['url'], cookie_path))
      os.system('curl -k -L --cookie %s --cookie-jar %s %s -o %s' % (cookie_path, cookie_path, item['url'], 'InternalNotesPDF/'+item['id']+'.pdf'))

if __name__ == "__main__":
   main(sys.argv[1:])
