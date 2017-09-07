'''
  author: Maria Grigorieva
  maria.grigorieva@cern.ch

  Get papers with links to Supporting documents
  from GLANCE api

  request:
  https://glance-stage.cern.ch/api/atlas/analysis/papers

  output data:
    - json-document
'''

import sys
import getopt
import json
import requests
import os
from urlparse import urlparse


def main(argv):
    cookie_path = './glance.cookie'
    GLANCE_API_HTTP_REQUEST = 'https://glance-stage.cern.ch/api/atlas'\
        '/analysis/papers'
    # cern-get-sso-cookie --nocertverify -krb \
    # --url https://glance-stage.cern.ch/api/atlas/analysis/papers \
    # --outfile ./glance.cookie
    os.system('cern-get-sso-cookie --nocertverify --krb --url %s --outfile %s'
              % (GLANCE_API_HTTP_REQUEST, cookie_path))
    # curl -k -L --cookie ./glance.cookie --cookie-jar \
    # ./glance.cookie https://glance-stage.cern.ch/api/atlas/analysis/papers \
    # -o papers.json
    os.system('curl -k -L --cookie %s --cookie-jar %s %s -o %s' % (
        cookie_path, cookie_path, GLANCE_API_HTTP_REQUEST,
        'list_of_papers.json'))


if __name__ == "__main__":
    main(sys.argv[1:])
