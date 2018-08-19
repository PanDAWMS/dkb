#!/usr/bin/env python

import logging
import json
import urlparse

logging.basicConfig(format="%(asctime)s (%(levelname)s) %(message)s",
                    level=logging.DEBUG)


def dkb_app(environ, start_response):
    path = environ.get('SCRIPT_NAME')
    logging.debug('REQUEST: %s' % path)
    params = urlparse.parse_qs(environ.get('QUERY_STRING', ''), True)
    response = {'text_info': 'DKB API server. Status: WIP'}
    start_response('200 OK', [('Content-Type', 'application/json')])
    result = {}
    result['response'] = response
    indent = None
    newline = ''
    if params.get('pretty'):
        indent = 2
        newline = '\n'
    result = json.dumps(result, indent=indent).encode('utf-8') + newline
    return [result]


if __name__ == '__main__':
    from flup.server.fcgi import WSGIServer
    logging.info("Starting server.")
    WSGIServer(dkb_app, bindAddress='%%SOCK%%', umask=0117).run()
