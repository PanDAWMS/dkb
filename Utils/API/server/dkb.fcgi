#!/usr/bin/env python

import logging
import json

logging.basicConfig(format="%(asctime)s (%(levelname)s) %(message)s",
                    level=logging.DEBUG)


def dkb_app(environ, start_response):
    path = environ.get('SCRIPT_NAME')
    logging.debug('REQUEST: %s' % path)
    response = {'text_info': 'DKB API server. Status: WIP'}
    start_response('200 OK', [('Content-Type', 'application/json')])
    result = {}
    result['response'] = response
    result = json.dumps(result).encode('utf-8')
    return [result]


if __name__ == '__main__':
    from flup.server.fcgi import WSGIServer
    logging.info("Starting server.")
    WSGIServer(dkb_app, bindAddress='%%SOCK%%', umask=0117).run()
