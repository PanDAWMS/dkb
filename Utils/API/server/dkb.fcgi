#!/usr/bin/env python

import logging
import json
import urlparse
import sys

logging.basicConfig(format="%(asctime)s (%(levelname)s) %(message)s",
                    level=logging.DEBUG)

sys.path.append("%%WWW_DIR%%")

import api
from api import methods
from api import STATUS_CODES

def dkb_app(environ, start_response):
    path = environ.get('SCRIPT_NAME')
    logging.debug('REQUEST: %s' % path)
    params = urlparse.parse_qs(environ.get('QUERY_STRING', ''), True)
    response = None
    error = None
    try:
        handler = methods.handler(path)
        response = handler(path, **params)
        status = response.pop('_status', 200)
    except Exception, err:
        error = methods.error_handler(sys.exc_info())
        status = error.pop('_status', 500)
    status_line = "%(code)d %(reason)s" % {'code': status,
                                           'reason': STATUS_CODES[status]}
    start_response(status_line, [('Content-Type', 'application/json')])
    if status/100 == 2:
        str_status = 'OK'
    else:
        str_status = 'failed'
    result = {'status': str_status}
    if response is not None:
        result['response'] = response
    if error is not None:
        result['error'] = error
    indent = None
    newline = ''
    if params.get('pretty'):
        indent = 2
        newline = '\n'
    result = json.dumps(result, indent=indent).encode('utf-8') + newline
    return [result]


if __name__ == '__main__':
    logging.info("Reading server configuration.")
    if not api.configure():
        logging.info("Stopping.")
        sys.exit(1)
    from flup.server.fcgi import WSGIServer
    logging.info("Starting server.")
    WSGIServer(dkb_app, bindAddress='%%SOCK%%', umask=0117).run()
