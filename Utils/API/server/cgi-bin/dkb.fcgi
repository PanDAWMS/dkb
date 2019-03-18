#!/usr/bin/env python

import logging
import json
import urlparse
import sys
import os
import signal

logging.basicConfig(format="%(asctime)s (%(levelname)s) %(message)s",
                    level=logging.DEBUG)

def signal_handler(sig, frame):
    logging.info('Stopping server.')
    os.remove('%%SOCK%%')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGQUIT, signal_handler)

sys.path.append(os.path.join("%%WWW_DIR%%", "lib", "dkb"))


import api
from api import methods
from api import STATUS_CODES


def parse_params(qs):
    """ Parse query string to get parameters hash.

    Parse rules:
     * parameter without value (``?key1&...``) has value ``True``;
     * parameter with value 'false' (case insensitive: ``?key=FaLsE&...``)
       has value ``False``;
     * parameter with single value (``?key=val&...``) has value passed
       as ``val``;
     * parameters with number of values (``?key[]=val1&key[]=val2&...``)
       has value of type ``list``: ``['val1', 'val2', ..]``.

    :param qs: query string
    :type qs: str

    :return: parsed parameters with values
    :rtype: hash
    """
    params = urlparse.parse_qs(qs, True)
    for key in params:
        if len(params[key]) == 1:
            params[key] = params[key][0]
        if params[key] == '':
            params[key] = True
        elif params[key].lower() == 'false':
            params[key] = False
    return params


def dkb_app(environ, start_response):
    path = environ.get('SCRIPT_NAME')
    logging.debug('REQUEST: %s' % path)
    params = parse_params(environ.get('QUERY_STRING', ''))
    logging.debug('PARAMS: %s' % params)
    response = None
    error = None
    try:
        handler = methods.handler(path)
        response = handler(path, **params)
        status = response.pop('_status', 200)
    except Exception, err:
        error = methods.error_handler(sys.exc_info())
        status = error.pop('_status', 500)
    status_line = "%(code)d %(reason)s" % {
        'code': status,
        'reason': STATUS_CODES.get(status, 'Unknown')
    }
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
