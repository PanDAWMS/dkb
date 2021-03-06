#!/usr/bin/env python

import logging
import json
import urlparse
import sys
import os
import signal
from datetime import datetime
import time

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
from api.exceptions import InvalidArgument, MethodException


RESPONSE_TYPE = {'json': 'application/json',
                 'img': 'image/png'
                 }


def parse_params(qs):
    """ Parse query string to get parameters hash.

    Parse rules:
     * parameter without value (``?key1&...``) has value ``True``;
     * parameter with value 'false' (case insensitive: ``?key=FaLsE&...``)
       has value ``False``;
     * parameter with single value (``?key=val&...``) has value passed
       as ``val``;
     * parameters with number of values (``?key=val1&key=val2&...``)
       has value of type ``list``: ``['val1', 'val2', ..]``.

    :param qs: query string
    :type qs: str

    :return: parsed parameters with values
    :rtype: hash
    """
    params = urlparse.parse_qs(qs, True)
    for key in params:
        for idx,val in enumerate(params[key]):
            if val == '':
                params[key][idx] = True
            elif val.lower() == 'false':
                params[key][idx] = False
            else:
                # Try to detect and parse date/datetime parameter values
                formats = ['%Y-%m%dT%H:%M:%S', '%Y-%m-%d']
                for f in formats:
                    try:
                        params[key][idx] = datetime.strptime(val, f)
                    except ValueError:
                        pass
        if len(params[key]) == 1:
            params[key] = params[key][0]
    if not params.get('rtype'):
        params['rtype'] = 'json'
    return params


def construct_response(handler_response, rtime=None, rtype='json', pretty=False):
    """ Construct HTTP response according to the requested type.

    :param handler_response: response data and method execution metadata
    :type handler_response: tuple(object, dict)
    :param rtime: request timestamp (when processing started)
    :type rtime: float
    :param rtype: requested content type (default: 'json')
    :type rtype: str
    :param pretty: if JSON should be pretty-formatted (default: False)
    :type pretty: bool

    :returns: status, response
    :rtype: tuple(int, str)
    """
    data, metadata = handler_response
    status = metadata.pop('status', 200)
    if status/100 != 2:
        # Some error occured, so we need to return error info,
        # not the request results
        rtype = 'json'
    if rtype == 'json':
        if status/100 == 2:
            str_status = 'OK'
        else:
            str_status = 'failed'
        result = {'status': str_status}
        result.update(metadata)
        if data:
            result['data'] = data
        if rtime and result.get('took_total_ms') is None:
            result['took_total_ms'] = int((time.time() - rtime)*1000)
        indent = None
        newline = ''
        if pretty:
            indent = 2
            newline = '\n'
        result = json.dumps(result, indent=indent).encode('utf-8') + newline
    elif rtype == 'img':
        try:
            result = data['img']
        except KeyError:
            msg = "Method does not support chosen format ('%s')" % rtype
            raise MethodException(None, msg)
    else:
        if rtype in RESPONSE_TYPE:
            raise DkbApiNotImplemented("Response type '%s' is not implemented"
                                       " yet" % rtype)
        # Incorrect rtype should already be detected in `dkb_app()`,
        # so just in case that something went wrong...
        raise InvalidArgument(None, ('rtype', rtype, RESPONSE_TYPE.keys()))
    return status, result


def dkb_app(environ, start_response):
    rtime = time.time()
    path = environ.get('SCRIPT_NAME')
    logging.debug('REQUEST: %s' % path)
    params = parse_params(environ.get('QUERY_STRING', ''))
    logging.debug('ALL PARAMS: %s' % params)
    # Remove ``pretty`` from parameters,
    # for it should not be passed to the method handler
    pretty = params.pop('pretty', False)
    # Parameter ``rtype`` is to be passed to some methods,
    # so left in the params
    rtype = params.get('rtype', 'json')
    logging.debug('METHOD PARAMS: %s' % params)
    try:
        if rtype not in RESPONSE_TYPE:
            raise InvalidArgument(None, ('rtype', rtype, RESPONSE_TYPE.keys()))
        handler = methods.handler(path)
        response = handler(path, **params)
        status, result = construct_response(response, rtime, rtype, pretty)
    except Exception, err:
        error = methods.error_handler(sys.exc_info())
        rtype = 'json'
        status, result = construct_response(error, rtime, rtype, pretty)
    status_line = "%(code)d %(reason)s" % {
        'code': status,
        'reason': STATUS_CODES.get(status, 'Unknown')
    }
    start_response(status_line, [('Content-Type', RESPONSE_TYPE[rtype])])
    return [result]


if __name__ == '__main__':
    logging.info("Reading server configuration.")
    if not api.configure():
        logging.info("Stopping.")
        sys.exit(1)
    from flup.server.fcgi import WSGIServer
    logging.info("Starting server.")
    WSGIServer(dkb_app, bindAddress='%%SOCK%%', umask=0117).run()
