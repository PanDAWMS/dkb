#!/usr/bin/env python

import logging

logging.basicConfig(format="%(asctime)s (%(levelname)s) %(message)s",
                    level=logging.DEBUG)


def dkb_app(environ, start_response):
    path = environ.get('SCRIPT_NAME')
    logging.debug('REQUEST: %s' % path)
    start_response('200 OK', [('Content-Type', 'text/plain')])
    return ['DKB API server. Status: WIP\n']


if __name__ == '__main__':
    from flup.server.fcgi import WSGIServer
    logging.info("Starting server.")
    WSGIServer(dkb_app, bindAddress='%%SOCK%%', umask=0117).run()
