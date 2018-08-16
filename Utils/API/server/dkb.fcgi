#!/usr/bin/env python

def dkb_app(environ, start_response):
    start_response('200 OK', [('Content-Type', 'text/plain')])
    return ['DKB API server. Status: WIP\n']


if __name__ == '__main__':
    from flup.server.fcgi import WSGIServer
    WSGIServer(dkb_app, bindAddress='%%SOCK%%', umask=0117).run()
