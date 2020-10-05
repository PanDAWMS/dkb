#!/usr/bin/env python

import json
import datetime
import traceback
import os
import sys
import logging

base_dir = os.path.dirname(os.path.abspath(__file__))
cfg_dir = os.path.join(base_dir, os.pardir, os.pardir)
lib_dir = os.path.join(cfg_dir, 'lib', 'dkb')

try:
    sys.path.append(lib_dir)
    import api
except ImportError:
    logging.error("Failed to import 'api' library module from: %s"
                  % lib_dir)

api.CONFIG_DIR = cfg_dir
api.configure()

tests = {
    'Chain data': {
        'path': '/task/chain',
        'params': {'tid': 21774573}
    },
    'Histogram': {
        'path': '/task/hist',
        'params': {'htags': 'returnofrpvllreprocessingdata16',
                   'rtype': 'json',
                   'start': datetime.datetime(2020, 9, 1)}
    },
    'Kwsearch': {
        'path': '/task/kwsearch',
        'params': {'kw': ['mc16e', '346899', 'gingrich'],
                   'timeout': 60, 'production': True,
                   'analysis': False}
    },
    'Derivation statistics': {
        'path': '/task/deriv',
        'params': {'project': 'mc16_13TeV', 'amitag': 'r11748'}
    },
    'Campaign statistics': {
        'path': '/campaign/stat',
        'params': {'events_src': 'all',
                   'htag': 'returnofrpvllreprocessingdata16',
                   'step_type': 'ctag_format'}
    },
    'Step statistics': {
        'path': '/step/stat',
        'params': {'htag': 'returnofrpvllreprocessingdata16',
                   'step_type': 'ctag_format'}
    }
}


def test(cfg, nested=False):
    test_type = 'Nested' if nested else 'Default'
    print '-----\n%s\n-----' % test_type
    path = '/nested' * nested + cfg['path']
    params = cfg['params']
    data, metadata = api.methods.handler(path)(path, **params)
    try:
        data = json.dumps(data, indent=2)
    except Exception:
        pass
    print "Data: %s\n" % data
    print "Meta: %s\n" % json.dumps(metadata, indent=2)


for t in tests:
    print '\n=== %s ===>' % t
    for nested in (False, True):
        try:
            test(tests[t], nested)
        except Exception, err:
            traceback.print_exc()
    print '<=== %s ===' % t
