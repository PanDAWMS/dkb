#!/usr/bin/env python

import json
import sys


def get_uid_key(data):
    possible_keys = ['taskid', '_id', 'datasetname']
    for key in possible_keys:
        if key in data:
            return key


def main(file1, file2):
    UID_KEY = None
    r1 = {}
    with open(file1, 'r') as f:
        line = f.readline()
        while line:
            parsed = json.loads(line)
            if parsed.keys() == ['index']:
                line = f.readline()
                continue
            if not UID_KEY:
                UID_KEY = get_uid_key(parsed)
            r1[parsed[UID_KEY]] = parsed
            line = f.readline()
    with open(file2, 'r') as f:
        line = f.readline()
        while line:
            parsed = json.loads(line)
            if parsed.keys() == ['index']:
                line = f.readline()
                continue
            uid = parsed[UID_KEY]
            if not r1.get(uid):
                sys.stderr.write("Record missed in %s (uid=%s)\n"
                                 % (file1, uid))
                line = f.readline()
                continue
            stored = r1.pop(uid)
            if stored != parsed:
                sys.stderr.write("Record seem to differ for uid=%s\n" % uid)
                for key in stored.keys():
                    v1 = stored.pop(key)
                    try:
                        v2 = parsed.pop(key)
                    except KeyError:
                        sys.stderr.write("Item missed in (2): '%s'\n" % key)
                        continue
                    if v1 != v2:
                        header1 = "key = %s:\n" % key
                        if type(v1) is not list:
                            v1 = [v1]
                        if type(v2) is not list:
                            v2 = [v2]
                        header2 = "Items missed in (2):\n"
                        for v in v1:
                            if v not in v2:
                                sys.stderr.write("%s%s(1) %s\n"
                                                 % (header1, header2, v))
                                header1 = ''
                                header2 = ''
                        header2 = "Items missed in (1):\n"
                        for v in v2:
                            if v not in v1:
                                sys.stderr.write("%s%s(2) %s\n"
                                                 % (header1, header2, v))
                                header1 = ''
                                header2 = ''
                for key in parsed:
                    sys.stderr.write("Key missed in (1): '%s'\n" % key)
            line = f.readline()
    for uid in r1:
        sys.stderr.write("Record missed in %s (uid=%s)\n" % (file2, uid))


if __name__ == '__main__':
    main(*sys.argv[1:])
