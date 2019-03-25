#!/usr/bin/env python


import sys
import ndjson
import jsondiff


def diff_json(old, new):
    """Returns a diff of two JSON files in a dictionary format.

    Just a wrapper of ndjson.diff with pre-sorting ndjson records"""
    with open(old) as f1:
        j1 = ndjson.load(f1)

    with open(new) as f2:
        j2 = ndjson.load(f2)

    j1.sort(key=lambda x: str(x))
    j2.sort(key=lambda x: str(x))

    return jsondiff.diff(j1, j2, syntax='explicit')


def make_report(diff):
    """Turns results of jsondiff.diff into a readable format.

    Result is a string with a report summary.
    """
    report = {}
    for v in diff.values():
        if str(v) in report:
            report[str(v)] = report[str(v)] + 1
        else:
            report[str(v)] = 1

    events = [list(v.keys())[0] for v in diff.values()]
    inserted = len([v for v in events if v == jsondiff.insert])
    deleted = len([v for v in events if v == jsondiff.delete])
    updated = len([v for v in events if v == jsondiff.update])

    rep_str = """Records summary:
  Changed: {}
  Deleted: {}
  Added: {}

Details:
""".format(updated, deleted, inserted)

    for v in report:
        rep_str = "  " + report_str + v + ": " + str(report[v]) + " records\n"

    return rep_str


if __name__ == "__main__":
    sys.stderr.write(str(make_report(diff_json(*sys.argv[1:]))))