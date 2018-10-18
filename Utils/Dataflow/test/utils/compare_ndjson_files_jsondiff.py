#!/usr/bin/env python


import sys
import ndjson
import jsondiff


def diff_json(old, new):
    """Returns a diff of two JSON files in a dictionary format.

    Just a wrapper of ndjson.diff"""
    with open(old) as f1:
        j1 = ndjson.load(f1)

    with open(new) as f2:
        j2 = ndjson.load(f2)

    return jsondiff.diff(j1, j2)


def make_report(diff):
    """Turns results of jsondiff.diff into a readable format.

    Result is a dictionary where keys are string representations of changes, and values are
    number of times such changes occured in the diff."""
    report = {}
    for v in diff.values():
        if str(v) in report:
            report[str(v)] = report[str(v)] + 1
        else:
            report[str(v)] = 1
    return report


if __name__ == "__main__":
    sys.stderr.write(str(make_report(diff_json(*sys.argv[1:]))))



