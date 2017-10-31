#!/usr/bin/env python

"""
Stage 018: download PDF files from CDS and upload them to HDFS
"""

import sys
import os
from urlparse import urlparse
import subprocess

import traceback

sys.path.append("../")

import pyDKB
from pyDKB.dataflow import DataflowException
from pyDKB.dataflow.stage import JSONProcessorStage
from pyDKB.common import hdfs, HDFSException

def transfer(url, hdfs_name):
    """ Download file from given URL and upload to HDFS. """
    base_dir = os.path.dirname(__file__)
    cmd = [os.path.join(base_dir, "transferPDF.sh"), url, hdfs_name]
    try:
        sp = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   stdout=subprocess.PIPE)
        if hdfs.check_stderr(sp):
            raise subprocess.CalledProcessError(sp.returncode, cmd)
        out = sp.stdout.readline()
        if out:
            out = out.strip()
        return out
    except (subprocess.CalledProcessError, OSError, HDFSException), err:
        sys.stderr.write("(ERROR) Failed to transfer data from CDS to HDSF:"
                         " %s\n" % err)
        return None

def get_url(item):
    """ Get URL of the document`s PDF from CDS data. """
    result = None
    for f in item.get('files', []):
        url = f.get('url')
        if not url:
            continue
        desc = f.get('description', None)
        v = -1
        if url.split('.')[-1].lower() == "pdf" \
          and (desc == None or desc.lower().find("fulltext") >= 0):
            if f.get('version', None) != None and f['version'] > v \
              or v < 0:
                v = f.get('version', v)
                result = url
    return result

def process(stage, msg):
    """ Message processing function.

    Input message: JSON
    Output message: JSON
    """
    data = msg.content()
    ARGS = stage.ARGS
    urls = []
    for item in data.get('supporting_notes', []):
        dkbID = item.get('dkbID')
        url = get_url(item.get('CDS', {}))
        if not (dkbID and url and url not in urls):
            continue
        urls.append(url)
        hdfs_location = transfer(url, dkbID + ".pdf")
        if not hdfs_location:
            continue
        out_data = {'dkbID': dkbID, 'PDF': "hdfs://" + hdfs_location}
        out_msg = stage.output_message_class()(out_data)
        stage.output(out_msg)
    return True

def main(args):
    """ Main function. """
    stage = JSONProcessorStage()
    stage.process = process

    exit_code = 0
    exc_info = None
    try:
        stage.parse_args(args)
        stage.run()
    except (DataflowException, RuntimeError), err:
        if str(err):
            sys.stderr.write("(ERROR) %s\n" % err)
        else:
            exc_info = sys.exc_info()
        exit_code = 2
    except Exception:
        exc_info = sys.exc_info()
        exit_code = 1
    finally:
        stage.stop()

    if exc_info:
        trace = traceback.format_exception(*exc_info)
        for line in trace:
            sys.stderr.write("(ERROR) %s" % line)

    exit(exit_code)

if __name__ == "__main__":
    main(sys.argv[1:])
