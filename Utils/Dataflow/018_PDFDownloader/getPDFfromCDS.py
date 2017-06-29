#!/usr/bin/env python

"""
Stage 018: download PDF files from CDS and upload them to HDFS
"""

import sys
from urlparse import urlparse
import subprocess

sys.path.append("../")

import pyDKB
from pyDKB.dataflow import DataflowException
from pyDKB.dataflow.stage import JSONProcessorStage
from pyDKB.common import hdfs, HDFSException

def transfer(url, hdfs_name):
    """ Download file from given URL and upload to HDFS. """
    cmd = ["./transferPDF.sh", url, hdfs_name]
    try:
        sp = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   stdout=subprocess.PIPE)
        if hdfs.check_stderr(sp):
            raise subprocess.CalledProcessError(proc.returncode, cmd)
        out = sp.stdout.readline()
        if out:
            out = out.strip()
        return out
    except (subprocess.CalledProcessError, OSError, HDFSException), err:
        sys.stderr.write("Failed to transfer data from CDS to HDSF: %s\n"
                         % err)
        return None

def get_url(item):
    """ Get URL of the document`s PDF from CDS data. """
    result = None
    for f in item.get('files', []):
        url = f.get('url')
        if not url:
            continue
        desc = f.get('description', '')
        v = -1
        if url.split('.')[-1].lower() == "pdf" \
          and (desc == None or desc.lower().find("fulltext") >= 0):
            if f.get('version') and f['version'] > v:
                v = f['version']
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
    try:
        stage.parse_args(args)
        stage.run()
    except (DataflowException, RuntimeError), err:
        if str(err):
            sys.stderr.write("(ERROR) %s\n" % err)
        exit_code = 1
    finally:
        stage.stop()

    exit(exit_code)

if __name__ == "__main__":
    main(sys.argv[1:])
