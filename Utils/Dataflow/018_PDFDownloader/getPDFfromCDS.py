#!/usr/bin/env python

"""
Stage 018: download PDF files from CDS and upload them to HDFS
"""

import sys
import os
from urlparse import urlparse
import subprocess

import traceback

try:
    base_dir = os.path.dirname(__file__)
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
    from pyDKB.dataflow import DataflowException
    from pyDKB.dataflow.stage import ProcessorStage
    from pyDKB.dataflow import messageType
    from pyDKB.common import hdfs, HDFSException
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


_fails_in_row = 0
_fails_max = 3


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
                and (desc is None or desc.lower().find("fulltext") >= 0):
            if f.get('version', None) is not None and f['version'] > v \
                    or v < 0:
                v = f.get('version', v)
                result = url
    return result


def process(stage, msg):
    """ Message processing function.

    Input message: JSON
    Output message: JSON
    """
    global _fails_in_row
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
            _fails_in_row += 1
            continue
        _fails_in_row = 0
        out_data = {'dkbID': dkbID, 'PDF': "hdfs://" + hdfs_location}
        out_msg = stage.output_message_class()(out_data)
        stage.output(out_msg)
    if _fails_in_row > _fails_max:
        raise DataflowException("Failed to transfer PDF from CDS to HDFS"
                                " %s (>=%s) times in a row"
                                % (_fails_in_row, _fails_max))
    return True


def main(args):
    """ Main function. """
    stage = ProcessorStage()
    stage.set_input_message_type(messageType.JSON)
    stage.set_output_message_type(messageType.JSON)

    stage.process = process

    stage.parse_args(args)
    exit_code = stage.run()

    if exit_code == 0:
        stage.stop()

    exit(exit_code)


if __name__ == "__main__":
    main(sys.argv[1:])
