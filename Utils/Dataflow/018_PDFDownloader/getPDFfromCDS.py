#!/usr/bin/env python

"""
Stage 018: download PDF files from CDS and upload them to HDFS
"""

import sys
from urlparse import urlparse
import subprocess
import warnings
from requests.packages.urllib3.exceptions import InsecurePlatformWarning

sys.path.append("../")

import pyDKB
from pyDKB.dataflow import DataflowException
from pyDKB.dataflow.stage import JSONProcessorStage
from pyDKB.dataflow import CDSInvenioConnector, KerberizedCDSInvenioConnector
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

def get_recid(item):
    """ Get CDS record ID. """
    url = urlparse(item.get('url', ''))
    if not url.netloc in ("cds.cern.ch", "cdsweb.cern.ch"):
        return None
    s = url.path.split('/')
    if len(s) < 3 or not s[2].isdigit():
        return None
    return s[2]

def get_url(item, cds):
    """ Get URL of the document`s PDF from CDS. """
    recid = get_recid(item)
    if not recid:
        return None
    cds_results = cds.get_record(recid)
    if not cds_results:
        return None
    # use ['8564_u'] instead of .get('8564_u') as it is a
    # invenio_client.connector.Record and in list-dict terms the structure
    # looks this way:
    # { ..., "8564_": [{"y": ["Description", ...], "u": ["url", ...]}], ...}
    urls = cds_results[0]['8564_u']
    for url in urls:
        if url.split('.')[-1].lower() == "pdf":
            return url
    return None

def process(stage, msg):
    """ Message processing function.

    Input message: JSON
    Output message: JSON
    """
    data = msg.content()
    ARGS = stage.ARGS
    for item in data.get('supporting_notes', []):
        dkbID = item.get('dkbID')
        url = get_url(item.get('GLANCE', {}), ARGS.cds)
        if not (dkbID and url):
            continue
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

    stage.add_argument("-l", "--login", action="store", type=str, nargs='?',
                       help="CERN account login",
                       default='',
                       const='',
                       metavar="LOGIN",
                       dest='login'
                      )
    stage.add_argument("-p", "--password", action="store", type=str, nargs='?',
                       help="CERN account password",
                       default='',
                       const='',
                       metavar="PASSWD",
                       dest='password'
                      )
    stage.add_argument("-k", "--kerberos", action="store", type=bool, nargs='?',
                       help="Use Kerberos-based authentification",
                       default=False,
                       const=True,
                       metavar="KERBEROS",
                       dest='kerberos'
                      )
    stage.process = process

    exit_code = 0
    try:
        stage.parse_args(args)

        if not stage.ARGS.login and not stage.ARGS.kerberos:
            sys.stderr.write("WARNING: no authentication method"
                             " will be used.\n")

        warnings.simplefilter("once", InsecurePlatformWarning)
        ARGS = stage.ARGS

        if ARGS.kerberos:
            Connector = KerberizedCDSInvenioConnector
        else:
            Connector = CDSInvenioConnector

        with Connector(ARGS.login, ARGS.password) as cds:
            ARGS.cds = cds
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
