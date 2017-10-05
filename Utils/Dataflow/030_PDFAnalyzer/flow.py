#!/usr/bin/env python

"""
PDF Analyzer streaming interface
"""

import json
import os
import shutil
import subprocess
import sys
import traceback

from manager import cfg
from manager import path_join
from manager import Paper
from manager import re_pdfname

base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(base_dir))
import pyDKB


def process(stage, msg):
    """ Obtain the PDF name from the input.

    Input is expected to be a JSON containing a single dictionary with
    2 items:
      PDF - name of the PDF.
      dkbID - must be passed forward.
    """
    inp = msg.content()
    if "PDF" not in inp:
        sys.stderr.write("(ERROR) no PDF specified.\n")
        return False
    fname = inp["PDF"]

    # Process the PDF and export it.
    if fname.startswith("hdfs://"):
        hdfs = True
        fname = path_join(cfg["HDFS_PDF_DIR"], fname[len("hdfs://"):])
    else:
        hdfs = False
        fname = os.path.abspath(fname).replace("\\", "/")
        if not os.access(fname, os.F_OK):
            sys.stderr.write("(ERROR) No such file or directory:"
                             +fname+"\n")
            return False
    pdfname = re_pdfname.search(fname)
    if not pdfname:
        sys.stderr.write("(ERROR) File "+fname+" is not a pdf file.\n")
        return False
    else:
        pdfname = pdfname.group(1)
        dirname = path_join(cfg["WORK_DIR"], "%s_tmp" % pdfname)
        os.mkdir(dirname)
        try:
            if hdfs:
                command_list = cfg["HDFS_DOWNLOAD_COMMAND"].split() \
                               + [fname, dirname]
                subprocess.check_call(command_list, stderr=sys.stderr,
                                stdout=sys.stderr)
            else:
                shutil.copy(fname, dirname)
        except Exception as e:
            sys.stderr.write("(ERROR) Failed to copy file into temporary"
                             " directory\n")
            if hdfs:
                sys.stderr.write("(ERROR) hdfs download command:"
                                 +str(command_list)
                                 +"\n")
            sys.stderr.write("(ERROR) "+str(e)+"\n")
            shutil.rmtree(dirname)
            return False
        p = Paper(pdfname, dirname)
        outf = pdfname + ".json"
        p.mine_text()
        p.export(quick=True, outf=outf)
        p.delete()
        with open(outf, "r") as f:
            outp = json.load(f)
        os.remove(outf)

    # Construct the output.
    if "dkbID" in inp:
        outp["dkbID"] = inp["dkbID"]
    outMessage = pyDKB.dataflow.Message(pyDKB.dataflow.messageType.JSON)(outp)
    stage.output(outMessage)
    return True

if __name__ == "__main__":
    analyzer_stage = pyDKB.dataflow.stage.JSONProcessorStage()
    analyzer_stage.process = process

    exit_code = 0
    exc_info= None
    try:
        analyzer_stage.parse_args(sys.argv[1:])
        analyzer_stage.run()
    except (pyDKB.dataflow.DataflowException, RuntimeError), e:
        if str(e):
            sys.stderr.write("(ERROR) while running stage 30: %s\n" % e)
        else:
            exc_info = sys.exc_info()
        exit_code = 2
    except Exception:
        exc_info = sys.exc_info()
    finally:
        analyzer_stage.stop()

    if exc_info:
        trace = traceback.format_exception(*exc_info)
        for line in trace:
            sys.stderr.write("(ERROR) %s" % line)

    exit(exit_code)
