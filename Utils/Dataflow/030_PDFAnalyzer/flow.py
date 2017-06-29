#!/usr/bin/env python

import json
import os
import shutil
import subprocess
import sys

from manager import cfg
from manager import path_join
from manager import Paper
from manager import re_pdfname

import pyDKB

def process(stage, msg):
    # Obtain the PDF name from the input. Input is expected to be a JSON containing a single dictionary with 2 items:
    # PDF - name of the PDF.
    # dkbID - must be passed forward.
    inp = msg.content()
    if "PDF" not in inp:
        sys.stderr.write("Error: no PDF specified.")
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
            sys.stderr.write("No such file or directory:" + fname + "\n")
            return False
    pdfname = re_pdfname.search(fname)
    if not pdfname:
        sys.stderr.write("File " + fname + " is not a pdf file.\n")
        return False
    else:
        pdfname = pdfname.group(1)
        dirname = path_join(cfg["WORK_DIR"], "%s_tmp" % (pdfname))
        os.mkdir(dirname)
        try:
            if hdfs:
                command_list = cfg["HDFS_DOWNLOAD_COMMAND"].split() + [fname, dirname]
                subprocess.call(command_list, stderr = sys.stderr, stdout = sys.stderr)
            else:
                shutil.copy(fname, dirname)
        except Exception as e:
            sys.stderr.write("Failed to copy file into temporary directory")
            if hdfs:
                sys.stderr.write("hdfs download command:" + str(command_list))
            sys.stderr.write(str(e))
            shutil.rmtree(dirname)
            return False
        p = Paper(pdfname, dirname)
        outf = pdfname + ".json"
        p.mine_text()
        p.export(quick = True, outf = outf)
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
    stage = pyDKB.dataflow.stage.JSONProcessorStage()
    stage.process = process

    try:
        stage.parse_args(sys.argv[1:])
        stage.run()
    except (pyDKB.dataflow.DataflowException, RuntimeError), e:
        if str(e):
            sys.stderr.write("Error while running stage 30: %s\n" % e)
    finally:
        stage.stop()
    
