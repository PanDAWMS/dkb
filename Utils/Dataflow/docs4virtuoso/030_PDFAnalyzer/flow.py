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

try:
    base_dir = os.path.dirname(__file__)
    dkb_dir = os.path.join(base_dir, os.pardir)
    sys.path.append(dkb_dir)
    import pyDKB
except Exception, err:
    sys.stderr.write("(ERROR) Failed to import pyDKB library: %s\n" % err)
    sys.exit(1)


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
                             + fname + "\n")
            return False
    pdfname = re_pdfname.search(fname)
    if not pdfname:
        sys.stderr.write("(ERROR) File " + fname + " is not a pdf file.\n")
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
                                 + str(command_list)
                                 + "\n")
            sys.stderr.write("(ERROR) " + str(e) + "\n")
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
    outMessage = pyDKB.dataflow.communication.Message(pyDKB.dataflow
                                                      .messageType.JSON)(outp)
    stage.output(outMessage)
    return True


if __name__ == "__main__":
    analyzer_stage = pyDKB.dataflow.stage.ProcessorStage()
    analyzer_stage.set_input_message_type(pyDKB.dataflow.messageType.JSON)
    analyzer_stage.set_output_message_type(pyDKB.dataflow.messageType.JSON)
    analyzer_stage.process = process

    analyzer_stage.configure(sys.argv[1:])
    exit_code = analyzer_stage.run()

    if exit_code == 0:
        analyzer_stage.stop()

    exit(exit_code)
