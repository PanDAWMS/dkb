# -*- coding: utf-8 -*-

import os, os.path, shutil, subprocess, sys

from manager import cfg, re_pdfname
from manager import path_join
from manager import Paper

if __name__ == "__main__":
    while True:
        fname = sys.stdin.readline().strip()
        if not fname:
            break
        if fname.startswith("hdfs:"):
            hdfs = True
            fname = path_join(cfg["HDFS_PDF_DIR"], fname[len("hdfs:"):])
        else:
            hdfs = False
            fname = os.path.abspath(fname).replace("\\", "/")
            if not os.access(fname, os.F_OK):
                sys.stderr.write("No such file or directory:" + fname + "\n")
                continue
        pdfname = re_pdfname.search(fname)
        if not pdfname:
            sys.stderr.write("File " + fname + " is not a pdf file.\n")
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
                sys.stderr.write(str(command_list))
                sys.stderr.write(str(e))
                shutil.rmtree(dirname)
                continue
            p = Paper(pdfname, dirname)
            outf = pdfname + ".json"
            p.mine_text()
            p.export(quick = True, outf = outf)
            p.delete()
            with open(outf, "r") as f:
                text = f.read() + "\0"
            os.remove(outf)
            sys.stdout.write(text)
            sys.stderr.write("\n")
