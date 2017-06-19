"""
Utils to interact with HDFS.
"""

import sys
import subprocess
import select
import os

from . import HDFSException

DEVNULL = open(os.path.devnull, "w")

def getfile(fname):
    """ Download file from HDFS.

    Check if there already is a local version of the file and remove it.

    Return value: file name (without directory)
    """
    cmd = ["hadoop", "fs", "-get", fname]
    name = os.path.basename(fname)
    try:
        if os.access(name, os.F_OK):
            os.remove(name)
        subprocess.check_call(cmd, stderr=DEVNULL, stdout=DEVNULL)
    except (subprocess.CalledProcessError, OSError), err:
        raise RuntimeError("(ERROR) Failed to get file from HDFS: %s\n"
                           "Error message: %s\n" % (fname, err))
    return name

def listdir(dirname, mode='a'):
    """ List files and/or subdirectories of HDFS directory.

    Parameters:
        dirname -- directory to list
        mode    -- 'a': list all objects
                   'f': list files
                   'd': list subdirectories
    """
    cmd = ["hadoop", "fs", "-ls", dirname]
    out = []
    try:
        # Use PIPE for all the std* to avoid catching and/or blocking
        # current process std*
        proc = subprocess.Popen(cmd,
                              stdin =subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              stdout=subprocess.PIPE)
        while proc.poll() == None:
            ready, _, _ = select.select((proc.stdout, proc.stderr), (), (), .1)
            if proc.stdout in ready:
                out.append(proc.stdout.readline().strip())
            elif proc.stderr in ready:
                err = proc.stderr.readline()
                if err:
                    proc.kill()
                    raise HDFSException(err)
        if proc.poll():
            raise subprocess.CalledProcessError(proc.returncode, cmd)
    except (subprocess.CalledProcessError, OSError, HDFSException), err:
        sys.stderr.write("(ERROR) Can not list the HDFS directory: %s\n"
                         "Error message: %s\n" % (dirname, err))
        return []

    # Parse output of `ls`
    subdirs, files = [], []
    for line in out:
        line = line.split(None, 7)
        if len(line) != 8:
            continue

        # We need to return only the name of the file or subdir
        line[7] = os.path.basename(line[7])
        if line[0][0] == 'd':
            subdirs.append(line[7])
        elif line[0][0] == '-':
            files.append(line[7])

    if mode == 'a':
        result = subdirs + files
    elif mode == 'f':
        result = files
    elif mode == 'd':
        result = subdirs

    return result
