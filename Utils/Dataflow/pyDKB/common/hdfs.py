"""
Utils to interact with HDFS.
"""

import sys
import subprocess
import select
import os

from . import HDFSException

DEVNULL = open(os.path.devnull, "w")
DKB_HOME = "/user/DKB/"

def check_stderr(proc, timeout=None):
    """ Check STDERR of the subprocess and kill it if there`s something.

    If STDERR is closed, waits till the process ends.
    """
    if not isinstance(proc, subprocess.Popen):
        raise TypeError("proc must be an instance of subprocess.Popen")
    ready, _, _ = select.select((proc.stderr, ), (), (), timeout)
    if ready:
        err = proc.stderr.readline()
        if err:
            proc.kill()
            raise HDFSException(err)
    if not timeout:
            proc.wait()
    return proc.poll()

def makedirs(dirname):
    """ Try to create directory (with parents). """
    cmd = ["hadoop", "fs", "-mkdir", "-p", dirname]
    try:
        proc = subprocess.Popen(cmd,
                                stdin=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                stdout=DEVNULL)
        if check_stderr(proc):
            raise(subprocess.CalledProcessError(proc.returncode, cmd))
    except (subprocess.CalledProcessError, OSError, HDFSException), err:
        raise RuntimeError("Failed to create HDFS directory: %s\n"
                           "Error message: %s\n" % (dirname, err))

def putfile(fname, dest):
    """ Upload file to HDFS. """
    cmd = ["hadoop", "fs", "-put", fname, dest]
    try:
        proc = subprocess.Popen(cmd,
                                stdin=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                stdout=DEVNULL)
        if check_stderr(proc):
            raise(subprocess.CalledProcessError(proc.returncode, cmd))
    except (subprocess.CalledProcessError, OSError, HDFSException), err:
        raise RuntimeError("Failed to put file to HDFS: %s\n"
                           "Error message: %s\n" % (fname, err))

def getfile(fname):
    """ Download file from HDFS.

    Return value: file name (without directory)
    """
    cmd = ["hadoop", "fs", "-get", fname]
    name = os.path.basename(fname)
    try:
        proc = subprocess.Popen(cmd,
                                stdin=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                stdout=DEVNULL)
        if check_stderr(proc):
            raise(subprocess.CalledProcessError(proc.returncode, cmd))
    except (subprocess.CalledProcessError, OSError, HDFSException), err:
        raise RuntimeError("Failed to get file from HDFS: %s\n"
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
                              stdin=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              stdout=subprocess.PIPE)
        while proc.poll() == None:
            timeout = 0.1
            check_stderr(proc, timeout)
            ready, _, _ = select.select((proc.stdout, ), (), (), timeout)
            if ready:
                out.append(proc.stdout.readline().strip())
        if proc.poll():
            raise subprocess.CalledProcessError(proc.returncode, cmd)
    except (subprocess.CalledProcessError, OSError, HDFSException), err:
        sys.stderr.write("(ERROR) Can not list the HDFS directory: %s\n"
                         "Error message: %s\n" % (dirname, err))
        return []

    # Parse output of `ls`:
    # {{{
    # Found 3 items
    # -rwxrwx---   3 $user        $group 1114404 2016-09-28 16:11 /path/to/file1
    # -rwxrwx---   3 $user        $group 1572867 2016-09-28 16:11 /path/to/file2
    # drwxrwx---   - $user        $group       0 2017-05-22 14:07 /path/to/subdir
    # }}}

    subdirs, files = [], []
    for line in out:
        line = line.split(None, 7)
        if len(line) != 8:
            continue

        # We need to return only the name of the file or subdir
        filename = line[7]
        filename = os.path.basename(filename)
        if line[0][0] == 'd':
            subdirs.append(filename)
        elif line[0][0] == '-':
            files.append(filename)

    if mode == 'a':
        result = subdirs + files
    elif mode == 'f':
        result = files
    elif mode == 'd':
        result = subdirs

    return result
