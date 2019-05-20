"""
Utils to interact with HDFS.
"""

import sys
import subprocess
import select
import os
import posixpath as path
import tempfile

from . import HDFSException

DEVNULL = open(os.path.devnull, "w")
DKB_HOME = "/user/DKB/"


def check_stderr(proc, timeout=None, max_lines=1):
    """ Wait till the end of the subprocess and send its STDERR to STDERR.

    Output only MAX_LINES of the STDERR to the current STDERR;
    if MAX_LINES == None, output all the STDERR.

    Return value is the subprocess` return code.
    """
    if not isinstance(proc, subprocess.Popen):
        raise TypeError("proc must be an instance of subprocess.Popen")
    n_lines = 0
    while proc.poll() is None:
        ready, _, _ = select.select((proc.stderr, ), (), (), timeout)
        if ready:
            err = proc.stderr.readline()
            if err:
                n_lines += 1
                if max_lines is None or n_lines <= max_lines:
                    sys.stderr.write("(INFO) (proc) %s\n" % err)
    if proc.poll():
        raise subprocess.CalledProcessError(proc.returncode, None)
    return proc.poll()


def makedirs(dirname):
    """ Try to create directory (with parents). """
    cmd = ["hadoop", "fs", "-mkdir", "-p", dirname]
    try:
        proc = subprocess.Popen(cmd,
                                stdin=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                stdout=DEVNULL)
        check_stderr(proc)
    except (subprocess.CalledProcessError, OSError), err:
        if isinstance(err, subprocess.CalledProcessError):
            err.cmd = ' '.join(cmd)
        raise HDFSException("Failed to create HDFS directory: %s\n"
                            "Error message: %s\n" % (dirname, err))


def putfile(fname, dest):
    """ Upload file to HDFS. """
    cmd = ["hadoop", "fs", "-put", fname, dest]
    try:
        proc = subprocess.Popen(cmd,
                                stdin=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                stdout=DEVNULL)
        check_stderr(proc)
    except (subprocess.CalledProcessError, OSError), err:
        if isinstance(err, subprocess.CalledProcessError):
            err.cmd = ' '.join(cmd)
        raise HDFSException("Failed to put file to HDFS: %s\n"
                            "Error message: %s\n" % (fname, err))


def movefile(fname, dest):
    """ Move local file to HDFS. """
    if os.path.exists(fname):
        putfile(fname, dest)
        try:
            os.remove(fname)
        except OSError, err:
            sys.stderr.write("(WARN) Failed to remove local copy of HDFS file"
                             " (%s): %s" % (fname, err))


def getfile(fname):
    """ Download file from HDFS.

    Return value: file name (without directory)
    """
    cmd = ["hadoop", "fs", "-get", fname]
    name = basename(fname)
    try:
        proc = subprocess.Popen(cmd,
                                stdin=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                stdout=DEVNULL)
        check_stderr(proc)
    except (subprocess.CalledProcessError, OSError), err:
        if isinstance(err, subprocess.CalledProcessError):
            err.cmd = ' '.join(cmd)
        raise HDFSException("Failed to get file from HDFS: %s\n"
                            "Error message: %s\n" % (fname, err))
    return name


def File(fname):
    """ Get and open temporary local copy of HDFS file

    Return value: open file object (TemporaryFile).
    """
    cmd = ["hadoop", "fs", "-cat", fname]
    tmp_file = tempfile.TemporaryFile()
    try:
        proc = subprocess.Popen(cmd,
                                stdin=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                stdout=tmp_file)
        check_stderr(proc)
        tmp_file.seek(0)
    except (subprocess.CalledProcessError, OSError), err:
        if isinstance(err, subprocess.CalledProcessError):
            err.cmd = ' '.join(cmd)
        tmp_file.close()
        raise HDFSException("Failed to get file from HDFS: %s\n"
                            "Error message: %s\n" % (fname, err))
    if tmp_file.closed:
        return None

    return tmp_file


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
        while proc.poll() is None:
            timeout = 0.1
            check_stderr(proc, timeout)
            ready, _, _ = select.select((proc.stdout, ), (), (), timeout)
            if ready:
                out.append(proc.stdout.readline().strip())
        if proc.poll():
            raise subprocess.CalledProcessError(proc.returncode, ' '.join(cmd))
    except (subprocess.CalledProcessError, OSError), err:
        if isinstance(err, subprocess.CalledProcessError):
            err.cmd = ' '.join(cmd)
        raise HDFSException("Failed to list the HDFS directory: %s\n"
                            "Error message: %s\n" % (dirname, err))

    # Parse output of `ls`:
    # {{{
    # Found 3 items
    # -rwxrwx---   3 $user      $group 1114404 2016-09-28 16:11 /path/to/file1
    # -rwxrwx---   3 $user      $group 1572867 2016-09-28 16:11 /path/to/file2
    # drwxrwx---   - $user      $group       0 2017-05-22 14:07 /path/to/subdir
    # }}}

    subdirs, files = [], []
    for line in out:
        line = line.split(None, 7)
        if len(line) != 8:
            continue

        # We need to return only the name of the file or subdir
        filename = line[7]
        filename = basename(filename)
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


def basename(p):
    """ Return file name without path. """
    if p is None:
        p = ''
    return path.basename(p).strip()


def dirname(p):
    """ Return dirname without filename. """
    if p is None:
        p = ''
    return path.dirname(p).strip()


def join(p, filename):
    """ Join path and filename. """
    if p is None:
        p = ''
    if filename is None:
        filename = ''
    return path.join(p, filename).strip()
