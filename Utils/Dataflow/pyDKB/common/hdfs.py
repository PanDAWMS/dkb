"""
Utils to interact with HDFS.
"""

import sys
import subprocess
import select

from . import HDFSException

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
        line[7] = line[7][len(dirname)+1:]
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
