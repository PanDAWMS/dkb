"""
pyDKB.common.utils

Miscellaneous useful functions.

.. todo:: make import of ``fcntl`` (or of this module) optional
  to avoid errors when library is used under Windows.
"""

import select
import os
import fcntl


def custom_readline(f, newline):
    """ Read lines with custom line separator.

    Construct generator with readline-like functionality:
    with every call of ``next()`` method it will read data from ``f``
    untill the ``newline`` separator is found; then yields what was read.

    .. warning:: the last line can be incomplete, if the input data flow
      is interrupted in the middle of data writing.

    :param f: readable file object
    :type f: file
    :param newline: delimeter to be used instead of ``\\n``
    :type newline: str

    :return: iterable object
    :rtype: generator

    .. todo::
      * make last "line" handling more strict: no ``newline`` == no line;
      * rethink function name (as "line" is actually a "message");
      * move functionality to ``pyDKB.dataflow.communication`` [1]_
        submodule)

    .. [1] https://github.com/PanDAWMS/dkb/pull/129
    """
    poller = select.poll()
    poller.register(f, select.POLLIN)
    flags = fcntl.fcntl(f.fileno(), fcntl.F_GETFL)
    fcntl.fcntl(f.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)
    buf = ""
    while True:
        if poller.poll(500):
            chunk = f.read()
            if not chunk:
                yield buf
                break
            buf += chunk
        while newline in buf:
            pos = buf.index(newline)
            yield buf[:pos]
            buf = buf[pos + len(newline):]


def read_es_config(cfg_file):
    """ Read ES configuration file.

    We have ES config in form of file with shell variables declaration,
    but sometimes need to parse it in Python as well.

    :param cfg_file: open file descriptor with ES access configuration
    :type cfg_file: file descriptor
    """
    keys = {'ES_HOST': 'host',
            'ES_PORT': 'port',
            'ES_USER': 'user',
            'ES_PASSWORD': '__passwd',
            'ES_INDEX': 'index'
            }
    cfg = {}
    for line in cfg_file.readlines():
        if line.strip().startswith('#'):
            continue
        line = line.split('#')[0].strip()
        if '=' not in line:
            continue
        key, val = line.split('=')[:2]
        try:
            cfg[keys[key]] = val
        except KeyError:
            sys.stderr.write("(WARN) Unknown configuration parameter: "
                             "'%s'.\n" % key)
    return cfg
