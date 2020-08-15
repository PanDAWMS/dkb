"""
Implementation of "readline"-like functionality for custom separator.

.. todo:: make import of ``fcntl`` (or of this module) optional
  to avoid errors when library is used under Windows.
"""

import select
import os
import fcntl


def custom_readline(f, newline, markers={}):
    """ Read lines with custom line separator.

    Construct generator with readline-like functionality:
    with every call of ``next()`` method it will read data from ``f``
    until the ``newline`` separator is found; then yields what was read.
    If ``markers`` are supplied, then check for their presence: markers
    are special strings that can occur:
    - At the beginning of ``f``.
    - Immediately after a ``newline``.
    - Immediately after another marker.
    If a marker's value is found in such place, its name is yielded instead
    of another chunk of text and the value is removed. Markers in other
    places are ignored.

    .. warning:: the last line can be incomplete, if the input data flow
      is interrupted in the middle of data writing.

    To check if iteration is not over without reading next value, one may
    `send(True)` to the generator: it will return `True` if there is another
    message to yield or raise `StopIteration` if nothing left.

    :param f: readable file object
    :type f: file
    :param newline: delimeter to be used instead of ``\\n``
    :type newline: str
    :param markers: markers to look for, {name:value}
    :type markers: dict

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
    # Flag variable to say send() from next()
    send_not_next = None
    while True:
        if poller.poll(500):
            chunk = f.read()
            if not chunk:
                if buf:
                    if markers:
                        # Look for markers after last newline.
                        look_for_markers = True
                        while look_for_markers:
                            look_for_markers = False
                            for name, value in markers.iteritems():
                                if buf.startswith(value):
                                    buf = buf[len(value):]
                                    look_for_markers = True
                                    while send_not_next:
                                        send_not_next = yield True
                                    send_not_next = yield name
                                    break
                    while send_not_next:
                        # If we are here, the source is not empty for sure:
                        # we have another message to yield
                        send_not_next = yield True
                    yield buf
                break
            buf += chunk
        if send_not_next:
            # We keep on reading the stream, so it is not closed yet
            # and (in theory) may provide another message sooner or later
            send_not_next = yield True
        while newline in buf:
            if markers:
                # Look for markers before each yielded "line".
                # This includes start of f.
                look_for_markers = True
                while look_for_markers:
                    look_for_markers = False
                    for name, value in markers.iteritems():
                        if buf.startswith(value):
                            buf = buf[len(value):]
                            look_for_markers = True
                            while send_not_next:
                                send_not_next = yield True
                            send_not_next = yield name
                            break
            pos = buf.index(newline) + len(newline)
            while send_not_next:
                # If we are here, the source is not empty for sure:
                # we have another message to yield
                send_not_next = yield True
            send_not_next = yield buf[:pos]
            buf = buf[pos:]
