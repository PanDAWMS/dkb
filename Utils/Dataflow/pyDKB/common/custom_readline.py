import select
import os
import fcntl
import sys


def custom_readline(f, newline):
    """Custom readline() function.

    Custom_readline() separates content from a text file 'f'
    by delimiter 'newline' to distinct messages.
    The last line can be incomplete, if the input data flow is interrupted
    in the middle of data writing.

    To check if iteration is not over without reading next value, one may
    `send(True)` to the generator: it will return `True` if there is another
    message to yield or raise `StopIteration` if nothing left.

    Keyword arguments:
    f -- file/stream to read
    newline -- custom delimiter
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
            pos = buf.index(newline) + len(newline)
            while send_not_next:
                # If we are here, the source is not empty for sure:
                # we have another message to yield
                send_not_next = yield True
            send_not_next = yield buf[:pos]
            buf = buf[pos:]
