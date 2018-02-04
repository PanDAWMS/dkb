import select
import sys
import os
import fcntl

poller = select.poll()
poller.register(sys.stdin, select.POLLIN)
flags = fcntl.fcntl(sys.stdin.fileno(), fcntl.F_GETFL)
fcntl.fcntl(sys.stdin.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)


def custom_readline(f, newline):
    """Custom readline() function. It separates content from a text file 'f'
    by delimiter 'newline' to distinct messages.
    The last line can be incomplete, if the input data flow is interrupted
    in the middle of data writing.
    """
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
