import select
import os
import fcntl


def custom_readline(f, newline):
    """Custom readline() function.

    Custom_readline() separates content from a text file 'f'
    by delimiter 'newline' to distinct messages.
    The last line can be incomplete, if the input data flow is interrupted
    in the middle of data writing.

    Keyword arguments:
    f -- file/stream to read
    newline -- custom delimiter
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
                if buf:
                    yield buf
                break
            buf += chunk
        while newline in buf:
            pos = buf.index(newline)
            yield buf[:pos]
            buf = buf[pos + len(newline):]
