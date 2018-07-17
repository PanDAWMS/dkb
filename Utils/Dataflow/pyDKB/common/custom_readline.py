import select
import os
import fcntl


def custom_readline(f, newline):
    """Custom readline() function.

    Custom_readline() separates content from a text file 'f'
    by delimiter 'newline' to distinct messages.
    The last line can be incomplete, if the input data flow is interrupted
    in the middle of data writing.

    To check if iteration is over without reading next value, one may
    `send(True)` to the generator.

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
                if (yield buf):
                    # `send(True)` was called
                    is_empty = True
                    while (yield is_empty):
                        pass
                break
            buf += chunk
        while newline in buf:
            pos = buf.index(newline)
            if (yield buf[:pos]):
                # `send(True)` was called
                is_empty = (pos + len(newline) == len(buf))
                while (yield is_empty):
                    pass
            buf = buf[pos + len(newline):]
