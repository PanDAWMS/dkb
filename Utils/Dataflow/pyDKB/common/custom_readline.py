def custom_readline(f, newline):
    """Custom readline() function. It separates content from a text file 'f'
    by delimiter 'newline' to distinct messages.
    The last line can be incomplete, if the input data flow is interrupted
    in the middle of data writing.
    """
    buf = ""
    max_buf_size = 1
    while True:
        while newline in buf:
            pos = buf.index(newline)
            yield buf[:pos]
            buf = buf[pos + len(newline):]
        chunk = f.read(max_buf_size)
        if not chunk:
            yield buf
            break
        buf += chunk
