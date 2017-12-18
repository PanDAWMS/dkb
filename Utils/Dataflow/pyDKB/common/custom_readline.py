def custom_readline(f, newline):
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
