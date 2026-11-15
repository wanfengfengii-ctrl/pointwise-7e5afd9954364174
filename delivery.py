"""正文交付：正文与下游生命周期、字节计数与首错保留。

只依赖会话端口（acquire/open/complete/abort/release/close），
不读控制连接、不解释回复码、不拼命令、不访问端口外的会话字段。
"""


def transfer(control, path, sink, trace):
    control.acquire(path)
    data = None
    stopped = False
    count = 0
    primary = None
    try:
        data = control.open()
        while True:
            chunk = data.recv(4)
            if not chunk:
                break
            trace.append(["body", chunk.hex()])
            answer = sink.write(chunk)
            count += len(chunk)
            trace.append(["write", count])
            if answer is False:
                stopped = True
                break
        channel, data = data, None
        trace.append(["data.close"])
        channel.close()
        if not stopped:
            control.complete()
            sink.finish(count)
            trace.append(["finish", count])
    except BaseException as exc:
        primary = exc
    # 先释放数据通道，再收尾控制回复，最后关闭下游；首错不被覆盖。
    if data is not None:
        trace.append(["data.close"])
        try:
            data.close()
        except BaseException as exc:
            if primary is None:
                primary = exc
    try:
        control.abort()
    except BaseException as exc:
        if primary is None:
            primary = exc
    trace.append(["sink.close"])
    try:
        sink.close()
    except BaseException as exc:
        if primary is None:
            primary = exc
    control.release()
    if primary is not None:
        raise primary
    return count
