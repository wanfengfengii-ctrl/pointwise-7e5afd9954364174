"""公告原文件采集入口：仅装配控制会话与正文交付并兼容转发。"""
import socket

from control import ControlSession
from delivery import transfer
from errors import ProtocolError, TransferError

__all__ = ["Session", "collect", "ProtocolError", "TransferError"]


class Session:
    """兼容旧入口：公开属性与 fetch/close 均转发，不留执行链。"""

    def __init__(self, address, trace=None, connect=socket.create_connection):
        self._control = ControlSession(address, trace, connect)

    @property
    def trace(self):
        return self._control.trace

    @property
    def closed(self):
        return self._control.closed

    @property
    def busy(self):
        return self._control.busy

    def fetch(self, path, sink):
        return transfer(self._control, path, sink, self._control.trace)

    def close(self):
        self._control.close()


def collect(address, jobs, trace=None, connect=socket.create_connection):
    session = Session(address, trace, connect)
    try:
        return [session.fetch(path, sink) for path, sink in jobs]
    finally:
        session.close()
