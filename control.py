"""控制线独占：命令发送、回复解析与归属、准入与销毁。"""
import re
import socket

from errors import ProtocolError, TransferError


class BodyChannel:
    """交出的正文通道：仅 recv(size) 与 close()。"""

    def __init__(self, raw):
        self._raw = raw

    def recv(self, size):
        return self._raw.recv(size)

    def close(self):
        self._raw.close()


class ControlSession:
    """独占控制 socket 与回复归属；不接触下游回调，不消费正文。"""

    def __init__(self, address, trace=None, connect=socket.create_connection):
        self.trace = [] if trace is None else trace
        self._connect = connect
        self._closed = False
        self._busy = False
        self._pending = False
        self._path = None
        self._ctrl = connect(address)
        self._ctrl.settimeout(3)
        self._reader = self._ctrl.makefile("rb")
        try:
            if self._reply()[0] != 220:
                raise ProtocolError("greeting")
        except BaseException:
            self._break()
            raise

    @property
    def closed(self):
        return self._closed

    @property
    def busy(self):
        return self._busy

    def _break(self):
        if not self._closed:
            self._closed = True
            self.trace.append(["broken"])
            try:
                self._reader.close()
            finally:
                self._ctrl.close()

    def _send(self, command):
        self.trace.append(["send", command])
        self._ctrl.sendall(command.encode("ascii") + b"\r\n")

    def _reply(self):
        line = self._reader.readline(4097)
        if (len(line) > 4096 or len(line) < 6 or
                not line.endswith(b"\r\n") or
                not line[:3].isdigit() or line[3:4] != b" "):
            raise ProtocolError("reply framing")
        try:
            text = line[4:-2].decode("ascii")
        except UnicodeDecodeError:
            raise ProtocolError("reply ascii") from None
        code = int(line[:3])
        self.trace.append(["reply", code, text])
        return code, text

    def acquire(self, path):
        # 准入检查并持有 busy，记住本次路径；无 I/O，失败不持有。
        if self._closed:
            raise RuntimeError("closed")
        if self._busy:
            raise RuntimeError("busy")
        if not isinstance(path, str) or not path or any(
                ord(c) < 32 or ord(c) > 126 for c in path):
            raise ValueError("path")
        self._busy = True
        self._path = path

    def open(self):
        # 协商被动连接并取得 150；交出前的连接归本组件，失败由它释放。
        data = None
        try:
            self._send("EPSV")
            code, text = self._reply()
            match = re.fullmatch(
                r"Entering Extended Passive Mode \(\|\|\|([0-9]+)\|\)", text)
            if code != 229 or match is None:
                raise ProtocolError("passive")
            port = int(match[1])
            if not 0 < port < 65536:
                raise ProtocolError("passive port")
            data = self._connect((self._ctrl.getpeername()[0], port))
            data.settimeout(3)
            self._send("RETR " + self._path)
            self._pending = True
            code, _ = self._reply()
            if code == 550:
                self._pending = False
                raise TransferError("retrieve 550")
            if code != 150:
                raise ProtocolError("preliminary")
            return BodyChannel(data)
        except BaseException as exc:
            if data is not None:
                self.trace.append(["data.close"])
                try:
                    data.close()
                except BaseException:
                    pass
            if isinstance(exc, ProtocolError) or (
                    isinstance(exc, (OSError, EOFError)) and data is None):
                try:
                    self._break()
                except BaseException:
                    pass
            raise

    def complete(self):
        # 消费正常传输结尾；先结清归属再返回或抛传输错误。
        try:
            code, _ = self._reply()
            if code == 426:
                tail, _ = self._reply()
                if tail != 226:
                    raise ProtocolError("completion tail")
                self._pending = False
                raise TransferError("transfer 426")
            if code not in (226, 451):
                raise ProtocolError("completion")
            self._pending = False
            if code == 451:
                raise TransferError("transfer 451")
        except ProtocolError:
            try:
                self._break()
            except BaseException:
                pass
            raise

    def abort(self):
        # 对未结清传输执行收尾；已结清或已关闭时无副作用。
        if self._closed or not self._pending:
            return
        try:
            self._send("ABOR")
            self._send("NOOP")
            replies = []
            while True:
                code, _ = self._reply()
                if code == 200:
                    break
                replies.append(code)
                if len(replies) > 3 or code not in (225, 226, 426, 451):
                    raise ProtocolError("abort reply")
            if tuple(replies) not in (
                    (225,), (226,), (426, 226), (226, 225),
                    (426, 226, 225), (451, 225)):
                raise ProtocolError("abort sequence")
            self._pending = False
        except BaseException:
            try:
                self._break()
            except BaseException:
                pass
            raise

    def release(self):
        # 在下游关闭尝试结束后解除 busy；无 I/O。
        self._busy = False

    def close(self):
        if not self._closed:
            self._closed = True
            self.trace.append(["session.close"])
            try:
                self._reader.close()
            finally:
                self._ctrl.close()
