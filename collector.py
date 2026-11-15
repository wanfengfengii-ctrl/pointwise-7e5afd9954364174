"""Working synchronous collector; intentionally mixed transfer/control ownership."""
import re
import socket


class ProtocolError(Exception):
    pass


class TransferError(Exception):
    pass


class Session:
    def __init__(self, address, trace=None, connect=socket.create_connection):
        self.trace = [] if trace is None else trace
        self.connect = connect
        self.closed = False
        self.busy = False
        self.ctrl = connect(address)
        self.ctrl.settimeout(3)
        self.reader = self.ctrl.makefile("rb")
        try:
            if self._reply()[0] != 220:
                raise ProtocolError("greeting")
        except BaseException:
            self._break()
            raise

    def _break(self):
        if not self.closed:
            self.closed = True
            self.trace.append(["broken"])
            try:
                self.reader.close()
            finally:
                self.ctrl.close()

    def _send(self, command):
        self.trace.append(["send", command])
        self.ctrl.sendall(command.encode("ascii") + b"\r\n")

    def _reply(self):
        line = self.reader.readline(4097)
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

    def fetch(self, path, sink):
        if self.closed:
            raise RuntimeError("closed")
        if self.busy:
            raise RuntimeError("busy")
        if not isinstance(path, str) or not path or any(
                ord(c) < 32 or ord(c) > 126 for c in path):
            raise ValueError("path")
        self.busy = True
        data = None
        pending = False
        settled = False
        stopped = False
        count = 0
        primary = None
        poison = False
        try:
            self._send("EPSV")
            code, text = self._reply()
            match = re.fullmatch(r"Entering Extended Passive Mode \(\|\|\|([0-9]+)\|\)", text)
            if code != 229 or match is None:
                poison = True
                raise ProtocolError("passive")
            port = int(match[1])
            if not 0 < port < 65536:
                poison = True
                raise ProtocolError("passive port")
            data = self.connect((self.ctrl.getpeername()[0], port))
            data.settimeout(3)
            self._send("RETR " + path)
            pending = True
            code, _ = self._reply()
            if code == 550:
                pending = False
                raise TransferError("retrieve 550")
            if code != 150:
                poison = True
                raise ProtocolError("preliminary")
            while True:
                chunk = data.recv(4)
                if not chunk:
                    break
                self.trace.append(["body", chunk.hex()])
                answer = sink.write(chunk)
                count += len(chunk)
                self.trace.append(["write", count])
                if answer is False:
                    stopped = True
                    break
            channel, data = data, None
            self.trace.append(["data.close"])
            channel.close()
            if not stopped:
                code, _ = self._reply()
                if code == 426:
                    tail, _ = self._reply()
                    if tail != 226:
                        poison = True
                        raise ProtocolError("completion tail")
                    pending = False
                    settled = True
                    raise TransferError("transfer 426")
                if code not in (226, 451):
                    poison = True
                    raise ProtocolError("completion")
                pending = False
                settled = True
                if code == 451:
                    raise TransferError("transfer 451")
                sink.finish(count)
                self.trace.append(["finish", count])
        except BaseException as exc:
            primary = exc
            if isinstance(exc, ProtocolError):
                poison = True
        # These exits deliberately share local ownership and error variables.
        if data is not None:
            self.trace.append(["data.close"])
            try:
                data.close()
            except BaseException as exc:
                if primary is None:
                    primary = exc
        if pending and not poison:
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
                pending = False
                settled = True
            except BaseException as exc:
                poison = True
                if primary is None:
                    primary = exc
        # A framing/I/O fault with an uncertain reply owner destroys the session.
        if poison or (pending and not settled) or (
                isinstance(primary, (OSError, EOFError)) and data is None and not settled):
            try:
                self._break()
            except BaseException as exc:
                if primary is None:
                    primary = exc
        self.trace.append(["sink.close"])
        try:
            sink.close()
        except BaseException as exc:
            if primary is None:
                primary = exc
        self.busy = False
        if primary is not None:
            raise primary
        return count

    def close(self):
        if not self.closed:
            self.closed = True
            self.trace.append(["session.close"])
            try:
                self.reader.close()
            finally:
                self.ctrl.close()


def collect(address, jobs, trace=None, connect=socket.create_connection):
    session = Session(address, trace, connect)
    try:
        return [session.fetch(path, sink) for path, sink in jobs]
    finally:
        session.close()
