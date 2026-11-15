"""Frozen business regressions plus structural boundary checks for the split."""
import ast
import inspect
import json
import pathlib
import sys

import collector
import control
import delivery
from collector import Session, ProtocolError, TransferError
from control import ControlSession
from delivery import transfer
from portal import Portal
from samples import CASES, Sink, SinkError, CleanupError, connector


def run_case(path, sink=None, fault=None):
    sink = Sink() if sink is None else sink
    trace = []
    with Portal(CASES) as portal:
        session = Session(portal.address, trace, connector(portal.connect, fault))
        error = None
        result = None
        try:
            result = session.fetch(path, sink)
        except BaseException as exc:
            error = exc
        reusable = not session.closed
        follow = Sink()
        if reusable:
            assert session.fetch("empty", follow) == 0
            assert follow.events == [["finish", 0], ["close"]]
        session.close()
    # Replace only the OS-selected port; preserve all other trace items.
    frozen = []
    for item in trace:
        if item[:2] == ["reply", 229]:
            frozen.append(["reply", 229, "Entering Extended Passive Mode (|||PORT|)"])
        else:
            frozen.append(item)
    return {"path": path, "result": result,
            "error": None if error is None else [type(error).__name__, str(error)],
            "body": bytes(sink.body).hex(), "sink": sink.events,
            "reusable": reusable, "commands": portal.commands, "trace": frozen}, error


def regression():
    outputs = []
    def check(path, **kwargs):
        record, exc = run_case(path, **kwargs)
        outputs.append(record)
        return record, exc
    record, exc = check("normal")
    assert exc is None and record["result"] == 8
    assert record["body"] == b"abcdefgh".hex()
    assert record["commands"] == ["EPSV", "RETR normal", "EPSV", "RETR empty"]
    record, exc = check("empty")
    assert exc is None and record["result"] == 0
    for path in ("abort", "raced", "failed-race", "late"):
        record, exc = check(path, sink=Sink(stop=True))
        assert exc is None and record["result"] == 4
        assert record["sink"] == [["write", "61626364"], ["close"]]
        assert record["commands"][2:4] == ["ABOR", "NOOP"]
        assert record["reusable"]
    for path, message in (("gone", "retrieve 550"),
                          ("transfer-fail", "transfer 426"),
                          ("local-fail", "transfer 451")):
        record, exc = check(path)
        assert isinstance(exc, TransferError) and str(exc) == message
        assert record["reusable"] and "ABOR" not in record["commands"]
    marker = SinkError("write failed")
    record, exc = check("raced", sink=Sink(write_error=marker,
                                        close_error=CleanupError("sink close")))
    assert exc is marker and record["reusable"]
    assert record["sink"][-1] == ["close"]
    marker = SinkError("finish failed")
    record, exc = check("normal", sink=Sink(finish_error=marker))
    assert exc is marker and "ABOR" not in record["commands"]
    record, exc = check("raced", fault="close")
    assert isinstance(exc, CleanupError) and str(exc) == "data close"
    assert record["reusable"] and record["commands"][2:4] == ["ABOR", "NOOP"]
    record, exc = check("abort", fault="read")
    assert isinstance(exc, OSError) and str(exc) == "data read"
    assert record["reusable"]
    record, exc = check("normal", sink=Sink(close_error=CleanupError("sink close")))
    assert isinstance(exc, CleanupError) and record["reusable"]
    record, exc = check("bad", sink=Sink(stop=True))
    assert isinstance(exc, ProtocolError) and not record["reusable"]
    marker = SinkError("primary")
    record, exc = check("bad", sink=Sink(write_error=marker,
                                      close_error=CleanupError("secondary")))
    assert exc is marker and not record["reusable"]
    # Admission stays held through both sink callbacks, even after 226.
    with Portal(CASES) as portal:
        session = Session(portal.address, connect=portal.connect)
        class Reentrant(Sink):
            def probe(self):
                child = Sink()
                try:
                    session.fetch("empty", child)
                except RuntimeError as exc:
                    assert str(exc) == "busy"
                else:
                    raise AssertionError("callback admitted a nested fetch")
                assert child.events == []
            def finish(self, count):
                self.probe()
                super().finish(count)
            def close(self):
                self.probe()
                super().close()
        assert session.fetch("normal", Reentrant()) == 8
        invalid = Sink()
        try:
            session.fetch("bad\rpath", invalid)
        except ValueError as exc:
            assert str(exc) == "path"
        else:
            raise AssertionError("invalid path accepted")
        assert invalid.events == []
        session.close()
        session.close()
        try:
            session.fetch("empty", invalid)
        except RuntimeError as exc:
            assert str(exc) == "closed"
        else:
            raise AssertionError("closed session accepted")
        assert invalid.events == []
    return outputs


class FakeChannel:
    """正文通道替身：脚本化 recv/close，记录每次请求长度。"""

    def __init__(self, chunks=(), recv_error=None, close_error=None):
        self.chunks = list(chunks)
        self.recv_error = recv_error
        self.close_error = close_error
        self.sizes = []
        self.closed = 0

    def recv(self, size):
        self.sizes.append(size)
        if self.recv_error is not None:
            error, self.recv_error = self.recv_error, None
            raise error
        if self.chunks:
            return self.chunks.pop(0)
        return b""

    def close(self):
        self.closed += 1
        if self.close_error is not None:
            error, self.close_error = self.close_error, None
            raise error


class PortDouble:
    """只实现会话端口的替身；不调用旧 Session 或真实控制实现。"""

    def __init__(self, channel=None, open_error=None,
                 complete_error=None, abort_error=None):
        self.calls = []
        self.closed = False
        self.busy = False
        self.channel = FakeChannel() if channel is None else channel
        self.open_error = open_error
        self.complete_error = complete_error
        self.abort_error = abort_error

    def acquire(self, path):
        self.calls.append(["acquire", path])
        if self.closed:
            raise RuntimeError("closed")
        if self.busy:
            raise RuntimeError("busy")
        if not isinstance(path, str) or not path or any(
                ord(c) < 32 or ord(c) > 126 for c in path):
            raise ValueError("path")
        self.busy = True

    def open(self):
        self.calls.append(["open"])
        if self.open_error is not None:
            raise self.open_error
        return self.channel

    def complete(self):
        self.calls.append(["complete"])
        if self.complete_error is not None:
            raise self.complete_error

    def abort(self):
        self.calls.append(["abort"])
        if self.abort_error is not None:
            raise self.abort_error

    def release(self):
        self.calls.append(["release"])
        self.busy = False

    def close(self):
        self.calls.append(["close"])
        self.closed = True

    def names(self):
        return [call[0] for call in self.calls]


def transfer_probes():
    # 正常：正文、计数、finish、端口调用次序与逐项 trace。
    double = PortDouble(channel=FakeChannel([b"ab", b"cd"]))
    sink = Sink()
    trace = []
    assert transfer(double, "file", sink, trace) == 4
    assert sink.events == [["write", "6162"], ["write", "6364"],
                           ["finish", 4], ["close"]]
    assert double.names() == ["acquire", "open", "complete", "abort", "release"]
    assert double.channel.sizes == [4, 4, 4]
    assert trace == [["body", "6162"], ["write", 2], ["body", "6364"],
                     ["write", 4], ["data.close"], ["finish", 4], ["sink.close"]]
    assert not double.busy
    # 提前停止：跳过 complete，仍咨询 abort，不调用 finish。
    double = PortDouble(channel=FakeChannel([b"ab", b"cd"]))
    sink = Sink(stop=True)
    assert transfer(double, "file", sink, []) == 2
    assert double.names() == ["acquire", "open", "abort", "release"]
    assert sink.events == [["write", "6162"], ["close"]]
    # open 失败：无正文可关，abort 仍被咨询，首错原样抛出。
    marker = SinkError("open")
    double = PortDouble(open_error=marker)
    sink = Sink()
    try:
        transfer(double, "file", sink, [])
    except SinkError as exc:
        assert exc is marker
    else:
        raise AssertionError("open failure lost")
    assert double.names() == ["acquire", "open", "abort", "release"]
    assert sink.events == [["close"]]
    # complete 失败：不调用 finish，首错原样抛出。
    marker = SinkError("complete")
    double = PortDouble(channel=FakeChannel([b"ab"]), complete_error=marker)
    sink = Sink()
    try:
        transfer(double, "file", sink, [])
    except SinkError as exc:
        assert exc is marker
    else:
        raise AssertionError("complete failure lost")
    assert double.names() == ["acquire", "open", "complete", "abort", "release"]
    assert sink.events == [["write", "6162"], ["close"]]
    # abort 失败：无主异常时成为首错。
    marker = SinkError("abort")
    double = PortDouble(channel=FakeChannel([b"ab"]), abort_error=marker)
    sink = Sink()
    try:
        transfer(double, "file", sink, [])
    except SinkError as exc:
        assert exc is marker
    else:
        raise AssertionError("abort failure lost")
    assert sink.events == [["write", "6162"], ["finish", 2], ["close"]]
    # abort 失败不覆盖主异常。
    primary = SinkError("primary")
    double = PortDouble(channel=FakeChannel([b"ab"]),
                        abort_error=SinkError("abort"))
    sink = Sink(write_error=primary)
    try:
        transfer(double, "file", sink, [])
    except SinkError as exc:
        assert exc is primary
    else:
        raise AssertionError("primary overridden")
    # 正文读取失败：通道只释放一次，首错原样抛出。
    marker = OSError("recv")
    channel = FakeChannel(recv_error=marker)
    double = PortDouble(channel=channel)
    sink = Sink()
    try:
        transfer(double, "file", sink, [])
    except OSError as exc:
        assert exc is marker
    else:
        raise AssertionError("recv failure lost")
    assert channel.closed == 1
    assert double.names() == ["acquire", "open", "abort", "release"]
    # 交出后的通道关闭失败：成为首错，complete 不再调用。
    marker = CleanupError("data close")
    channel = FakeChannel([b"ab"], close_error=marker)
    double = PortDouble(channel=channel)
    sink = Sink()
    try:
        transfer(double, "file", sink, [])
    except CleanupError as exc:
        assert exc is marker
    else:
        raise AssertionError("close failure lost")
    assert channel.closed == 1
    assert "complete" not in double.names()
    # 写入失败叠加关闭失败：首错保留。
    primary = SinkError("write")
    sink = Sink(write_error=primary, close_error=CleanupError("sink close"))
    double = PortDouble(channel=FakeChannel([b"ab"]))
    try:
        transfer(double, "file", sink, [])
    except SinkError as exc:
        assert exc is primary
    else:
        raise AssertionError("write failure lost")
    assert sink.events[-1] == ["close"]
    # finish 失败：complete 已成功，错误原样抛出。
    marker = SinkError("finish")
    sink = Sink(finish_error=marker)
    double = PortDouble(channel=FakeChannel([b"ab"]))
    try:
        transfer(double, "file", sink, [])
    except SinkError as exc:
        assert exc is marker
    else:
        raise AssertionError("finish failure lost")
    assert double.names() == ["acquire", "open", "complete", "abort", "release"]
    # sink.close 失败：唯一的错误原样抛出。
    marker = CleanupError("sink close")
    sink = Sink(close_error=marker)
    double = PortDouble(channel=FakeChannel([b"ab"]))
    try:
        transfer(double, "file", sink, [])
    except CleanupError as exc:
        assert exc is marker
    else:
        raise AssertionError("sink close failure lost")
    # 重入边界：write/finish/close 回调内重入 transfer 得 busy。
    double = PortDouble(channel=FakeChannel([b"ab"]))
    class Reentrant(Sink):
        def probe(self):
            child = Sink()
            try:
                transfer(double, "other", child, [])
            except RuntimeError as exc:
                assert str(exc) == "busy"
            else:
                raise AssertionError("callback admitted a nested transfer")
            assert child.events == []
        def write(self, chunk):
            self.probe()
            return super().write(chunk)
        def finish(self, count):
            self.probe()
            super().finish(count)
        def close(self):
            self.probe()
            super().close()
    assert transfer(double, "file", Reentrant(), []) == 2
    assert not double.busy
    # 准入失败不调用 sink：closed 与非法 path；准入失败不持有 busy。
    double = PortDouble()
    double.closed = True
    sink = Sink()
    try:
        transfer(double, "file", sink, [])
    except RuntimeError as exc:
        assert str(exc) == "closed"
    else:
        raise AssertionError("closed port admitted")
    assert sink.events == []
    double = PortDouble()
    try:
        transfer(double, "bad\rpath", sink, [])
    except ValueError as exc:
        assert str(exc) == "path"
    else:
        raise AssertionError("invalid path admitted")
    assert sink.events == [] and not double.busy
    # transfer 不关闭会话端口。
    assert "close" not in double.names()


class ControlWire:
    """录制控制线：按脚本回放回复，记录命令与读取次数。"""

    def __init__(self, lines):
        self.lines = list(lines)
        self.sent = []
        self.reads = 0
        self.closed = False

    def settimeout(self, timeout):
        self.timeout = timeout

    def makefile(self, mode):
        assert mode == "rb"
        return self

    def readline(self, limit=-1):
        self.reads += 1
        return self.lines.pop(0) if self.lines else b""

    def sendall(self, data):
        self.sent.append(data)

    def getpeername(self):
        return ("peer", 21)

    def close(self):
        self.closed = True


class DataWire:
    """正文替身：记录 recv/close，控制组件不得消费或累计。"""

    def __init__(self):
        self.recvs = []
        self.closed = False

    def settimeout(self, timeout):
        self.timeout = timeout

    def recv(self, size):
        self.recvs.append(size)
        return b""

    def close(self):
        self.closed = True


class Connector:
    def __init__(self, control_wire):
        self.control_wire = control_wire
        self.data_wires = []
        self.addresses = []

    def __call__(self, address):
        self.addresses.append(address)
        if len(self.addresses) == 1:
            return self.control_wire
        wire = DataWire()
        self.data_wires.append(wire)
        return wire


def control_session(lines):
    wire = ControlWire([b"220 fixture\r\n"] + list(lines))
    connector = Connector(wire)
    trace = []
    session = ControlSession(("peer", 21), trace, connector)
    return session, wire, connector, trace


def control_probes():
    # 问候与准入：acquire/release 无 I/O，准入失败不持有 busy。
    session, wire, connector, trace = control_session([])
    assert trace == [["reply", 220, "fixture"]]
    assert not session.closed and not session.busy
    session.acquire("one")
    assert session.busy
    try:
        session.acquire("two")
    except RuntimeError as exc:
        assert str(exc) == "busy"
    else:
        raise AssertionError("double acquire admitted")
    session.release()
    assert not session.busy
    for bad in ("", "a\rb", "a\nb", "a\x7fb"):
        try:
            session.acquire(bad)
        except ValueError as exc:
            assert str(exc) == "path"
        else:
            raise AssertionError("invalid path admitted")
        assert not session.busy
    reads, sent = wire.reads, list(wire.sent)
    session.acquire("one")
    session.release()
    assert wire.reads == reads and wire.sent == sent
    # 正常 open/complete：回复结清、无副作用 abort、下一次准入。
    session, wire, connector, trace = control_session([
        b"229 Entering Extended Passive Mode (|||65000|)\r\n",
        b"150 ok\r\n",
        b"226 done\r\n",
    ])
    session.acquire("file")
    channel = session.open()
    assert connector.addresses[1] == ("peer", 65000)
    assert wire.sent == [b"EPSV\r\n", b"RETR file\r\n"]
    assert set(n for n in dir(channel) if not n.startswith("_")) <= {"close", "recv"}
    session.complete()
    session.abort()
    assert wire.sent == [b"EPSV\r\n", b"RETR file\r\n"]
    assert connector.data_wires[0].recvs == []
    assert not connector.data_wires[0].closed
    session.release()
    session.acquire("next")
    session.release()
    session.close()
    assert session.closed
    session.close()
    assert trace.count(["session.close"]) == 1
    channel.close()
    assert connector.data_wires[0].closed
    # 550：交出前的连接由控制组件释放，无 ABOR，可继续准入。
    session, wire, connector, trace = control_session([
        b"229 Entering Extended Passive Mode (|||65001|)\r\n",
        b"550 gone\r\n",
    ])
    session.acquire("gone")
    try:
        session.open()
    except TransferError as exc:
        assert str(exc) == "retrieve 550"
    else:
        raise AssertionError("550 accepted")
    assert connector.data_wires[0].closed
    assert trace.count(["data.close"]) == 1
    session.abort()
    assert wire.sent == [b"EPSV\r\n", b"RETR gone\r\n"]
    session.release()
    session.acquire("next")
    session.release()
    # 451 与 426/226：已结清的失败不留作下一次 abort。
    for extra, message in (
            ((b"150 ok\r\n", b"451 no\r\n"), "transfer 451"),
            ((b"150 ok\r\n", b"426 x\r\n", b"226 y\r\n"), "transfer 426")):
        session, wire, connector, trace = control_session(
            [b"229 Entering Extended Passive Mode (|||65002|)\r\n"] + list(extra))
        session.acquire("file")
        session.open()
        try:
            session.complete()
        except TransferError as exc:
            assert str(exc) == message
        else:
            raise AssertionError("settled failure not raised")
        session.abort()
        assert wire.sent == [b"EPSV\r\n", b"RETR file\r\n"]
        session.release()
        session.acquire("next")
        session.release()
    # abort：ABOR/NOOP 交错回复结清后才放行下一次准入。
    session, wire, connector, trace = control_session([
        b"229 Entering Extended Passive Mode (|||65003|)\r\n",
        b"150 ok\r\n",
        b"426 x\r\n", b"226 y\r\n", b"200 z\r\n",
    ])
    session.acquire("file")
    session.open()
    session.abort()
    assert wire.sent == [b"EPSV\r\n", b"RETR file\r\n", b"ABOR\r\n", b"NOOP\r\n"]
    session.abort()
    assert len(wire.sent) == 4
    session.release()
    session.acquire("next")
    session.release()
    # 非法完成回复：销毁会话，后续准入得 closed，abort 无副作用。
    session, wire, connector, trace = control_session([
        b"229 Entering Extended Passive Mode (|||65004|)\r\n",
        b"150 ok\r\n",
        b"200 no\r\n",
    ])
    session.acquire("file")
    session.open()
    try:
        session.complete()
    except ProtocolError as exc:
        assert str(exc) == "completion"
    else:
        raise AssertionError("bad completion accepted")
    assert session.closed and ["broken"] in trace
    session.abort()
    assert wire.sent == [b"EPSV\r\n", b"RETR file\r\n"]
    session.release()
    try:
        session.acquire("next")
    except RuntimeError as exc:
        assert str(exc) == "closed"
    else:
        raise AssertionError("broken session admitted")
    # 426 后非 226 尾巴：协议损坏即销毁。
    session, wire, connector, trace = control_session([
        b"229 Entering Extended Passive Mode (|||65005|)\r\n",
        b"150 ok\r\n",
        b"426 x\r\n", b"451 y\r\n",
    ])
    session.acquire("file")
    session.open()
    try:
        session.complete()
    except ProtocolError as exc:
        assert str(exc) == "completion tail"
    else:
        raise AssertionError("bad tail accepted")
    assert session.closed
    # abort 序列非法：销毁会话。
    session, wire, connector, trace = control_session([
        b"229 Entering Extended Passive Mode (|||65006|)\r\n",
        b"150 ok\r\n",
        b"200 z\r\n",
    ])
    session.acquire("file")
    session.open()
    try:
        session.abort()
    except ProtocolError as exc:
        assert str(exc) == "abort sequence"
    else:
        raise AssertionError("bad abort sequence accepted")
    assert session.closed
    # open 阶段协议错误：交出前连接由控制组件释放并销毁，顺序可核。
    session, wire, connector, trace = control_session([
        b"229 Entering Extended Passive Mode (|||65007|)\r\n",
        b"450 no\r\n",
    ])
    session.acquire("file")
    try:
        session.open()
    except ProtocolError as exc:
        assert str(exc) == "preliminary"
    else:
        raise AssertionError("bad preliminary accepted")
    assert connector.data_wires[0].closed and session.closed
    assert trace.index(["data.close"]) < trace.index(["broken"])
    # 损坏屏障行：abort 收尾失败即销毁，不再准入。
    session, wire, connector, trace = control_session([
        b"229 Entering Extended Passive Mode (|||65008|)\r\n",
        b"150 ok\r\n",
        b"426 x\r\n", b"226 y\r\n", b"bad\r\n",
    ])
    session.acquire("file")
    session.open()
    try:
        session.abort()
    except ProtocolError as exc:
        assert str(exc) == "reply framing"
    else:
        raise AssertionError("corrupt barrier accepted")
    assert session.closed
    session.release()
    try:
        session.acquire("next")
    except RuntimeError as exc:
        assert str(exc) == "closed"
    else:
        raise AssertionError("broken session admitted")
    # 问候失败：即销毁。
    wire = ControlWire([b"500 no\r\n"])
    trace = []
    try:
        ControlSession(("peer", 21), trace, Connector(wire))
    except ProtocolError as exc:
        assert str(exc) == "greeting"
    else:
        raise AssertionError("bad greeting accepted")
    assert ["broken"] in trace


def structural():
    # 依赖方向：control 不依赖 delivery/collector；delivery 无任何依赖。
    def imports_of(module):
        tree = ast.parse(pathlib.Path(module.__file__).read_text(encoding="utf-8"))
        found = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                found.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                found.add(node.module.split(".")[0])
        return found
    assert not {"delivery", "collector"} & imports_of(control)
    assert imports_of(delivery) == set()
    # 源码级职责边界：控制不碰下游与正文计数，交付不拼命令不读回复。
    sources = {}
    for module in (control, delivery, collector):
        sources[module.__name__] = pathlib.Path(
            module.__file__).read_text(encoding="utf-8")
    for token in ("sink", "finish", '["body"', "count"):
        assert token not in sources["control"], token
    for token in ("EPSV", "RETR", "ABOR", "NOOP", "reader", "ctrl", "makefile",
                  "sendall", "reply", "229", "150", "226", "426", "451", "550"):
        assert token not in sources["delivery"], token
    # 原入口仅转发，没有备用执行器。
    for token in ("_reply", "_send", "_break", "EPSV", "RETR", "ABOR", "NOOP",
                  "recv(", "makefile", "pending"):
        assert token not in sources["collector"], token
    assert collector.transfer is delivery.transfer
    for name in ("_reply", "_send", "_break"):
        assert not hasattr(Session, name), name
    tree = ast.parse(sources["collector"])
    session_cls = next(node for node in tree.body
                       if isinstance(node, ast.ClassDef) and node.name == "Session")
    methods = sorted(node.name for node in session_cls.body
                     if isinstance(node, ast.FunctionDef))
    assert methods == ["__init__", "busy", "close", "closed", "fetch", "trace"]
    fetch = next(node for node in session_cls.body
                 if isinstance(node, ast.FunctionDef) and node.name == "fetch")
    assert len(fetch.body) == 1 and isinstance(fetch.body[0], ast.Return)
    call = fetch.body[0].value
    assert isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
    assert call.func.id == "transfer"
    close = next(node for node in session_cls.body
                 if isinstance(node, ast.FunctionDef) and node.name == "close")
    assert len(close.body) == 1
    # 端口形状：ControlSession 不接收 sink；transfer 只按端口签名。
    assert list(inspect.signature(ControlSession.__init__).parameters) == [
        "self", "address", "trace", "connect"]
    assert list(inspect.signature(transfer).parameters) == [
        "control", "path", "sink", "trace"]
    assert isinstance(ControlSession.closed, property)
    assert isinstance(ControlSession.busy, property)
    for name in ("acquire", "open", "complete", "abort", "release", "close"):
        assert callable(getattr(ControlSession, name)), name
    # 替身驱动交付，录制控制线驱动控制会话。
    transfer_probes()
    control_probes()


def main():
    records = regression()
    if "--demo" in sys.argv:
        chosen = [r for r in records if r["path"] in ("normal", "failed-race", "bad")]
        print(json.dumps(chosen, indent=2, ensure_ascii=False))
    else:
        structural()
        from frozen import DIGEST
        import hashlib
        digest = hashlib.sha256(json.dumps(records, sort_keys=True,
                                          separators=(",", ":")).encode()).hexdigest()
        assert digest == DIGEST, (digest, DIGEST)
        print("FTP_STRUCTURAL_OK")
        print("FTP_REGRESSION_OK", len(records), digest)


if __name__ == "__main__":
    main()
