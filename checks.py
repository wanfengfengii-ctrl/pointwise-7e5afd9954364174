"""Frozen business regressions; extend main() with structural checks after refactoring."""
import json
import sys
from collector import Session, ProtocolError, TransferError
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


def main():
    records = regression()
    if "--demo" in sys.argv:
        chosen = [r for r in records if r["path"] in ("normal", "failed-race", "bad")]
        print(json.dumps(chosen, indent=2, ensure_ascii=False))
    else:
        from frozen import DIGEST
        import hashlib
        digest = hashlib.sha256(json.dumps(records, sort_keys=True,
                                          separators=(",", ":")).encode()).hexdigest()
        assert digest == DIGEST, (digest, DIGEST)
        print("FTP_REGRESSION_OK", len(records), digest)


if __name__ == "__main__":
    main()
