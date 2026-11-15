import socket


CASES = {
    "normal": {"body": b"abcdefgh"},
    "empty": {"body": b""},
    "abort": {"body": b"abcdefgh", "abort_only": True, "abort": [426, 226]},
    "raced": {"body": b"abcdefgh", "early": [226], "abort": [225]},
    "failed-race": {"body": b"abcdefgh", "early": [426, 226], "abort": [225]},
    "late": {"body": b"abcdefgh", "abort_only": True, "abort": [226]},
    "gone": {"missing": True},
    "transfer-fail": {"body": b"abcd", "complete": [426, 226]},
    "local-fail": {"body": b"abcd", "complete": [451]},
    "bad": {"body": b"abcdefgh", "abort_only": True,
            "abort": [426, 226], "bad_barrier": True},
}


class SinkError(Exception):
    pass


class CleanupError(Exception):
    pass


class Sink:
    def __init__(self, stop=False, write_error=None, finish_error=None, close_error=None):
        self.body = bytearray()
        self.events = []
        self.stop = stop
        self.write_error = write_error
        self.finish_error = finish_error
        self.close_error = close_error

    def write(self, chunk):
        self.events.append(["write", chunk.hex()])
        if self.write_error is not None:
            raise self.write_error
        self.body.extend(chunk)
        return False if self.stop else None

    def finish(self, count):
        self.events.append(["finish", count])
        if self.finish_error is not None:
            raise self.finish_error

    def close(self):
        self.events.append(["close"])
        if self.close_error is not None:
            raise self.close_error


class Channel:
    def __init__(self, raw, fault):
        self.raw = raw
        self.fault = fault

    def settimeout(self, timeout):
        self.raw.settimeout(timeout)

    def recv(self, size):
        if self.fault == "read":
            self.fault = None
            raise OSError("data read")
        parts = []
        left = size
        while left:
            part = self.raw.recv(left)
            if not part:
                break
            parts.append(part)
            left -= len(part)
        return b"".join(parts)

    def close(self):
        self.raw.close()
        if self.fault == "close":
            self.fault = None
            raise CleanupError("data close")


def connector(base=socket.create_connection, fault=None):
    calls = 0
    def connect(address):
        nonlocal calls
        calls += 1
        raw = base(address)
        return Channel(raw, fault) if calls == 2 else raw
    return connect
