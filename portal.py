"""Scripted FTP peer over two independent kernel stream socket pairs."""
import socket
import threading


class Wire:
    def __init__(self, raw, peer):
        self.raw = raw
        self.peer = peer

    def getpeername(self):
        return self.peer

    def __getattr__(self, name):
        return getattr(self.raw, name)


class Portal:
    def __init__(self, cases):
        self.cases = cases
        self.commands = []
        self.errors = []
        self.address = ("fixture", 21)
        client, self.server = socket.socketpair()
        self.client = Wire(client, self.address)
        self.passive = None
        self.port = 12000
        self.thread = threading.Thread(target=self._run, daemon=True)

    def connect(self, address):
        if address == self.address and self.client is not None:
            client, self.client = self.client, None
            return client
        if address == ("fixture", self.port) and self.passive is not None:
            client, self.passive = self.passive, None
            return client
        raise OSError("unknown fixture endpoint")

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.thread.join(5)
        if self.thread.is_alive():
            self.server.close()
            raise AssertionError("portal did not stop")
        if self.client is not None:
            self.client.close()
        if self.passive is not None:
            self.passive.close()
        if self.errors:
            raise self.errors[0]

    def _run(self):
        control = self.server
        control.settimeout(4)
        reader = control.makefile("rb")
        data = None
        try:
            def reply(code, text="ok"):
                control.sendall(("%d %s\r\n" % (code, text)).encode("ascii"))
            reply(220, "fixture")
            active = None
            while True:
                line = reader.readline()
                if not line:
                    break
                command = line.decode("ascii").rstrip("\r\n")
                self.commands.append(command)
                if command == "EPSV":
                    self.port += 1
                    self.passive, data = socket.socketpair()
                    reply(229, "Entering Extended Passive Mode (|||%d|)" % self.port)
                elif command.startswith("RETR "):
                    active = self.cases[command[5:]]
                    if active.get("missing"):
                        data.close()
                        data = None
                        reply(550)
                        continue
                    reply(150)
                    try:
                        data.sendall(active.get("body", b""))
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    finally:
                        data.close()
                        data = None
                    if active.get("early"):
                        for code in active["early"]:
                            reply(code)
                    elif not active.get("abort_only"):
                        for code in active.get("complete", [226]):
                            reply(code)
                elif command == "ABOR":
                    for code in active.get("abort", [225]):
                        reply(code)
                elif command == "NOOP":
                    if active.get("bad_barrier"):
                        control.sendall(b"bad\r\n")
                    else:
                        reply(200)
                else:
                    raise AssertionError(command)
        except (ConnectionResetError, BrokenPipeError):
            pass
        except BaseException as exc:
            self.errors.append(exc)
        finally:
            if data is not None:
                data.close()
            reader.close()
            control.close()
