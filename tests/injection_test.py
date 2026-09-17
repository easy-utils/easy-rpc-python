"""Composition-root injection parity: connect(transport=...) wraps a custom
adapter with the SAME built-in interceptors as a mode-picked one."""
import asyncio
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from easyrpc import connect, Request, Response


class FakeTransport:
    def __init__(self):
        self.seen = []

    async def send(self, req):
        self.seen.append(req.headers)
        return Response(status=200)

    async def open_stream(self, req):
        raise NotImplementedError


def run(coro):
    return asyncio.run(coro)


def test_injected_adapter_gets_standard_interceptors():
    fake = FakeTransport()
    t = connect(base="http://x", token="sekret", timeout_ms=1500, transport=fake)
    run(t.send(Request(url="/x")))
    h = fake.seen[0]
    assert h.get("authorization") == ["Bearer sekret"], h
    assert h.get("connect-timeout-ms") == ["1500"], h


def test_no_opts_leaves_injected_adapter_untouched():
    fake = FakeTransport()
    t = connect(base="http://x", transport=fake)
    run(t.send(Request(url="/x", headers={"a": ["b"]})))
    h = fake.seen[0]
    assert "authorization" not in h
    assert h.get("a") == ["b"], h


if __name__ == "__main__":
    test_injected_adapter_gets_standard_interceptors()
    test_no_opts_leaves_injected_adapter_untouched()
    print("PY_INJECTION_OK")
