"""Composition-root behaviour: metadata attached; deadline cancels locally."""
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from easyrpc import (  # noqa: E402
    MetadataInterceptor,
    Request,
    Response,
    RPCError,
    TimeoutInterceptor,
    Transport,
    interceptors,
)

seen = {}


class Fake(Transport):
    async def send(self, req):
        seen.update(req.headers)
        return Response(status=200)

    async def open_stream(self, req):
        raise NotImplementedError()


class Slow(Transport):
    async def send(self, req):
        await asyncio.sleep(5)
        return Response(status=200)

    async def open_stream(self, req):
        raise NotImplementedError()


def test_interceptors_over_any_adapter():
    seen.clear()
    t = interceptors(Fake(), MetadataInterceptor({"authorization": ["Bearer abc"]}), TimeoutInterceptor(0))
    asyncio.run(t.send(Request(url="/x")))
    assert seen.get("authorization") == ["Bearer abc"], seen


def test_deadline_cancels():
    t = interceptors(Slow(), TimeoutInterceptor(60))
    start = time.monotonic()
    try:
        asyncio.run(t.send(Request(url="/x")))
        raise AssertionError("should have raised")
    except RPCError as e:
        assert e.code == 4, e
    assert time.monotonic() - start < 2


if __name__ == "__main__":
    test_interceptors_over_any_adapter()
    test_deadline_cancels()
    print("py connect/interceptor ok")
