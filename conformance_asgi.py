"""easy-rpc Python conformance server (ASGI). Shared app used by both the
uvicorn (HTTP/1) and Hypercorn (h2c + HTTP/1) server entries.

Thin ASGI adapter over the core `easyrpc.server.dispatch`, so the fixed
conformance service shares the exact protocol edge semantics (POST-only 405,
415, 404, protocol-version, request/response gzip, END frames, frame
validation) with every other language. Implements the fixed easy-rpc v2
service: proto only, POST only, gRPC-style paths.
"""
import os

from easyrpc import (
    MethodSpec, RPCError, ErrorDetail, HandlerContext, frame, read_frame,
)
from easyrpc.server import ServerRegistry, dispatch
from easyrpc.conformance.v1 import conformance_pb2 as pb

PORT = int(os.environ.get("PORT", "18888"))
NAME = "conformance"

_SVC = "easyrpc.conformance.v1.ConformanceService"

# (name, is_stream) in service order.
_RPCS = [
    ("Health", False), ("Echo", False), ("Count", True), ("Fail", False),
    ("StreamFail", True), ("EchoMeta", False), ("Big", False),
    ("FailDetails", False), ("StreamFailDetails", True), ("EchoTrailer", False),
    ("CountTrailer", True), ("EchoBytes", False), ("Sleep", False),
    ("Empty", False), ("BigStream", True),
]

METHODS = [
    MethodSpec(service=_SVC, name=n, path=f"/{_SVC}/{n}", client_stream=False, server_stream=s)
    for n, s in _RPCS
]


from easyrpc import decode_msg, encode_msg


def _parse(cls, raw, ctx=None):
    return decode_msg(raw, cls, ctx.kind if ctx is not None else "proto")


def _ser(msg, ctx=None):
    return encode_msg(msg, ctx.kind if ctx is not None else "proto")


def build_registry() -> ServerRegistry:
    reg = ServerRegistry()

    async def health(_req, ctx):
        return _ser(pb.HealthResponse(ok=True, name=NAME), ctx)

    async def echo(req, ctx):
        return _ser(pb.EchoResponse(output="echo:" + _parse(pb.EchoRequest, req, ctx).input), ctx)

    async def count(req, ctx, emit):
        m = _parse(pb.CountRequest, req, ctx)
        n = m.count if m.count > 0 else 3
        for i in range(n):
            await emit(_ser(pb.CountResponse(index=i), ctx), False)

    async def fail(req, ctx):
        m = _parse(pb.FailRequest, req, ctx)
        if m.message:
            raise RPCError(3, m.message)
        return _ser(pb.FailResponse(ok=True), ctx)

    async def stream_fail(req, ctx, emit):
        m = _parse(pb.StreamFailRequest, req, ctx)
        for i in range(m.emit_before):
            await emit(_ser(pb.StreamFailResponse(index=i), ctx), False)
        raise RPCError(m.code or 13, m.message or "boom")

    async def echo_meta(req, ctx):
        m = _parse(pb.EchoMetaRequest, req, ctx)
        meta = {k: ctx.headers.get(k, [""])[0] for k in ("x-test", "authorization") if k in ctx.headers}
        return _ser(pb.EchoMetaResponse(input=m.input, meta=meta), ctx)

    async def big(req, ctx):
        m = _parse(pb.BigRequest, req, ctx)
        return _ser(pb.BigResponse(size=m.size), ctx)

    async def fail_details(req, ctx):
        m = _parse(pb.FailDetailsRequest, req, ctx)
        raise RPCError(m.code or 8, m.message or "limited", [_detail(m)])

    async def stream_fail_details(req, ctx, emit):
        m = _parse(pb.StreamFailDetailsRequest, req, ctx)
        for i in range(m.emit_before):
            await emit(_ser(pb.StreamFailDetailsResponse(index=i), ctx), False)
        raise RPCError(m.code or 13, m.message or "boom", [_detail(m)])

    async def echo_trailer(req, ctx):
        m = _parse(pb.EchoTrailerRequest, req, ctx)
        ctx.set_trailer("x-trl", "unary-" + m.input)
        return _ser(pb.EchoTrailerResponse(output="trailer:" + m.input), ctx)

    async def count_trailer(req, ctx, emit):
        ctx.set_trailer("x-ctrailer", "done")
        m = _parse(pb.CountTrailerRequest, req, ctx)
        n = m.count if m.count > 0 else 3
        for i in range(n):
            await emit(_ser(pb.CountTrailerResponse(index=i), ctx), False)

    async def echo_bytes(req, ctx):
        m = _parse(pb.EchoBytesRequest, req, ctx)
        return _ser(pb.EchoBytesResponse(data=m.data), ctx)

    async def sleep(req, ctx):
        import asyncio
        m = _parse(pb.SleepRequest, req, ctx)
        timeout = 0
        raw = ctx.headers.get("connect-timeout-ms")
        if raw:
            raw = raw[0] if isinstance(raw, (list, tuple)) else raw
            try:
                timeout = int(raw)
            except ValueError:
                timeout = 0
        if timeout > 0 and timeout < m.millis:
            await asyncio.sleep(timeout / 1000.0)
            raise RPCError(4, "deadline exceeded")
        if m.millis > 0:
            await asyncio.sleep(m.millis / 1000.0)
        return _ser(pb.SleepResponse(ok=True), ctx)

    async def empty(_req, ctx):
        return _ser(pb.EmptyResponse(), ctx)

    async def big_stream(req, ctx, emit):
        m = _parse(pb.BigStreamRequest, req, ctx)
        n = m.count if m.count > 0 else 3
        for i in range(n):
            await emit(_ser(pb.BigStreamResponse(index=i, size=m.size), ctx), False)

    reg.unary["Health"] = health
    reg.unary["Echo"] = echo
    reg.stream["Count"] = count
    reg.unary["Fail"] = fail
    reg.stream["StreamFail"] = stream_fail
    reg.unary["EchoMeta"] = echo_meta
    reg.unary["Big"] = big
    reg.unary["FailDetails"] = fail_details
    reg.stream["StreamFailDetails"] = stream_fail_details
    reg.unary["EchoTrailer"] = echo_trailer
    reg.stream["CountTrailer"] = count_trailer
    reg.unary["EchoBytes"] = echo_bytes
    reg.unary["Sleep"] = sleep
    reg.unary["Empty"] = empty
    reg.stream["BigStream"] = big_stream
    return reg


def _detail(m):
    return ErrorDetail(m.detail_type or "t/x", (m.detail_text or "d").encode())


REG = build_registry()

# Legacy route table (path -> (is_stream, name)); kept for compatibility.
ROUTES = {m.path: (m.server_stream, m.name) for m in METHODS}


class _AsgiWriter:
    """ASGI ResponseWriter: buffers frames, emits one response body. ASGI does
    not expose per-frame flush portably, so this buffers — the raw-wire oracle
    checks bytes, not timing, for this fixture."""

    def __init__(self):
        self.status_code = 200
        self.headers = []
        self.chunks = []

    def status(self, code):
        self.status_code = code

    def header(self, name, value):
        self.headers.append((name.lower().encode(), str(value).encode()))

    async def write_frame(self, payload):
        self.chunks.append(bytes(payload))


async def handle(scope, receive, send, headers):
    body = b""
    while True:
        event = await receive()
        if event["type"] == "http.request":
            body += event.get("body", b"")
            if not event.get("more_body", False):
                break

    # Normalize to the multi-value header shape the core expects.
    multi = {k: [v] for k, v in headers.items()}
    method = scope.get("method", "POST")
    multi.setdefault(":method", [method])

    from easyrpc import Request
    writer = _AsgiWriter()
    await dispatch(Request(url=scope["path"], headers=multi, body=body), METHODS, REG, writer)
    await send({"type": "http.response.start", "status": writer.status_code, "headers": writer.headers})
    await send({"type": "http.response.body", "body": b"".join(writer.chunks)})


async def asgi_app(scope, receive, send):
    if scope["type"] == "lifespan":
        # Minimal ASGI lifespan: acknowledge startup/shutdown so Hypercorn
        # (which otherwise waits for a startup event) can serve.
        while True:
            event = await receive()
            if event["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif event["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return
        return
    if scope["type"] != "http":
        return
    headers = {}
    for k, v in scope.get("headers", []):
        key = k.decode("latin1").lower()
        headers.setdefault(key, v.decode("latin1"))
    await handle(scope, receive, send, headers)
