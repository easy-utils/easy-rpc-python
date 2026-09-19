"""easy-rpc Python server core: push-based ASGI dispatch for the Connect wire
subset (unary + server-stream, proto only, POST only).

`dispatch` decodes an RPC request, runs the handler, and PUSHES the response
into a `ResponseWriter`. A runtime adapter (aiohttp, uvicorn/hypercorn ASGI, ...)
implements the writer and flushes each frame, so server-stream RPCs reach the
client incrementally — never buffered.
"""
from __future__ import annotations

from typing import Awaitable, Callable, Dict, List, Protocol

from . import RPCError, Request, frame, read_frame, http_status, MethodSpec  # noqa: F401
from . import (
    FLAG_END_STREAM, encode_end_stream, decode_end_stream, encode_error_json,  # noqa: F401
    HEADER_PROTOCOL_VERSION, CONNECT_PROTOCOL_VERSION, DEFAULT_MAX_MESSAGE_BYTES,
    ENCODING_GZIP, COMPRESS_MIN_BYTES, gzip_compress,
    HandlerContext, mux_trailers, gzip_decompress,
    content_kind_of, is_stream_content_type, content_type_for,
)

CONTENT_TYPE_UNARY = "application/proto"
CONTENT_TYPE_STREAM = "application/connect+proto"
HEADER_STREAM_ACCEPT_ENCODING = "connect-accept-encoding"
HEADER_CONTENT_ENCODING = "content-encoding"
HEADER_ACCEPT_ENCODING = "accept-encoding"

UnaryHandler = Callable[[bytes, HandlerContext], Awaitable[bytes]]
StreamHandler = Callable[[bytes, HandlerContext, Callable[[bytes, bool], "Awaitable[None]"]], "Awaitable[None]"]


class ResponseWriter(Protocol):
    """Push-based server sink. Adapters implement this for their transport."""

    def status(self, code: int) -> None: ...

    def header(self, name: str, value: str) -> None: ...

    async def write_frame(self, payload: bytes) -> None: ...


class ServerRegistry:
    def __init__(self) -> None:
        self.unary: Dict[str, UnaryHandler] = {}
        self.stream: Dict[str, StreamHandler] = {}


def _read_single_frame(body: bytes) -> bytes:
    """Unframe the enveloped server-stream request (one data frame)."""
    step = read_frame(body)
    if step is None:
        raise RPCError(13, "stream request: truncated frame")
    payload, _end, _consumed = step
    return payload


async def dispatch(
    req: Request,
    methods: "List[MethodSpec]",
    reg: ServerRegistry,
    w: ResponseWriter,
) -> None:
    """Decode + run + push. Server-stream is always HTTP 200; failures ride the
    END frame. Unary resolves fully before writing so errors set a real status."""
    path = req.url.split("?", 1)[0]
    ct = req.headers.get("content-type", [""])[0]
    pv = req.headers.get(HEADER_PROTOCOL_VERSION, [""])[0]
    if pv and pv != CONNECT_PROTOCOL_VERSION:
        return await _write_error(w, RPCError(12, f"unsupported connect-protocol-version: {pv}"))

    # POST-only (spec §0). A non-POST verb is 405 (code 2).
    verb = req.headers.get(":method", [""])[0]
    if verb and verb != "POST":
        return await _write_error(w, RPCError(2, f"method {verb} not allowed"), status=405)

    if len(req.body or b"") > DEFAULT_MAX_MESSAGE_BYTES:
        return await _write_error(w, RPCError(8, "request too large"))

    spec = next((m for m in methods if m.path == path), None)
    # Unknown path -> 404 with code 12 (unimplemented), matching Connect.
    if spec is None:
        return await _write_error(w, RPCError(12, "unimplemented"), status=404)

    # codec + shape negotiation (spec §2): proto (default) or proto3 JSON; the
    # content type also encodes the shape, which must match the method.
    kind = content_kind_of(ct)
    stream_shape = is_stream_content_type(ct)
    if not kind or stream_shape != spec.server_stream:
        return await _write_error(w, RPCError(2, f"unsupported content-type: {ct}"), status=415)

    ctx = HandlerContext(headers={k: list(v) for k, v in req.headers.items()}, kind=kind)

    if spec.server_stream:
        h = reg.stream.get(spec.name)
        if h is None:
            return await _stream_fail(w, RPCError(12, "no handler"), kind)
        # Request compression for streams uses `connect-content-encoding`.
        req_enc = req.headers.get("connect-content-encoding", [""])[0].strip().lower()
        if req_enc and req_enc != "identity" and req_enc != ENCODING_GZIP:
            return await _stream_fail(w, RPCError(12, f"unsupported content-encoding: {req_enc}"), kind)
        # A server-stream request MUST carry exactly one enveloped message;
        # zero frames or more than one => unimplemented (Connect semantics).
        try:
            frame_count = _count_frames(req.body or b"")
        except RPCError as e:
            return await _stream_fail(w, e, kind)
        if frame_count != 1:
            msg = "missing request message" if frame_count == 0 else \
                "server-stream request must contain exactly one message"
            return await _stream_fail(w, RPCError(12, msg), kind)
        try:
            body = _read_single_frame(req.body or b"")
        except RPCError as e:
            return await _stream_fail(w, e, kind)
        # Connect semantics: stream is always HTTP 200; failures ride the END
        # frame. Handler headers are applied lazily on the first emit.
        w.status(200)
        w.header("content-type", content_type_for(True, kind))
        ended = False
        headers_applied = False

        def apply_headers() -> None:
            nonlocal headers_applied
            if headers_applied:
                return
            headers_applied = True
            for k, vs in ctx.response_headers.items():
                if k == "content-type":
                    continue
                for v in vs:
                    w.header(k, v)

        wants_gzip = any(
            ENCODING_GZIP in [x.strip() for x in v.split(",")]
            for v in req.headers.get(HEADER_STREAM_ACCEPT_ENCODING, [])
        )
        if wants_gzip:
            w.header("connect-content-encoding", ENCODING_GZIP)

        async def emit(payload: bytes, end: bool) -> None:
            nonlocal ended
            if ended:
                return
            apply_headers()
            if end:
                ended = True
                await w.write_frame(frame(encode_end_stream(0, "", None, ctx.trailers), True))
            elif wants_gzip and len(payload) >= COMPRESS_MIN_BYTES:
                z = gzip_compress(payload)
                await w.write_frame(bytes([0x01]) + len(z).to_bytes(4, "big") + z)
            else:
                await w.write_frame(frame(payload, False))

        try:
            await h(body, ctx, emit)
        except Exception as e:  # noqa: BLE001
            err = e if isinstance(e, RPCError) else RPCError(13, str(e))
            apply_headers()
            if not ended:
                ended = True
                await w.write_frame(frame(encode_end_stream(err.code, err.message, err.details, ctx.trailers), True))
            return
        apply_headers()
        if not ended:
            await w.write_frame(frame(encode_end_stream(0, "", None, ctx.trailers), True))
        return

    h = reg.unary.get(spec.name)
    if h is None:
        return await _write_error(w, RPCError(5, "no handler"))
    # Request compression (spec §3.5): unary uses `Content-Encoding: gzip`.
    in_body = req.body or b""
    req_enc = req.headers.get(HEADER_CONTENT_ENCODING, [""])[0].strip().lower()
    if req_enc and req_enc != ENCODING_GZIP:
        return await _write_error(w, RPCError(12, f"unsupported content-encoding: {req_enc}"))
    if req_enc == ENCODING_GZIP and in_body:
        try:
            in_body = gzip_decompress(in_body)
        except RPCError as e:
            return await _write_error(w, e)
    try:
        out = await h(in_body, ctx)
    except Exception as e:  # noqa: BLE001
        err = e if isinstance(e, RPCError) else RPCError(13, str(e))
        return await _write_error(w, err, trailers=ctx.trailers, extra=ctx.response_headers)
    w.status(200)
    # Unary gzip (spec §3.5): compress when the client accepts gzip.
    wants_gzip = any(
        ENCODING_GZIP in [x.strip() for x in v.split(",")]
        for v in req.headers.get(HEADER_ACCEPT_ENCODING, [])
    )
    headers = {"content-type": content_type_for(False, kind)}
    if wants_gzip and len(out) >= COMPRESS_MIN_BYTES:
        out = gzip_compress(out)
        headers[HEADER_CONTENT_ENCODING] = ENCODING_GZIP
    for k, vs in ctx.response_headers.items():
        if k == "content-type":
            continue
        for v in vs:
            w.header(k, v)
    w.header("content-type", headers["content-type"])
    if HEADER_CONTENT_ENCODING in headers:
        w.header(HEADER_CONTENT_ENCODING, ENCODING_GZIP)
    for k, v in mux_trailers({}, ctx.trailers).items():
        if k == "content-type":
            continue
        for item in (v if isinstance(v, (list, tuple)) else [v]):
            w.header(k, item)
    await w.write_frame(out)


async def _write_error(w: ResponseWriter, err: RPCError, status: int = None, trailers: dict = None,
                       extra: dict = None) -> None:
    w.status(status if status is not None else http_status(err.code))
    for k, vs in (extra or {}).items():
        if k == "content-type":
            continue
        for v in vs:
            w.header(k, v)
    w.header("content-type", "application/json")
    for k, v in mux_trailers({}, trailers or {}).items():
        for item in (v if isinstance(v, (list, tuple)) else [v]):
            w.header(k, item)
    await w.write_frame(encode_error_json(err.code, err.message, err.details))


async def _stream_fail(w: ResponseWriter, err: RPCError, kind: str = "proto") -> None:
    """Server-stream failure: HTTP 200 + END frame carrying the error."""
    w.status(200)
    w.header("content-type", content_type_for(True, kind))
    await w.write_frame(frame(encode_end_stream(err.code, err.message, err.details), True))


def _count_frames(body: bytes) -> int:
    off = 0
    n = 0
    while off < len(body):
        if off + 5 > len(body):
            raise RPCError(13, "truncated frame header")
        length = int.from_bytes(body[off + 1:off + 5], "big")
        if length > DEFAULT_MAX_MESSAGE_BYTES:
            raise RPCError(8, f"frame too large: {length}")
        off += 5 + length
        n += 1
    return n


# ---- aiohttp adapter ----

def make_app(routes: Dict[str, tuple], methods: "List[MethodSpec]") -> "object":
    """Build an aiohttp application. `routes` maps path -> (is_stream, name, reg)."""
    from aiohttp import web

    app = web.Application()

    async def handler(request: web.Request) -> web.StreamResponse:
        path = request.path
        entry = routes.get(path)
        if entry is None:
            return web.Response(status=404)
        is_stream, name, reg = entry
        body = await request.read()
        method = MethodSpec(service="", name=name, path=path,
                            client_stream=False, server_stream=is_stream)
        resp = web.StreamResponse(
            status=200,
            headers={"Content-Type": CONTENT_TYPE_STREAM if is_stream else CONTENT_TYPE_UNARY},
        )
        await resp.prepare(request)

        state = {"status": 200}

        class _W:
            def status(self, code: int) -> None:
                state["status"] = code

            def header(self, name: str, value: str) -> None:
                resp.headers[name] = value

            async def write_frame(self, payload: bytes) -> None:
                await resp.write(payload)

        await dispatch(
            Request(url=path,
                    headers={k.lower(): [v] for k, v in request.headers.items()},
                    body=body),
            [method],
            reg,
            _W(),
        )
        await resp.write_eof()
        return resp

    for path, (is_stream, name, reg) in routes.items():
        app.router.add_post(path, handler)
    return app


def run_server(app, host: str = "127.0.0.1", port: int = 18888) -> None:
    from aiohttp import web
    web.run_app(app, host=host, port=port)
