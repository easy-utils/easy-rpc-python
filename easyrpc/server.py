"""easy-rpc Python server core: push-based ASGI dispatch + JSON/proto content
negotiation.

`Dispatch` decodes an RPC request, runs the handler, and PUSHES the response
into a `ResponseWriter` as it is produced. A runtime adapter (aiohttp,
uvicorn/hypercorn ASGI, ...) implements the writer and flushes each frame, so
server-stream RPCs reach the client incrementally — never buffered.
"""
from __future__ import annotations

from typing import Awaitable, Callable, Dict, Optional, Protocol

from . import RPCError, Request, frame, read_frame, http_status, MethodSpec  # noqa: F401
from . import (
    FLAG_END_STREAM, encode_end_stream, decode_end_stream, encode_error_json,  # noqa: F401
    HEADER_PROTOCOL_VERSION, CONNECT_PROTOCOL_VERSION, DEFAULT_MAX_MESSAGE_BYTES,
)

ContentKind = str  # 'proto' | 'json'

UnaryHandler = Callable[[bytes, str], bytes]
StreamHandler = Callable[[bytes, str, Callable[[bytes, bool], "Awaitable[None]"]], "Awaitable[None]"]


class ResponseWriter(Protocol):
    """Push-based server sink. Adapters implement this for their transport."""

    def status(self, code: int) -> None: ...

    def header(self, name: str, value: str) -> None: ...

    async def write_frame(self, payload: bytes) -> None: ...


class ServerRegistry:
    def __init__(self) -> None:
        self.unary: Dict[str, UnaryHandler] = {}
        self.stream: Dict[str, StreamHandler] = {}


def detect_kind(ct: str) -> ContentKind:
    return "json" if ct.startswith("application/json") else "proto"


def stream_content(kind: ContentKind) -> str:
    return "application/connect+json" if kind == "json" else "application/connect+proto"


def _content(kind: ContentKind) -> str:
    return "application/json" if kind == "json" else "application/proto"


async def dispatch(
    req: Request,
    methods: "list[MethodSpec]",
    reg: ServerRegistry,
    w: ResponseWriter,
) -> None:
    """Decode + run + push. Server-stream is always HTTP 200; failures ride the
    END frame. Unary resolves fully before writing so errors set a real status."""
    path = req.url.split("?", 1)[0]
    ct = req.headers.get("content-type", [""])[0]
    kind = detect_kind(ct)
    pv = req.headers.get(HEADER_PROTOCOL_VERSION, [""])[0]
    if pv and pv != CONNECT_PROTOCOL_VERSION:
        return await _write_error(w, RPCError(12, f"unsupported connect-protocol-version: {pv}"))
    if len(req.body or b"") > DEFAULT_MAX_MESSAGE_BYTES:
        return await _write_error(w, RPCError(8, "request too large"))

    spec = next((m for m in methods if m.path == path), None)
    if spec is None:
        return await _write_error(w, RPCError(5, "not found"))
    if spec.server_stream:
        h = reg.stream.get(spec.name)
        if h is None:
            return await _write_error(w, RPCError(5, "no handler"))
        w.status(200)
        w.header("content-type", stream_content(kind))
        ended = False

        async def emit(payload: bytes, end: bool) -> None:
            nonlocal ended
            if ended:
                return
            if end:
                ended = True
                await w.write_frame(frame(b"", True))
            else:
                await w.write_frame(frame(payload, False))

        try:
            await h(req.body or b"", kind, emit)
        except Exception as e:  # noqa: BLE001
            err = e if isinstance(e, RPCError) else RPCError(13, str(e))
            if not ended:
                ended = True
                await w.write_frame(frame(encode_end_stream(err.code, err.message), True))
            return
        if not ended:
            await w.write_frame(frame(b"", True))
        return

    h = reg.unary.get(spec.name)
    if h is None:
        return await _write_error(w, RPCError(5, "no handler"))
    try:
        out = h(req.body or b"", kind)
    except Exception as e:  # noqa: BLE001
        err = e if isinstance(e, RPCError) else RPCError(13, str(e))
        return await _write_error(w, err)
    w.status(200)
    w.header("content-type", _content(kind))
    await w.write_frame(out)


async def _write_error(w: ResponseWriter, err: RPCError) -> None:
    w.status(http_status(err.code))
    w.header("content-type", "application/json")
    await w.write_frame(encode_error_json(err.code, err.message))


# ---- aiohttp adapter ----

def make_app(routes: Dict[str, tuple], methods: "list[MethodSpec]") -> "object":
    """Build an aiohttp application. `routes` maps path -> (is_stream, name, reg)
    (kept for backward compatibility with the conformance server)."""
    from aiohttp import web

    app = web.Application()

    async def handler(request: web.Request) -> web.StreamResponse:
        path = request.path
        entry = routes.get(path)
        if entry is None:
            return web.Response(status=404)
        is_stream, name, reg = entry
        kind = detect_kind(request.headers.get("Content-Type", ""))
        body = await request.read()
        method = MethodSpec(service="", name=name, path=path, http_method="POST",
                            client_stream=False, server_stream=is_stream)
        resp = web.StreamResponse(
            status=200,
            headers={"Content-Type": stream_content(kind) if is_stream else _content(kind)},
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
            Request(url=path, method="POST",
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
