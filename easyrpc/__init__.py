"""easy-rpc Python core: zero-runtime-bindings Transport + Connect wire
(unary + server-stream). Bridges adapt a concrete HTTP runtime.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import AsyncIterator, Callable, Dict, List, Optional

import asyncio
import json
import httpx


@dataclass
class RPCError(Exception):
    code: int = 13
    message: str = ""
    def __str__(self) -> str:
        return f"easyrpc: code={self.code} {self.message}"


@dataclass
class Request:
    url: str
    method: str = "POST"
    headers: Dict[str, List[str]] = field(default_factory=dict)
    body: Optional[bytes] = None
    # Local cancellation channel (asyncio.Event). Adapters that support abort
    # honour it; others ignore it.
    abort: Optional["asyncio.Event"] = None


@dataclass
class Response:
    status: int = 0
    headers: Dict[str, List[str]] = field(default_factory=dict)
    body: bytes = b""
    error: Optional[RPCError] = None


def result_aclosetask(client):
    async def _close():
        await client.aclose()
    return _close


def http_status(code: int) -> int:
    return {1: 499, 3: 400, 4: 504, 5: 404, 6: 409, 7: 403,
            8: 429, 9: 400, 10: 409, 11: 400, 12: 501,
            14: 503, 16: 401}.get(code, 500)


def rpc_error_from(status: int, headers, body) -> "RPCError":
    """Reconstruct an RPCError from the server's `connect-code`/`connect-error`
    headers (the HTTP status alone is lossy)."""
    code = None
    try:
        code = headers.get("connect-code")
    except Exception:
        code = None
    if code is not None:
        try:
            return RPCError(int(code), str(headers.get("connect-error", "")))
        except (TypeError, ValueError):
            pass
    c2, m2 = decode_error_json(body if isinstance(body, (bytes, bytearray)) else b"")
    if c2 != 0:
        return RPCError(c2, m2)
    text = body.decode() if isinstance(body, (bytes, bytearray)) else str(body)
    return RPCError(connect_from_status(status), text)


def connect_from_status(status: int) -> int:
    return {400: 3, 404: 5, 403: 7, 401: 16, 429: 8,
            503: 14, 409: 10, 504: 4, 501: 12, 499: 1}.get(status, 13)


FLAG_END_STREAM = 0x02

# Connect code -> stable lowercase wire name.
CODE_NAMES = {
    0: "ok", 1: "canceled", 2: "unknown", 3: "invalid_argument",
    4: "deadline_exceeded", 5: "not_found", 6: "already_exists",
    7: "permission_denied", 8: "resource_exhausted", 9: "failed_precondition",
    10: "aborted", 11: "out_of_range", 12: "unimplemented", 13: "internal",
    14: "unavailable", 15: "data_loss", 16: "unauthenticated",
}
CODE_BY_NAME = {v: k for k, v in CODE_NAMES.items()}


def code_to_string(code: int) -> str:
    return CODE_NAMES.get(code, "unknown")


def code_from_string(name: str) -> int:
    return CODE_BY_NAME.get(name, 2)


def encode_end_stream(code: int, message: str) -> bytes:
    """Connect end-stream payload: `{"error":{"code":"<name>","message":"..."}}`;
    a clean end is empty."""
    if code == 0:
        return b""
    return json.dumps({"error": {"code": code_to_string(code), "message": message}}).encode("utf-8")


def decode_end_stream(payload: bytes) -> tuple:
    """Decode a Connect end-stream payload into (code, message); (0, '') clean."""
    if not payload:
        return (0, "")
    try:
        v = json.loads(payload.decode("utf-8"))
        e = v.get("error") if isinstance(v, dict) else None
        if not isinstance(e, dict):
            return (0, "")
        return (code_from_string(e.get("code", "unknown")), str(e.get("message", "")))
    except Exception:
        return (0, "")


def encode_error_json(code: int, message: str) -> bytes:
    """Connect unary error body `{code,message}`."""
    return json.dumps({"code": code_to_string(code), "message": message}).encode("utf-8")


def decode_error_json(body: bytes) -> tuple:
    """Parse a Connect unary error body; (0, '') when not an error body."""
    if not body:
        return (0, "")
    try:
        v = json.loads(body.decode("utf-8"))
        if isinstance(v, dict) and isinstance(v.get("code"), str):
            return (code_from_string(v["code"]), str(v.get("message", "")))
    except Exception:
        pass
    return (0, "")


def frame(payload: bytes, end: bool = False) -> bytes:
    flags = FLAG_END_STREAM if end else 0
    return bytes([flags]) + len(payload).to_bytes(4, "big") + payload


def gzip_compress(data: bytes) -> bytes:
    import gzip as _g
    return _g.compress(data)


def gzip_decompress(data: bytes) -> bytes:
    import gzip as _g
    try:
        return _g.decompress(data)
    except Exception:
        return data


def read_frame(buf: bytes) -> Optional[tuple]:
    if len(buf) < 5:
        return None
    flags = buf[0]
    length = int.from_bytes(buf[1:5], "big")
    if length > DEFAULT_MAX_MESSAGE_BYTES:
        raise RPCError(8, f"frame too large: {length} > {DEFAULT_MAX_MESSAGE_BYTES}")
    if len(buf) < 5 + length:
        return None
    payload = buf[5:5 + length]
    if flags & 0x01:
        payload = gzip_decompress(payload)
    return payload, bool(flags & FLAG_END_STREAM), 5 + length


HEADER_TIMEOUT = "connect-timeout-ms"
HEADER_PROTOCOL_VERSION = "connect-protocol-version"
HEADER_ACCEPT_ENCODING = "connect-accept-encoding"
ENCODING_GZIP = "gzip"
COMPRESS_MIN_BYTES = 1024
CONNECT_PROTOCOL_VERSION = "1"
DEFAULT_MAX_MESSAGE_BYTES = 4 * 1024 * 1024


def parse_timeout(value) -> int:
    """Parse the Connect timeout header into milliseconds (0 = none)."""
    if not value:
        return 0
    try:
        n = int(value)
    except (TypeError, ValueError):
        return 0
    return n if n > 0 else 0


def with_timeout(req: "Request", timeout_ms: int) -> "Request":
    if timeout_ms <= 0:
        return req
    req.headers[HEADER_TIMEOUT] = [str(timeout_ms)]
    return req


def url_for(pkg: str, svc: str, method: str) -> str:
    return f"/{pkg}.{svc}/{method}"


@dataclass
class MethodSpec:
    service: str
    name: str
    path: str
    http_method: str
    client_stream: bool
    server_stream: bool
    body: str = ""


class Transport:
    async def send(self, req: Request) -> Response:
        raise NotImplementedError

    async def open_stream(self, req: Request) -> "Stream":
        raise NotImplementedError

class Interceptor:
    """Wrap a Transport call: mutate the request (auth/metadata), impose a
    deadline, observe, or short-circuit. `next_` performs the call."""

    async def unary(self, req: "Request", next_) -> "Response":
        return await next_(req)

    async def stream(self, req: "Request", next_) -> "Stream":
        return await next_(req)


class _InterceptedTransport(Transport):
    def __init__(self, ics, inner):
        self._ics, self._inner = ics, inner

    def _wrap(self, req, i, call):
        if i >= len(self._ics):
            return call(req)
        return getattr(self._ics[i], "unary" if call.__name__ == "send" else "stream")(
            req, lambda r: self._wrap(r, i + 1, call)
        )

    async def send(self, req: "Request") -> "Response":
        async def call(r):
            return await self._inner.send(r)
        return await self._wrap(req, 0, call)

    async def open_stream(self, req: "Request") -> "Stream":
        async def call(r):
            return await self._inner.open_stream(r)
        return await self._wrap(req, 0, call)


def interceptors(inner: Transport, *ics: Interceptor) -> Transport:
    return _InterceptedTransport(list(ics), inner)


class MetadataInterceptor(Interceptor):
    def __init__(self, md):
        self._md = md

    def _aug(self, req):
        for k, v in self._md.items():
            req.headers.setdefault(k, list(v))
        return req

    async def unary(self, req, next_):
        return await next_(self._aug(req))

    async def stream(self, req, next_):
        return await next_(self._aug(req))


class TimeoutInterceptor(Interceptor):
    """Deadline: sets the Connect header and a local abort Event; the adapter
    races the call against it, so cancellation works on any adapter."""

    def __init__(self, timeout_ms: int):
        self._ms = timeout_ms

    async def _run(self, req, next_):
        if self._ms <= 0:
            return await next_(req)
        req = with_timeout(req, self._ms)
        ev = asyncio.Event()
        req.abort = ev
        try:
            return await asyncio.wait_for(next_(req), timeout=self._ms / 1000.0)
        except asyncio.TimeoutError:
            ev.set()
            raise RPCError(4, "deadline exceeded")

    async def unary(self, req, next_):
        return await self._run(req, next_)

    async def stream(self, req, next_):
        return await self._run(req, next_)



class Stream:
    async def __aiter__(self) -> AsyncIterator[bytes]:
        raise NotImplementedError

    def cancel(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class HttpxTransport(Transport):
    """httpx-based bridge (h1/h2 prior knowledge as available)."""

    def __init__(self, base: str = "", client: Optional[httpx.AsyncClient] = None):
        self.base = base.rstrip("/")
        self.client = client or httpx.AsyncClient(
            http2=True, timeout=httpx.Timeout(5.0, read=None))

    def _url(self, u: str) -> str:
        return self.base + u if u.startswith("/") else u

    async def send(self, req: Request) -> Response:
        headers = {k: v[0] for k, v in req.headers.items()}
        r = await self.client.request(
            req.method, self._url(req.url),
            headers=headers, content=req.body,
        )
        return Response(
            status=r.status_code,
            headers=dict(r.headers),
            body=r.content,
            error=rpc_error_from(r.status_code, r.headers, r.content) if r.status_code >= 300 else None,
        )

    async def open_stream(self, req: Request) -> "Stream":
        headers = {k: v[0] for k, v in req.headers.items()}
        # Streaming responses must NOT buffer and must not time out mid-stream:
        # build the request and send it with stream=True so we read frames as
        # they arrive (server-stream RPCs stay open for the session's lifetime).
        request = self.client.build_request(
            req.method, self._url(req.url), headers=headers, content=req.body,
        )
        # No read timeout: a server-stream stays open between frames.
        resp = await self.client.send(request, stream=True)
        if resp.status_code >= 300:
            body = await resp.aread()
            raise rpc_error_from(resp.status_code, resp.headers, body)

        it = resp.aiter_bytes()

        class _Stream(Stream):
            def __init__(self, aiter, close_fn):
                self._it = aiter
                self._close = close_fn
                self._acc = b""
                self._started = False
                self._gen = self._frames()

            async def _frames(self):
                async for chunk in self._it:
                    self._acc += chunk
                    while True:
                        step = read_frame(self._acc)
                        if step is None:
                            break
                        payload, end, consumed = step
                        self._acc = self._acc[consumed:]
                        if end:
                            code, message = decode_end_stream(payload)
                            if code != 0:
                                raise RPCError(code, message)
                            return
                        yield payload

            def __aiter__(self):
                return self

            async def __anext__(self):
                try:
                    return await self._gen.__anext__()
                except StopAsyncIteration:
                    raise StopAsyncIteration

            def cancel(self):
                self._close()

            def close(self):
                self._close()

        return _Stream(it, lambda: result_aclosetask(self.client))
