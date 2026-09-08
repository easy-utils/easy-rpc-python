"""easy-rpc Python core: zero-runtime-bindings Transport + Connect wire
(unary + server-stream). Bridges adapt a concrete HTTP runtime.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import AsyncIterator, Callable, Dict, List, Optional

import asyncio
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
    return {3: 400, 5: 404, 7: 403, 8: 429, 16: 401, 14: 503}.get(code, 500)


def connect_from_status(status: int) -> int:
    return {400: 3, 404: 5, 403: 7, 401: 16, 429: 8, 503: 14}.get(status, 13)


FLAG_END_STREAM = 0x02


def frame(payload: bytes, end: bool = False) -> bytes:
    flags = FLAG_END_STREAM if end else 0
    return bytes([flags]) + len(payload).to_bytes(4, "big") + payload


def read_frame(buf: bytes) -> Optional[tuple]:
    if len(buf) < 5:
        return None
    flags = buf[0]
    length = int.from_bytes(buf[1:5], "big")
    if len(buf) < 5 + length:
        return None
    return buf[5:5 + length], bool(flags & FLAG_END_STREAM), 5 + length


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
        self.client = client or httpx.AsyncClient(http2=True)

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
            error=RPCError(connect_from_status(r.status_code), r.text) if r.status_code >= 300 else None,
        )

    async def open_stream(self, req: Request) -> "Stream":
        headers = {k: v[0] for k, v in req.headers.items()}
        resp = await self.client.request(
            req.method, self._url(req.url), headers=headers, content=req.body,
        )
        if resp.status_code >= 300:
            raise RPCError(connect_from_status(resp.status_code), resp.text)

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
