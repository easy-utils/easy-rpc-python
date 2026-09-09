"""easy-rpc Python AioquicTransport: HTTP/3 (QUIC) client bridge.

Requires the optional `aioquic` dependency. AioquicTransport implements the
core Transport interface for unary + server-stream over HTTP/3. Use via
``default_client(realm='auto')`` which combines this with the std httpx bridge
and negotiates h3 -> h2/h2c -> h1 automatically.
"""
from __future__ import annotations

import asyncio
import urllib.parse
from typing import Optional

try:
    from aioquic.asyncio import connect
    from aioquic.asyncio.protocol import QuicConnectionProtocol
    from aioquic.h3.connection import H3Connection
    from aioquic.h3.events import DataReceived, HeadersReceived
    from aioquic.quic.configuration import QuicConfiguration
    _H3_AVAILABLE = True
except Exception:  # pragma: no cover - optional dep
    _H3_AVAILABLE = False

from easyrpc import Request, Response, Stream, RPCError, read_frame


def h3_available() -> bool:
    return _H3_AVAILABLE


class _H3ClientProtocol(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._http = H3Connection(self._quic)
        self._request_promise: asyncio.Future = asyncio.get_event_loop().create_future()
        self._stream_events: dict[int, list[dict]] = {}
        self._stream_futures: dict[int, asyncio.Future] = {}

    async def request(self, method: str, url: str, headers: dict, data: bytes) -> Response:
        parsed = urllib.parse.urlparse(url)
        authority = parsed.netloc
        path = parsed.path + ("?" + parsed.query if parsed.query else "")
        stream_id = self._quic.get_next_available_stream_id()

        hdrs = [(b":method", method.encode()), (b":scheme", b"https"),
                (b":authority", authority.encode()), (b":path", path.encode())]
        for k, v in headers.items():
            hdrs.append((k.lower().encode(), v.encode()))
        self._http.send_headers(stream_id, hdrs, end_stream=(len(data) == 0))
        if data:
            self._http.send_data(stream_id, data, end_stream=True)
        self.transmit()

        fut = asyncio.get_event_loop().create_future()
        self._stream_futures[stream_id] = fut
        return await fut

    def quic_event_received(self, event):
        for http_event in self._http.handle_event(event):
            if isinstance(http_event, HeadersReceived):
                self._stream_events.setdefault(http_event.stream_id, []).append({
                    "headers": dict(http_event.headers), "type": "headers"})
            elif isinstance(http_event, DataReceived):
                self._stream_events.setdefault(http_event.stream_id, []).append(
                    {"data": http_event.data, "type": "data"})
                if http_event.stream_id in self._stream_futures:
                    fut = self._stream_futures.pop(http_event.stream_id)
                    evs = self._stream_events.pop(http_event.stream_id, [])
                    if not fut.done():
                        fut.set_result(evs)

    def transmit(self):
        self._quic.transmit()


class AioquicTransport:
    """HTTP/3 Transport built on aioquic. Unary + server-stream. h3 only."""

    def __init__(self, base: str = "", verify: bool = True):
        if not _H3_AVAILABLE:
            raise RuntimeError("aioquic is not installed; install it for h3 support")
        self.base = base.rstrip("/")
        self.verify = verify

    def _url(self, u: str) -> str:
        if u.startswith("https://") or u.startswith("http://"):
            return u
        return self.base + u

    async def send(self, req: Request) -> Response:
        url = self._url(req.url)
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        port = parsed.port or 443

        config = QuicConfiguration(is_client=True, alpn_protocols=["h3"])
        if not self.verify:
            config.verify_mode = False

        async with connect(host, port, configuration=config, create_protocol=_H3ClientProtocol) as proto:
            headers = {k: v[0] for k, v in (req.headers or {}).items()}
            resp = await proto.request(req.method, url, headers, req.body or b"")
            return self._resp(resp)

    async def open_stream(self, req: Request) -> Stream:
        url = self._url(req.url)
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        port = parsed.port or 443
        config = QuicConfiguration(is_client=True, alpn_protocols=["h3"])
        if not self.verify:
            config.verify_mode = False

        async with connect(host, port, configuration=config, create_protocol=_H3ClientProtocol) as proto:
            headers = {k: v[0] for k, v in (req.headers or {}).items()}
            stream_id = proto._quic.get_next_available_stream_id()
            hdrs = [(b":method", req.method.encode()), (b":scheme", b"https"),
                    (b":authority", parsed.netloc.encode()), (b":path",
                    (parsed.path + ("?" + parsed.query if parsed.query else "")).encode())]
            for k, v in headers.items():
                hdrs.append((k.lower().encode(), v.encode()))
            proto._http.send_headers(stream_id, hdrs, end_stream=False)
            if req.body:
                proto._http.send_data(stream_id, req.body, end_stream=False)
            proto.transmit()

            async def _iter():
                buffer = b""
                while True:
                    if stream_id in proto._stream_events and proto._stream_events[stream_id]:
                        evs = proto._stream_events[stream_id]
                        for ev in evs:
                            if ev["type"] == "data":
                                buffer += ev["data"]
                                while True:
                                    step = read_frame(buffer)
                                    if step is None:
                                        break
                                    payload, end, consumed = step
                                    buffer = buffer[consumed:]
                                    if end:
                                        return
                                    yield payload
                        proto._stream_events[stream_id] = []
                    else:
                        if stream_id in proto._stream_futures:
                            evs = await proto._stream_futures.pop(stream_id)
                        else:
                            await asyncio.sleep(0.05)
            return _Stream(_iter())

    @staticmethod
    def _resp(evs) -> Response:
        status = 0
        body = b""
        headers = {}
        for ev in evs:
            if ev["type"] == "headers":
                for k, v in ev["headers"].items():
                    k = k.decode() if isinstance(k, bytes) else k
                    v = v.decode() if isinstance(v, bytes) else v
                    headers[k] = v
                    if k == ":status":
                        status = int(v)
            elif ev["type"] == "data":
                body += ev["data"]
        return Response(status=status, headers=headers, body=body,
                        error=RPCError(connect_from_status(status), body.decode(errors="ignore")) if status >= 300 else None)


class _Stream(Stream):
    def __init__(self, agen):
        self._agen = agen

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self._agen.__anext__()

    def cancel(self):
        pass

    def close(self):
        pass


def connect_from_status(status: int) -> int:
    return {400: 3, 404: 5, 403: 7, 401: 16, 429: 8, 503: 14}.get(status, 13)
