"""easy-rpc Python transport selector.

Realm:
  - "std"  -> httpx (HTTP/1 + HTTP/2/h2c). Minimal dependencies.
  - "auto" -> httpx + aioquic (HTTP/3). Negotiates h3 -> h2/h2c -> h1.
"""
from __future__ import annotations

from easyrpc import HttpxTransport, Transport, Request, Response
from easyrpc.aioquic_bridge import AioquicTransport, h3_available


def default_client(base: str = "", realm: str = "std") -> Transport:
    """Return a Transport for the given realm.

    realm='std'  -> HttpxTransport (h1 + h2/h2c), no QUIC dependency.
    realm='auto' -> AioquicTransport when aioquic is installed (h3), falling
                    back to HttpxTransport (h2/h1) otherwise/on failure.
    """
    if realm == "std":
        return HttpxTransport(base=base)
    if realm == "auto":
        if h3_available():
            return _AutoTransport(base)
        return HttpxTransport(base=base)
    raise ValueError(f"unknown realm: {realm}")


class _AutoTransport(Transport):
    """Automatically pick h3 when the endpoint supports it, else h2/h1."""

    def __init__(self, base: str = ""):
        self.base = base
        self._h3 = AioquicTransport(base=base)
        self._httpx = HttpxTransport(base=base)

    async def send(self, req: Request) -> Response:
        if (req.url.startswith("https://") and self._is_h3_url(req.url)):
            try:
                return await self._h3.send(req)
            except Exception:
                pass
        return await self._httpx.send(req)

    async def open_stream(self, req) -> "Stream":
        if req.url.startswith("https://") and self._is_h3_url(req.url):
            try:
                return await self._h3.open_stream(req)
            except Exception:
                pass
        return await self._httpx.open_stream(req)

    @staticmethod
    def _is_h3_url(url: str) -> bool:
        # aioquic h3 client requires TLS/ALPN h3; https endpoints may negotiate.
        return True
