"""Fault injection (spec §4.2 M8/M10 + F2 end-to-end): a mock server emits
malformed stream bodies; the client MUST surface errors, never partial
payloads, never raw compressed bytes."""
import asyncio
import gzip as _gzip
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from easyrpc import RPCError, Request


def frame(payload: bytes, end: bool = False, compressed: bool = False) -> bytes:
    flags = (0x02 if end else 0) | (0x01 if compressed else 0)
    return bytes([flags]) + len(payload).to_bytes(4, "big") + payload


def serve(body: bytes) -> int:
    import http.server
    import threading

    class H(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers.get("content-length") or 0))
            self.send_response(200)
            self.send_header("content-type", "application/connect+proto")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    port = srv.server_address[1]
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return port


async def collect(port: int):
    from easyrpc import HttpxTransport
    t = HttpxTransport(base=f"http://127.0.0.1:{port}")
    st = await t.open_stream(Request(url="/x", headers={"content-type": ["application/connect+proto"]}))
    out = []
    async for p in st:
        out.append(p[0])
    return out


async def main():
    # F1: mid-frame truncation => RPCError
    body = frame(bytes([0])) + frame(bytes([1]))[:6]
    port = serve(body)
    try:
        await collect(port)
        raise SystemExit("F1 FAIL: expected error")
    except RPCError:
        print("F1 ok")

    # F2: close at frame boundary without END frame => RPCError(13)
    port = serve(frame(bytes([0])) + frame(bytes([1])))
    try:
        out = await collect(port)
        raise SystemExit(f"F2 FAIL: expected error, got {out}")
    except RPCError as e:
        assert e.code == 13, e
        print("F2 ok")

    # F3: garbage END payload => clean end after data frame
    port = serve(frame(bytes([0])) + frame(bytes([0xff, 0xfe, 0x42]), end=True))
    out = await collect(port)
    assert out == [0], out
    print("F3 ok")

    # F4: corrupt gzip => RPCError, never raw bytes
    corrupt = bytes([0x1f, 0x8b, 0x08, 0x00, 0xde, 0xad, 0xbe, 0xef])
    port = serve(frame(corrupt, compressed=True) + frame(b"", end=True))
    try:
        out = await collect(port)
        raise SystemExit(f"F4 FAIL: expected error, got {out}")
    except RPCError:
        print("F4 ok")

    # F6: valid gzip decodes
    port = serve(frame(_gzip.compress(bytes([7])), compressed=True) + frame(b"", end=True))
    out = await collect(port)
    assert out == [7], out
    print("F6 ok")

    print("PY_FAULT_OK")


if __name__ == "__main__":
    asyncio.run(main())
