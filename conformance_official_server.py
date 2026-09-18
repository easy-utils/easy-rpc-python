"""Official ConnectRPC conformance server (easy-rpc Python).

Server-under-test mode: reads a size-prefixed ServerCompatRequest from stdin,
starts an ASGI server (Hypercorn, so HTTP/1 and h2c share one port) on an
ephemeral port implementing the official
connectrpc.conformance.v1.ConformanceService, then writes the size-prefixed
ServerCompatResponse to stdout. Validates easy-rpc against the real
`connectconformance` runner.
"""
import asyncio
import socket
import struct
import sys

from google.protobuf.any_pb2 import Any
from hypercorn.asyncio import serve
from hypercorn.config import Config

from easyrpc import MethodSpec, RPCError, ErrorDetail, HandlerContext
from easyrpc.server import ServerRegistry, dispatch
from connectrpc.conformance.v1 import service_pb2 as pb
from connectrpc.conformance.v1 import server_compat_pb2 as sc

HEADER_TIMEOUT = "connect-timeout-ms"
SVC = "connectrpc.conformance.v1.ConformanceService"

METHODS = [
    MethodSpec(SVC, "Unary", f"/{SVC}/Unary", False, False),
    MethodSpec(SVC, "ServerStream", f"/{SVC}/ServerStream", False, True),
    MethodSpec(SVC, "ClientStream", f"/{SVC}/ClientStream", True, False),
    MethodSpec(SVC, "BidiStream", f"/{SVC}/BidiStream", True, True),
    MethodSpec(SVC, "Unimplemented", f"/{SVC}/Unimplemented", False, False),
    MethodSpec(SVC, "IdempotentUnary", f"/{SVC}/IdempotentUnary", False, False),
]


def _request_any(msg, name: str) -> Any:
    a = Any()
    a.Pack(msg, type_url_prefix="type.googleapis.com/")
    return a


def _make_request_info(headers, requests):
    info = pb.ConformancePayload.RequestInfo(requests=requests)
    for k, vs in headers.items():
        info.request_headers.add(name=k, value=list(vs))
    raw = headers.get(HEADER_TIMEOUT, [None])[0]
    if raw:
        try:
            info.timeout_ms = int(raw)
        except ValueError:
            pass
    return info


def _to_rpc_error(err, info):
    details = []
    for a in err.details:
        # Connect's wire `type` is the BARE type name (part after the last '/').
        details.append(ErrorDetail(a.type_url.rsplit("/", 1)[-1], a.value))
    if info is not None:
        details.append(ErrorDetail(
            "connectrpc.conformance.v1.ConformancePayload.RequestInfo",
            info.SerializeToString(),
        ))
    return RPCError(err.code, err.message or "", details)


def _apply_headers(items, ctx, trailer: bool) -> None:
    for h in items:
        for v in h.value:
            if trailer:
                ctx.set_trailer(h.name, v)
            else:
                ctx.set_header(h.name, v)


def build_registry() -> ServerRegistry:
    reg = ServerRegistry()

    async def do_unary(req_bytes, ctx):
        m = pb.UnaryRequest.FromString(req_bytes)
        info = _make_request_info(ctx.headers, [_request_any(m, "UnaryRequest")])
        def_ = m.response_definition
        payload = pb.ConformancePayload(request_info=info)
        if def_ is None:
            return pb.UnaryResponse(payload=payload).SerializeToString()
        _apply_headers(def_.response_headers, ctx, False)
        _apply_headers(def_.response_trailers, ctx, True)
        if def_.WhichOneof("response") == "error":
            raise _to_rpc_error(def_.error, info)
        if def_.response_delay_ms:
            await asyncio.sleep(def_.response_delay_ms / 1000.0)
        payload.data = def_.response_data
        return pb.UnaryResponse(payload=payload).SerializeToString()

    async def do_idempotent(req_bytes, ctx):
        m = pb.IdempotentUnaryRequest.FromString(req_bytes)
        info = _make_request_info(ctx.headers, [_request_any(m, "IdempotentUnaryRequest")])
        return pb.IdempotentUnaryResponse(payload=pb.ConformancePayload(request_info=info)).SerializeToString()

    async def do_unimplemented(_req_bytes, _ctx):
        raise RPCError(12, "unimplemented")

    async def do_client_stream(_req_bytes, _ctx):
        raise RPCError(12, "client streaming is not supported")

    async def do_server_stream(req_bytes, ctx, emit):
        m = pb.ServerStreamRequest.FromString(req_bytes)
        info = _make_request_info(ctx.headers, [_request_any(m, "ServerStreamRequest")])
        def_ = m.response_definition
        if def_ is None:
            return
        _apply_headers(def_.response_headers, ctx, False)
        _apply_headers(def_.response_trailers, ctx, True)
        first = True
        for data in def_.response_data:
            if def_.response_delay_ms:
                await asyncio.sleep(def_.response_delay_ms / 1000.0)
            payload = pb.ConformancePayload(data=data)
            if first:
                payload.request_info.CopyFrom(info)
            await emit(pb.ServerStreamResponse(payload=payload).SerializeToString(), False)
            first = False
        if def_.HasField("error"):
            raise _to_rpc_error(def_.error, info if first else None)

    async def do_bidi(_req_bytes, _ctx, _emit):
        raise RPCError(12, "bidi streaming is not supported")

    reg.unary["Unary"] = do_unary
    reg.unary["IdempotentUnary"] = do_idempotent
    reg.unary["Unimplemented"] = do_unimplemented
    reg.unary["ClientStream"] = do_client_stream
    reg.stream["ServerStream"] = do_server_stream
    reg.stream["BidiStream"] = do_bidi
    return reg


REG = build_registry()


async def asgi_app(scope, receive, send):
    if scope["type"] != "http":
        return
    headers = {}
    for k, v in scope.get("headers", []):
        key = k.decode("latin1").lower()
        headers.setdefault(key, []).append(v.decode("latin1"))
    method = scope.get("method", "POST")
    headers.setdefault(":method", [method])

    body = b""
    while True:
        event = await receive()
        if event["type"] == "http.request":
            body += event.get("body", b"")
            if not event.get("more_body", False):
                break

    state = {"status": 200}
    chunks = []
    out_headers = []

    class Writer:
        def status(self, code):
            state["status"] = code

        def header(self, name, value):
            out_headers.append((name.lower().encode(), str(value).encode()))

        async def write_frame(self, payload):
            chunks.append(bytes(payload))

    from easyrpc import Request

    await dispatch(
        Request(url=scope["path"], headers=headers, body=body),
        METHODS, REG, Writer(),
    )
    await send({"type": "http.response.start", "status": state["status"], "headers": out_headers})
    await send({"type": "http.response.body", "body": b"".join(chunks)})


async def main():
    # Size-prefixed ServerCompatRequest on stdin.
    header = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.buffer.read, 4)
    if len(header) < 4:
        return
    (n,) = struct.unpack(">I", header)
    body = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.buffer.read, n)
    req = sc.ServerCompatRequest.FromString(body)

    # Pick an ephemeral port by binding then releasing it, then let Hypercorn
    # bind that port (Hypercorn's serve() does not accept pre-made sockets).
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    resp = sc.ServerCompatResponse(host="127.0.0.1", port=port)
    out = resp.SerializeToString()
    sys.stdout.buffer.write(struct.pack(">I", len(out)) + out)
    sys.stdout.buffer.flush()

    config = Config()
    config.bind = [f"127.0.0.1:{port}"]
    config.accesslog = None
    await serve(asgi_app, config)


if __name__ == "__main__":
    asyncio.run(main())
