"""easy-rpc Python conformance server (ASGI). Shared app used by both the
uvicorn (HTTP/1) and Hypercorn (h2c + HTTP/1) server entries.

Implements the Connect wire directly for unary + server-stream, with
proto/json content negotiation and metadata (headers) accessible to handlers.
"""
import json
import os
from urllib.parse import urlparse

from google.protobuf import json_format

from easyrpc.server import ServerRegistry
from easyrpc.conformance.v1 import conformance_pb2 as pb

PORT = int(os.environ.get("PORT", "18888"))
NAME = "conformance"


def handler():
    reg = ServerRegistry()

    def parse_msg(proto_class, raw, kind):
        if kind == "json":
            return json_format.Parse(raw.decode(), proto_class())
        return proto_class.FromString(raw)

    def ser_msg(msg, kind):
        if kind == "json":
            return json_format.MessageToJson(msg).encode()
        return msg.SerializeToString()

    reg.unary["Health"] = lambda _req, kind: ser_msg(pb.HealthResponse(ok=True, name=NAME), kind)
    reg.unary["Echo"] = lambda req, kind: ser_msg(
        pb.EchoResponse(output="echo:" + parse_msg(pb.EchoRequest, req, kind).input), kind
    )
    reg.unary["Fail"] = lambda req, kind: ser_msg(
        pb.FailResponse(ok=(parse_msg(pb.FailRequest, req, kind).message == "")), kind
    )
    reg.stream["Count"] = lambda _req, kind, emit: [
        emit(ser_msg(pb.CountResponse(index=i), kind)) for i in range(3)
    ]
    return reg


REG = handler()

# REST routes: path -> (is_stream, method name).
ROUTES = {
    "/v1/health": (False, "Health"),
    "/v1/echo": (False, "Echo"),
    "/v1/count": (True, "Count"),
    "/v1/fail": (False, "Fail"),
}


async def handle(scope, receive, send, headers):
    path = scope["path"]
    method = scope["method"]
    msg = ROUTES.get(path)
    if msg is None:
        await send({"type": "http.response.start", "status": 404, "headers": []})
        await send({"type": "http.response.body", "body": b""})
        return

    is_stream, name = msg
    kind = "json" if headers.get("content-type", "").startswith("application/json") else "proto"

    body = b""
    while True:
        event = await receive()
        if event["type"] == "http.request":
            body += event.get("body", b"")
            if not event.get("more_body", False):
                break

    if is_stream:
        h = REG.stream.get(name)
        if h is None:
            await send({"type": "http.response.start", "status": 404, "headers": []})
            await send({"type": "http.response.body", "body": b""})
            return
        frames = []
        h(body, kind, lambda p: frames.append(p))
        out = b"".join(bytes([0]) + len(p).to_bytes(4, "big") + p for p in frames)
        ct = "application/connect+json" if kind == "json" else "application/connect+proto"
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", ct.encode())]})
        await send({"type": "http.response.body", "body": out})
        return

    h = REG.unary.get(name)
    if h is None:
        await send({"type": "http.response.start", "status": 404, "headers": []})
        await send({"type": "http.response.body", "body": b""})
        return
    try:
        out = h(body, kind)
        ct = "application/json" if kind == "json" else "application/proto"
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", ct.encode())]})
        await send({"type": "http.response.body", "body": out})
    except Exception as e:
        await send({"type": "http.response.start", "status": 400, "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.body", "body": str(e).encode()})


async def asgi_app(scope, receive, send):
    headers = {}
    for k, v in scope.get("headers", []):
        key = k.decode("latin1").lower()
        headers.setdefault(key, v.decode("latin1"))
    await handle(scope, receive, send, headers)
