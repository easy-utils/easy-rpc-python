"""easy-rpc Python conformance server (ASGI). Shared app used by both the
uvicorn (HTTP/1) and Hypercorn (h2c + HTTP/1) server entries.

Implements the Connect wire directly for unary + server-stream: proto/json
negotiation, metadata (headers) visible to handlers, Connect unary errors
(HTTP status + JSON body) and end-stream errors with structured details
(spec §4.1).
"""
import json
import os
from urllib.parse import urlparse

from google.protobuf import json_format

from easyrpc import RPCError, ErrorDetail, http_status, encode_error_json, encode_end_stream, frame
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

    reg.unary["Health"] = lambda _req, kind, _h: ser_msg(pb.HealthResponse(ok=True, name=NAME), kind)
    reg.unary["Echo"] = lambda req, kind, _h: ser_msg(
        pb.EchoResponse(output="echo:" + parse_msg(pb.EchoRequest, req, kind).input), kind
    )
    reg.unary["Fail"] = lambda req, kind, _h: ser_msg(
        pb.FailResponse(ok=(parse_msg(pb.FailRequest, req, kind).message == "")), kind
    )
    reg.stream["Count"] = lambda _req, kind, _h, emit: [
        emit(ser_msg(pb.CountResponse(index=i), kind), False) for i in range(3)
    ]
    reg.stream["StreamFail"] = lambda req, kind, _h, emit: _stream_fail(req, kind, emit)
    reg.unary["EchoMeta"] = lambda req, kind, h: ser_msg(
        pb.EchoMetaResponse(input=parse_msg(pb.EchoMetaRequest, req, kind).input,
                            meta={k: h[k] for k in ("x-test", "authorization") if k in h}), kind
    )
    reg.unary["Big"] = lambda req, kind, _h: ser_msg(
        pb.BigResponse(size=parse_msg(pb.BigRequest, req, kind).size), kind
    )
    reg.unary["FailDetails"] = lambda req, kind, _h: _fail_details(req, kind)
    reg.stream["StreamFailDetails"] = lambda req, kind, _h, emit: _stream_fail_details(req, kind, emit)
    return reg


def _detail(m):
    return ErrorDetail(m.detail_type or "t/x", (m.detail_text or "d").encode())


def _fail_details(req, kind):
    m = parse_fail_details(req, kind)
    raise RPCError(m.code or 8, m.message or "limited", [_detail(m)])


def parse_fail_details(req, kind):
    if kind == "json":
        v = json.loads(req.decode())
        return pb.FailDetailsRequest(
            code=int(v.get("code", 8)), message=v.get("message", "limited"),
            detail_type=v.get("detailType", "t/x"), detail_text=v.get("detailText", "d"))
    return pb.FailDetailsRequest.FromString(req)


def parse_stream_fail_details(req, kind):
    if kind == "json":
        v = json.loads(req.decode())
        return pb.StreamFailDetailsRequest(
            emit_before=int(v.get("emitBefore", 0)), code=int(v.get("code", 13)),
            message=v.get("message", "boom"),
            detail_type=v.get("detailType", "t/s"), detail_text=v.get("detailText", "sd"))
    return pb.StreamFailDetailsRequest.FromString(req)


def _stream_fail_details(req, kind, emit):
    m = parse_stream_fail_details(req, kind)
    for i in range(m.emit_before):
        emit(ser(pb.StreamFailDetailsResponse(index=i), kind), False)
    raise RPCError(m.code or 13, m.message or "boom", [_detail(m)])


def ser(msg, kind):
    if kind == "json":
        return json_format.MessageToJson(msg).encode()
    return msg.SerializeToString()


def _stream_fail(req, kind, emit):
    if kind == "json":
        v = json.loads(req.decode())
        before, code, message = int(v.get("emitBefore", 0)), int(v.get("code", 13)), v.get("message", "boom")
    else:
        m = pb.StreamFailRequest.FromString(req)
        before, code, message = m.emit_before, m.code, m.message
    for i in range(before):
        emit(ser(pb.StreamFailResponse(index=i), kind), False)
    raise RPCError(code, message)


REG = handler()

# REST routes: path -> (is_stream, method name).
ROUTES = {
    "/v1/health": (False, "Health"),
    "/v1/echo": (False, "Echo"),
    "/v1/count": (True, "Count"),
    "/v1/fail": (False, "Fail"),
    "/v1/stream-fail": (True, "StreamFail"),
    "/v1/echo-meta": (False, "EchoMeta"),
    "/v1/big": (False, "Big"),
    "/v1/fail-details": (False, "FailDetails"),
    "/v1/stream-fail-details": (True, "StreamFailDetails"),
}


async def handle(scope, receive, send, headers):
    path = scope["path"]
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
        chunks: list = []
        end_payload = b""

        def emit(payload: bytes, end: bool) -> None:
            chunks.append(payload)

        try:
            h(body, kind, headers, emit)
        except RPCError as e:
            end_payload = encode_end_stream(e.code, e.message, e.details)
        out = b"".join(frame(c, False) for c in chunks) + frame(end_payload, True)
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
        out = h(body, kind, headers)
        ct = "application/json" if kind == "json" else "application/proto"
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", ct.encode())]})
        await send({"type": "http.response.body", "body": out})
    except RPCError as e:
        # Connect unary error: HTTP status carries the class, the JSON body
        # carries the exact code/message (+ details, spec §4.1).
        ct = b"application/json"
        await send({
            "type": "http.response.start",
            "status": http_status(e.code),
            "headers": [(b"content-type", ct), (b"connect-code", str(e.code).encode()), (b"connect-error", e.message.encode())],
        })
        await send({"type": "http.response.body", "body": encode_error_json(e.code, e.message, e.details)})
    except Exception as e:  # noqa: BLE001
        await send({"type": "http.response.start", "status": 500, "headers": [(b"content-type", b"application/json")]})
        await send({"type": "http.response.body", "body": encode_error_json(13, str(e))})


async def asgi_app(scope, receive, send):
    headers = {}
    for k, v in scope.get("headers", []):
        key = k.decode("latin1").lower()
        headers.setdefault(key, v.decode("latin1"))
    await handle(scope, receive, send, headers)
