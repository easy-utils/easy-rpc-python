"""easy-rpc Python conformance server (ASGI). Shared app used by both the
uvicorn (HTTP/1) and Hypercorn (h2c + HTTP/1) server entries.

Implements the Connect wire directly for unary + server-stream (easy-rpc v2:
proto only, POST only, gRPC-style paths): metadata (headers) visible to
handlers, Connect unary errors (HTTP status + JSON body), end-stream errors
with structured details (spec §4.1) and trailing metadata (spec §3.3).
"""
import os

from easyrpc import (
    RPCError, ErrorDetail, http_status, encode_error_json, encode_end_stream,
    frame, read_frame, gzip_compress, mux_trailers, HandlerContext,
    CONTENT_TYPE_UNARY, CONTENT_TYPE_STREAM, HEADER_PROTOCOL_VERSION,
    CONNECT_PROTOCOL_VERSION, DEFAULT_MAX_MESSAGE_BYTES, ENCODING_GZIP,
    COMPRESS_MIN_BYTES,
)
from easyrpc.server import ServerRegistry
from easyrpc.conformance.v1 import conformance_pb2 as pb

PORT = int(os.environ.get("PORT", "18888"))
NAME = "conformance"


def handler():
    reg = ServerRegistry()

    def parse(proto_class, raw):
        return proto_class.FromString(raw)

    def ser(msg):
        return msg.SerializeToString()

    reg.unary["Health"] = lambda _req, _ctx: ser(pb.HealthResponse(ok=True, name=NAME))
    reg.unary["Echo"] = lambda req, _ctx: ser(
        pb.EchoResponse(output="echo:" + parse(pb.EchoRequest, req).input)
    )
    reg.unary["Fail"] = lambda req, _ctx: _fail(parse(pb.FailRequest, req))
    reg.unary["Count"] = None  # not a method
    reg.stream["Count"] = lambda req, _ctx, emit: _count(parse(pb.CountRequest, req), emit)
    reg.stream["StreamFail"] = lambda req, _ctx, emit: _stream_fail(parse(pb.StreamFailRequest, req), emit)
    reg.unary["EchoMeta"] = lambda req, ctx: ser(
        pb.EchoMetaResponse(input=parse(pb.EchoMetaRequest, req).input,
                            meta={k: ctx.headers.get(k, [""])[0] for k in ("x-test", "authorization") if k in ctx.headers})
    )
    reg.unary["Big"] = lambda req, _ctx: ser(pb.BigResponse(size=parse(pb.BigRequest, req).size))
    reg.unary["FailDetails"] = lambda req, _ctx: _fail_details(parse(pb.FailDetailsRequest, req))
    reg.stream["StreamFailDetails"] = lambda req, _ctx, emit: _stream_fail_details(parse(pb.StreamFailDetailsRequest, req), emit)
    reg.unary["EchoTrailer"] = lambda req, ctx: _echo_trailer(parse(pb.EchoTrailerRequest, req), ctx)
    reg.stream["CountTrailer"] = lambda req, ctx, emit: _count_trailer(parse(pb.CountTrailerRequest, req), ctx, emit)
    return reg


def _fail(m):
    # The unary Fail RPC: ok unless a message was provided (then invalid_argument).
    if m.message:
        raise RPCError(3, m.message)
    return pb.FailResponse(ok=True).SerializeToString()


def _count(m, emit):
    n = m.count if m.count > 0 else 3
    for i in range(n):
        emit(pb.CountResponse(index=i).SerializeToString(), False)


def _count_trailer(m, ctx, emit):
    ctx.set_trailer("x-ctrailer", "done")
    n = m.count if m.count > 0 else 3
    for i in range(n):
        emit(pb.CountTrailerResponse(index=i).SerializeToString(), False)


def _echo_trailer(m, ctx):
    ctx.set_trailer("x-trl", "unary-" + m.input)
    return pb.EchoTrailerResponse(output="trailer:" + m.input).SerializeToString()


def _detail(m):
    return ErrorDetail(m.detail_type or "t/x", (m.detail_text or "d").encode())


def _fail_details(m):
    raise RPCError(m.code or 8, m.message or "limited", [_detail(m)])


def _stream_fail(m, emit):
    for i in range(m.emit_before):
        emit(pb.StreamFailResponse(index=i).SerializeToString(), False)
    raise RPCError(m.code or 13, m.message or "boom")


def _stream_fail_details(m, emit):
    for i in range(m.emit_before):
        emit(pb.StreamFailDetailsResponse(index=i).SerializeToString(), False)
    raise RPCError(m.code or 13, m.message or "boom", [_detail(m)])


REG = handler()

# gRPC-style paths (easy-rpc v2): path -> (is_stream, method name).
_SVC = "easyrpc.conformance.v1.ConformanceService"
ROUTES = {
    f"/{_SVC}/Health": (False, "Health"),
    f"/{_SVC}/Echo": (False, "Echo"),
    f"/{_SVC}/Count": (True, "Count"),
    f"/{_SVC}/Fail": (False, "Fail"),
    f"/{_SVC}/StreamFail": (True, "StreamFail"),
    f"/{_SVC}/EchoMeta": (False, "EchoMeta"),
    f"/{_SVC}/Big": (False, "Big"),
    f"/{_SVC}/FailDetails": (False, "FailDetails"),
    f"/{_SVC}/StreamFailDetails": (True, "StreamFailDetails"),
    f"/{_SVC}/EchoTrailer": (False, "EchoTrailer"),
    f"/{_SVC}/CountTrailer": (True, "CountTrailer"),
}


async def handle(scope, receive, send, headers):
    path = scope["path"]
    msg = ROUTES.get(path)
    if msg is None:
        await send({"type": "http.response.start", "status": 404, "headers": []})
        await send({"type": "http.response.body", "body": b""})
        return

    is_stream, name = msg

    body = b""
    while True:
        event = await receive()
        if event["type"] == "http.request":
            body += event.get("body", b"")
            if not event.get("more_body", False):
                break

    # proto-only content type.
    ct = headers.get("content-type", "").split(";", 1)[0].strip().lower()
    want = CONTENT_TYPE_STREAM if is_stream else CONTENT_TYPE_UNARY
    if ct != want:
        await send({"type": "http.response.start", "status": 415,
                    "headers": [(b"content-type", b"application/json")]})
        await send({"type": "http.response.body", "body": encode_error_json(3, f"unsupported content-type: expected {want}")})
        return

    pv = headers.get(HEADER_PROTOCOL_VERSION, "")
    if pv and pv != CONNECT_PROTOCOL_VERSION:
        await send({"type": "http.response.start", "status": 400,
                    "headers": [(b"content-type", b"application/json")]})
        await send({"type": "http.response.body", "body": encode_error_json(12, f"unsupported connect-protocol-version: {pv}")})
        return
    if len(body) > DEFAULT_MAX_MESSAGE_BYTES:
        await send({"type": "http.response.start", "status": 500,
                    "headers": [(b"content-type", b"application/json")]})
        await send({"type": "http.response.body", "body": encode_error_json(8, "request too large")})
        return

    ctx = HandlerContext(headers={k: [v] for k, v in headers.items()})

    if is_stream:
        h = REG.stream.get(name)
        if h is None:
            await send({"type": "http.response.start", "status": 404, "headers": []})
            await send({"type": "http.response.body", "body": b""})
            return
        # Unframe the enveloped single-request frame.
        step = read_frame(body)
        if step is None:
            await send({"type": "http.response.start", "status": 200,
                        "headers": [(b"content-type", CONTENT_TYPE_STREAM.encode())]})
            await send({"type": "http.response.body", "body": frame(encode_end_stream(13, "stream request: truncated frame"), True)})
            return
        req_body = step[0]
        chunks: list = []
        err = None

        def emit(payload: bytes, end: bool) -> None:
            if not end:
                chunks.append(frame(payload, False))

        try:
            h(req_body, ctx, emit)
        except RPCError as e:
            err = e
        except Exception as e:  # noqa: BLE001
            err = RPCError(13, str(e))
        if err is not None:
            end_payload = encode_end_stream(err.code, err.message, err.details, ctx.trailers)
        else:
            end_payload = encode_end_stream(0, "", None, ctx.trailers)
        out = b"".join(chunks) + frame(end_payload, True)
        await send({"type": "http.response.start", "status": 200,
                    "headers": [(b"content-type", CONTENT_TYPE_STREAM.encode())]})
        await send({"type": "http.response.body", "body": out})
        return

    h = REG.unary.get(name)
    if h is None:
        await send({"type": "http.response.start", "status": 404, "headers": []})
        await send({"type": "http.response.body", "body": b""})
        return
    try:
        out = h(body, ctx)
        # Unary gzip when the client accepts it.
        extra = []
        accept = headers.get("accept-encoding", "")
        if ENCODING_GZIP in [x.strip() for x in accept.split(",")] and len(out) >= COMPRESS_MIN_BYTES:
            out = gzip_compress(out)
            extra.append((b"content-encoding", b"gzip"))
        for k, v in mux_trailers({}, ctx.trailers).items():
            extra.append((k.encode(), (v[0] if isinstance(v, (list, tuple)) else v).encode()))
        await send({"type": "http.response.start", "status": 200,
                    "headers": [(b"content-type", CONTENT_TYPE_UNARY.encode())] + extra})
        await send({"type": "http.response.body", "body": out})
    except RPCError as e:
        hdrs = [(b"content-type", b"application/json"),
                (b"connect-code", str(e.code).encode()),
                (b"connect-error", e.message.encode())]
        for k, v in mux_trailers({}, ctx.trailers).items():
            hdrs.append((k.encode(), (v[0] if isinstance(v, (list, tuple)) else v).encode()))
        await send({"type": "http.response.start", "status": http_status(e.code), "headers": hdrs})
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
