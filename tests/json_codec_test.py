"""JSON codec tests (transport-independent) for the Python core."""
import asyncio
import sys

sys.path.insert(0, "/home/user/easy-utils/easy-rpc-python")

from easyrpc import (  # noqa: E402
    MethodSpec, RPCError, HandlerContext, Request, Response,
    content_kind_of, content_type_for, encode_msg, decode_msg,
)
from easyrpc.server import ServerRegistry, dispatch  # noqa: E402
from easyrpc.conformance.v1 import conformance_pb2 as pb  # noqa: E402


class _Writer:
    def __init__(self):
        self.status_code = 200
        self.headers = []
        self.chunks = []

    def status(self, code):
        self.status_code = code

    def header(self, name, value):
        self.headers.append((name, value))

    async def write_frame(self, payload):
        self.chunks.append(bytes(payload))


def _reg():
    reg = ServerRegistry()

    async def echo(req, ctx):
        m = decode_msg(req, pb.EchoRequest, ctx.kind)
        return encode_msg(pb.EchoResponse(output="echo:" + m.input), ctx.kind)

    async def echo_bytes(req, ctx):
        m = decode_msg(req, pb.EchoBytesRequest, ctx.kind)
        return encode_msg(pb.EchoBytesResponse(data=m.data), ctx.kind)

    async def fail_details(req, ctx):
        m = decode_msg(req, pb.FailDetailsRequest, ctx.kind)
        raise RPCError(m.code or 8, m.message or "limited")

    reg.unary["Echo"] = echo
    reg.unary["EchoBytes"] = echo_bytes
    reg.unary["FailDetails"] = fail_details
    return reg


METHODS = [
    MethodSpec("svc", "Echo", "/svc/Echo", False, False),
    MethodSpec("svc", "EchoBytes", "/svc/EchoBytes", False, False),
    MethodSpec("svc", "FailDetails", "/svc/FailDetails", False, False),
]


def _call(path, body, ct):
    w = _Writer()
    req = Request(url=path, headers={"content-type": [ct]}, body=body)
    asyncio.run(dispatch(req, METHODS, _reg(), w))
    return w


def test_kind_mapping():
    assert content_kind_of("application/proto") == "proto"
    assert content_kind_of("application/json; charset=utf-8") == "json"
    assert content_kind_of("text/plain") == ""
    assert content_type_for(False, "json") == "application/json"
    assert content_type_for(True, "json") == "application/connect+json"


def test_unary_json_roundtrip():
    w = _call("/svc/Echo", b'{"input":"hi"}', "application/json")
    assert w.status_code == 200, w.status_code
    body = b"".join(w.chunks)
    out = pb.EchoResponse.FromString(b"")  # sanity that FromString exists
    from google.protobuf import json_format
    msg = json_format.Parse(body, pb.EchoResponse())
    assert msg.output == "echo:hi"
    assert any(k == "content-type" and v == "application/json" for k, v in w.headers)


def test_json_bytes_are_base64():
    from google.protobuf import json_format
    data = bytes([0, 1, 2, 0xff, 0xfe, 0x80])
    w = _call("/svc/EchoBytes", json_format.MessageToJson(pb.EchoBytesRequest(data=data)).encode(), "application/json")
    body = b"".join(w.chunks)
    out = json_format.Parse(body, pb.EchoBytesResponse())
    assert bytes(out.data) == data


def test_json_error_status_and_body():
    from google.protobuf import json_format
    w = _call("/svc/FailDetails", json_format.MessageToJson(pb.FailDetailsRequest(code=8, message="limited")).encode(), "application/json")
    assert w.status_code == 429, w.status_code
    assert b"resource_exhausted" in b"".join(w.chunks)


def test_shape_mismatch_415():
    w = _call("/svc/Echo", b'{"input":"hi"}', "application/connect+json")
    assert w.status_code == 415, w.status_code


if __name__ == "__main__":
    test_kind_mapping()
    test_unary_json_roundtrip()
    test_json_bytes_are_base64()
    test_json_error_status_and_body()
    test_shape_mismatch_415()
    print("PY_JSON_CODEC_OK")
