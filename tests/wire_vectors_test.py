"""Wire golden-vector conformance (transport-independent): the protocol layer
must reproduce easy-rpc-spec/conformance/wire-vectors.json. The vector file is
vendored at testdata/wire-vectors.json (synced from spec).

Frames are byte-exact; JSON payloads (end-stream, unary error) are compared
SEMANTICALLY (JSON key order is not significant)."""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from easyrpc import (  # noqa: E402
    ErrorDetail,
    code_from_string,
    code_to_string,
    decode_end_stream,
    demux_trailers,
    encode_end_stream,
    encode_error_json,
    frame,
    http_status,
    mux_trailers,
)

V = json.load(open(os.path.join(os.path.dirname(__file__), "testdata", "wire-vectors.json")))


def _hex(b: bytes) -> str:
    return b.hex()


def _bytes(h: str) -> bytes:
    return bytes.fromhex(h)


def test_frames():
    for f in V["frames"]:
        e = f["encode"]
        raw = bytearray(frame(_bytes(e["payloadHex"]), e["end"]))
        if e["compressed"]:
            raw[0] |= 0x01
        assert bytes(raw).hex() == f["bytesHex"], f["name"]


def test_end_stream():
    for e in V["endStream"]:
        code, msg, _details, metadata = decode_end_stream(_bytes(e["decode"]["bytesHex"]))
        assert code == e["code"], f"{e['name']} code"
        assert msg == e["message"], f"{e['name']} message"
        if e["metadata"] is not None:
            assert metadata == e["metadata"], f"{e['name']} metadata"
        if e.get("encode"):
            enc = e["encode"]
            got = encode_end_stream(enc["code"], enc["message"], None, enc.get("metadata") or {})
            # JSON payloads compare semantically.
            assert json.loads(got) == json.loads(_bytes(e["bytesHex"])), f"{e['name']} encode"


def test_unary_error():
    for u in V["unaryError"]:
        enc = u["encode"]
        details = [ErrorDetail(d["type"], _bytes(d["valueHex"])) for d in enc.get("details", [])]
        got = encode_error_json(enc["code"], enc["message"], details or None)
        assert json.loads(got) == json.loads(_bytes(u["bytesHex"])), u["name"]


def test_trailers():
    for t in V["trailerHeaders"]:
        if t.get("demux"):
            h, tl = demux_trailers(t["demux"])
            assert h == t["headers"], f"{t['name']} headers"
            assert tl == t["trailers"], f"{t['name']} trailers"
        if t.get("mux"):
            got = mux_trailers(t["mux"]["headers"], t["mux"]["trailers"])
            assert got == t["result"], f"{t['name']} mux"


def test_code_map():
    for c in V["codeNames"]:
        assert code_to_string(c["code"]) == c["name"], c["code"]
        assert code_from_string(c["name"]) == c["code"], c["name"]
        if c["code"] != 0:
            assert http_status(c["code"]) == c["http"], c["code"]


if __name__ == "__main__":
    test_frames()
    test_end_stream()
    test_unary_error()
    test_trailers()
    test_code_map()
    print("PY_WIRE_VECTORS_OK")
