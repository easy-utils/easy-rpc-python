"""Error-path matrix (spec §4.2 M1-M13) + Error Details round-trip (§4.1).

Mirrored in every language implementation; inputs are constructed directly
against the protocol functions - no server needed.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from easyrpc import (ErrorDetail, RPCError, encode_end_stream, decode_end_stream,
                     encode_error_json, decode_error_json, http_status,
                     connect_from_status, frame, read_frame)


def check(cond, name):
    if not cond:
        raise SystemExit(f"FAIL {name}")


def _detail():
    return ErrorDetail("type.googleapis.com/google.rpc.RetryInfo", bytes([1, 2, 3, 250]))


def m_m1_empty_payload_clean_end():
    check(decode_end_stream(b"") == (0, "", []), "M1")


def m_m2_garbage_is_clean_end():
    check(decode_end_stream(bytes([0xFF, 0xFE, 0x00, 0x42])) == (0, "", []), "M2")


def m_m3_error_without_code_is_unknown():
    code, msg, _ = decode_end_stream(b'{"error":{}}')
    check((code, msg) == (2, ""), "M3")


def m_m4_unknown_code_name_is_2():
    code, msg, _ = decode_end_stream(b'{"error":{"code":"nope","message":"m"}}')
    check((code, msg) == (2, "m"), "M4")


def m_m5_unknown_fields_ignored():
    code, _, _ = decode_end_stream(b'{"error":{"code":"not_found","message":"m"},"x":1}')
    check(code == 5, "M5")


def m_m6_details_roundtrip():
    payload = encode_end_stream(8, "rate limited", [_detail()])
    code, msg, ds = decode_end_stream(payload)
    check(code == 8 and msg == "rate limited", "M6")
    check(ds == [_detail()], "M6b")


def m_m7_malformed_details_skipped():
    _, _, ds = decode_end_stream(
        b'{"error":{"code":"resource_exhausted","details":'
        b'[{"type":"t","value":"!!!"},{"value":"x"},{"type":"ok"},{"type":"t2","value":"AQID"}]}}'
    )
    check(ds == [ErrorDetail("t2", bytes([1, 2, 3]))], "M7")


def m_details_omitted_when_empty():
    check(b"details" not in encode_end_stream(5, "gone"), "omit empty")


def m_m11_plain_text_not_json_error():
    check(decode_error_json(b"busy") == (0, "", []), "M11")


def m_unary_details_roundtrip():
    body = encode_error_json(8, "limited", [_detail()])
    code, msg, ds = decode_error_json(body)
    check((code, msg) == (8, "limited") and ds == [_detail()], "unary details")


def m_m12_m13_deadline_code():
    code, _, _ = decode_error_json(encode_error_json(4, "deadline exceeded"))
    check(code == 4, "M12 code")
    check(http_status(4) == 504 and connect_from_status(504) == 4, "M12/M13 mapping")


def m8_truncated():
    full = frame(b"0123456789")
    check(read_frame(full[:-4]) is None, "M8 truncated must not yield partial")


def m9_oversized():
    huge = bytearray(5)
    huge[1:5] = (4 * 1024 * 1024 + 1).to_bytes(4, "big")
    try:
        read_frame(bytes(huge), max_bytes=4 * 1024 * 1024)
        raised = False
    except RPCError as err:
        raised = err.code == 8
    check(raised, "M9 oversized must raise code 8")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("m_") and callable(fn):
            fn()
    print("PY_ERRORS_MATRIX_OK")
