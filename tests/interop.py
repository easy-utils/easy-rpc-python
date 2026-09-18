import asyncio, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from easyrpc import Request, read_frame, Transport
from easyrpc.conformance.v1 import conformance_pb2 as pb
from easyrpc.conformance.v1.conformance_easyrpc_pb2 import ConformanceServiceClient
from easyrpc.client_selector import default_client

async def main():
    base = os.environ.get("EASY_RPC_BASE", "http://127.0.0.1:18888")
    realm = os.environ.get("EASY_RPC_REALM") or os.environ.get("EASY_RPC_TRANSPORT") or "std"
    t = default_client(base=base, realm=realm)
    c = ConformanceServiceClient(t)
    out = await c.echo(pb.EchoRequest(input="hi"))
    assert out.output == "echo:hi", out.output
    idx = []
    st = await c.count(pb.CountRequest(count=3))
    async for res in st:
        idx.append(res.index)
    assert idx == [0, 1, 2], idx
    print(f"python echo+count ok ({realm}):", out.output, idx)

    # stream-fail: data frames then a Connect end-stream error (must raise).
    seen = []
    raised = None
    try:
        st = await c.streamFail(pb.StreamFailRequest(emit_before=2, code=13, message="boom"))
        async for r in st:
            seen.append(r.index)
    except Exception as e:  # noqa: BLE001
        raised = e
    assert seen == [0, 1], seen
    assert raised is not None and getattr(raised, "code", None) == 13, raised

    # echo-meta: request metadata visible server-side (inline wrapper, since
    # the core interceptor lands in phase E).
    class _Meta(Transport):
        def __init__(self, inner, md):
            self._inner, self._md = inner, md
        async def send(self, req):
            req.headers.update(self._md)
            return await self._inner.send(req)
        async def open_stream(self, req):
            req.headers.update(self._md)
            return await self._inner.open_stream(req)

    from easyrpc import Transport as _T
    c2 = ConformanceServiceClient(_Meta(t, {"x-test": ["abc"]}))
    m = await c2.echoMeta(pb.EchoMetaRequest(input="hi"))
    assert m.meta.get("x-test") == "abc", dict(m.meta)

    # error details (spec §4.1): unary + stream must surface structured details.
    raised = None
    try:
        await c.failDetails(pb.FailDetailsRequest(
            code=8, message="limited",
            detail_type="type.googleapis.com/google.rpc.RetryInfo", detail_text="retry:5s"))
    except Exception as e:  # noqa: BLE001
        raised = e
    assert getattr(raised, "code", None) == 8, raised
    ds = getattr(raised, "details", None) or []
    assert len(ds) == 1 and ds[0].type_ == "type.googleapis.com/google.rpc.RetryInfo" \
        and ds[0].value == b"retry:5s", ds

    seen = []
    raised = None
    try:
        st = await c.streamFailDetails(pb.StreamFailDetailsRequest(
                emit_before=2, code=13, message="boom", detail_type="t/stream", detail_text="sd"))
        async for r in st:
            seen.append(r.index)
    except Exception as e:  # noqa: BLE001
        raised = e
    assert seen == [0, 1], seen
    ds = getattr(raised, "details", None) or []
    assert getattr(raised, "code", None) == 13 and ds and ds[0].type_ == "t/stream" and ds[0].value == b"sd", raised
    print("python error-details ok")

    # trailing metadata (spec §3.3): unary + streaming.
    ures = await c.echoTrailer(pb.EchoTrailerRequest(input="x"))
    assert ures.output == "trailer:x", ures.output
    assert c.last_trailers.get("x-trl") == ["unary-x"], c.last_trailers
    st = await c.countTrailer(pb.CountTrailerRequest(count=2))
    tidx = []
    async for r in st:
        tidx.append(r.index)
    assert tidx == [0, 1], tidx
    assert st.trailers().get("x-ctrailer") == ["done"], st.trailers()
    print("python trailers ok")

    # extended shapes
    eb = await c.echoBytes(pb.EchoBytesRequest(data=b"\x00\x01\x02\xff\xfe\x80"))
    assert bytes(eb.data) == b"\x00\x01\x02\xff\xfe\x80", eb.data
    await c.empty(pb.EmptyRequest())
    assert (await c.sleep(pb.SleepRequest(millis=0))).ok
    bs = await c.bigStream(pb.BigStreamRequest(count=3, size=2048))
    bidx = []
    async for r in bs:
        bidx.append(r.index)
    assert bidx == [0, 1, 2], bidx
    print("python extended ok")

asyncio.run(main())
