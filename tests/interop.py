import asyncio, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from easyrpc import Request, read_frame, Transport
from easyrpc.conformance.v1 import conformance_pb2 as pb
from easyrpc.conformance.v1.conformance_easyrpc_pb2 import ConformanceServiceClient
from easyrpc.client_selector import default_client

async def main():
    base = os.environ.get("EASY_RPC_BASE", "http://127.0.0.1:18888")
    realm = os.environ.get("EASY_RPC_REALM", "std")
    t = default_client(base=base, realm=realm)
    c = ConformanceServiceClient(t)
    out = await c.echo(pb.EchoRequest(input="hi"))
    assert out.output == "echo:hi", out.output
    idx=[]
    async for res in c.count(pb.CountRequest(count=3)):
        idx.append(res.index)
    assert idx == [0,1,2], idx
    print(f"python echo+count ok ({realm}):", out.output, idx)

    # stream-fail: data frames then a Connect end-stream error (must raise).
    seen = []
    raised = None
    try:
        async for r in c.streamFail(pb.StreamFailRequest(emit_before=2, code=13, message="boom")):
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
    print("python stream-fail + echo-meta ok")

asyncio.run(main())
