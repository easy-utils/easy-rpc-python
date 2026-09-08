import asyncio, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from easyrpc import HttpxTransport
from easyrpc.conformance.v1 import conformance_pb2 as pb
from easyrpc.conformance.v1.conformance_easyrpc_pb2 import ConformanceServiceClient

async def main():
    t = HttpxTransport(base="http://127.0.0.1:18888")
    c = ConformanceServiceClient(t)
    out = await c.echo(pb.EchoRequest(input="hi"))
    assert out.output == "echo:hi", out.output
    idx=[]
    async for res in c.count(pb.CountRequest(count=3)):
        idx.append(res.index)
    assert idx == [0,1,2], idx
    print("python echo+count ok:", out.output, idx)

asyncio.run(main())
