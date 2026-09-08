import asyncio, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from easyrpc import HttpxTransport, Request
from easyrpc.conformance.v1 import conformance_pb2 as pb

async def main():
    t = HttpxTransport(base="http://127.0.0.1:18888")
    req = Request(url="/v1/echo", method="POST", body=pb.EchoRequest(input="hi").SerializeToString())
    res = await t.send(req)
    assert res.status == 200, res.status
    out = pb.EchoResponse()
    out.ParseFromString(res.body)
    assert out.output == "echo:hi", out.output
    print("python echo ok:", out.output)

asyncio.run(main())
