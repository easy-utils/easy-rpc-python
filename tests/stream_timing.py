"""Timing test: server-stream dispatch pushes frames incrementally (never
buffered). Run with `python tests/stream_timing.py`."""
import asyncio
import time

from easyrpc import Request, MethodSpec
from easyrpc.server import ServerRegistry, dispatch


async def main() -> None:
    reg = ServerRegistry()

    async def slow(req: bytes, kind: str, emit) -> None:
        for i in range(3):
            await emit(bytes([i]), False)
            await asyncio.sleep(0.2)
        await emit(b"", True)

    reg.stream["Slow"] = slow
    methods = [MethodSpec("", "Slow", "/t.Slow", "POST", False, True)]

    class Writer:
        def __init__(self) -> None:
            self.code = 0
            self.times: list[float] = []
            self.frames: list[bytes] = []

        def status(self, c: int) -> None:
            self.code = c

        def header(self, n: str, v: str) -> None:
            pass

        async def write_frame(self, p: bytes) -> None:
            self.times.append(time.monotonic())
            self.frames.append(p)

    w = Writer()
    start = time.monotonic()
    await dispatch(Request(url="/t.Slow", body=b""), methods, reg, w)
    offsets = [round(t - start, 3) for t in w.times]
    assert w.code == 200, w.code
    assert len(w.frames) == 4, len(w.frames)
    # First frame arrives immediately; last after the three 0.2s gaps.
    assert offsets[0] < 0.1, offsets
    assert offsets[-1] > 0.5, offsets
    print("PASS: first frame at", offsets[0], "last at", offsets[-1])


if __name__ == "__main__":
    asyncio.run(main())
