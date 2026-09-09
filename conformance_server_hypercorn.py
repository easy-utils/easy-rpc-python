"""easy-rpc Python conformance server entry (Hypercorn, h2c + HTTP/1). PORT env."""
import asyncio
import os
from hypercorn.asyncio import serve
from hypercorn.config import Config
from conformance_asgi import asgi_app

async def main():
    config = Config()
    config.bind = ["127.0.0.1:" + os.environ.get("PORT", "18888")]
    # Give Hypercorn the chance to serve h2c: it handles both h2c and h1 by default.
    await serve(asgi_app, config)

if __name__ == "__main__":
    asyncio.run(main())
