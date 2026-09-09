"""easy-rpc Python conformance server entry (uvicorn, HTTP/1 only). PORT env."""
import os
import uvicorn
from conformance_asgi import asgi_app

port = int(os.environ.get("PORT", "18888"))
uvicorn.run(asgi_app, host="127.0.0.1", port=port, h11_max_incomplete_event_size=0)
