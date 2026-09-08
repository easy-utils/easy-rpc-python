"""easy-rpc Python server core: REST + proto/json via aiohttp."""
from typing import Callable, Dict, Tuple
import json
import asyncio
from aiohttp import web

ContentKind = str  # 'proto' | 'json'

UnaryHandler = Callable[[bytes, str], bytes]
StreamHandler = Callable[[bytes, str, Callable[[bytes, bool], None]], None]

class ServerRegistry:
    def __init__(self):
        self.unary: Dict[str, UnaryHandler] = {}
        self.stream: Dict[str, StreamHandler] = {}

def detect_kind(ct: str) -> ContentKind:
    return 'json' if ct.startswith('application/json') else 'proto'

def make_app(routes: Dict[str, Tuple[bool, str, ServerRegistry]], method_specs):
    app = web.Application()
    for path, (is_stream, name, reg) in routes.items():
        async def handler(request, path=path, is_stream=is_stream, name=name, reg=reg):
            kind = detect_kind(request.headers.get('Content-Type', ''))
            body = await request.read()
            if is_stream:
                h = reg.stream.get(name)
                if not h: return web.Response(status=404)
                frames = []
                def emit(payload, end):
                    frames.append(payload)
                h(body, kind, emit)
                out = b''.join( bytes([0])+len(p).to_bytes(4,'big')+p for p in frames)
                return web.Response(body=out, content_type='application/connect+proto' if kind=='proto' else 'application/connect+json')
            h = reg.unary.get(name)
            if not h: return web.Response(status=404)
            try:
                out = h(body, kind)
                ct = 'application/json' if kind=='json' else 'application/proto'
                return web.Response(body=out, content_type=ct)
            except Exception as e:
                return web.Response(status=400, text=str(e))
        app.router.add_post(path, handler)
    return app

def run_server(app, host='127.0.0.1', port=18888):
    web.run_app(app, host=host, port=port)
