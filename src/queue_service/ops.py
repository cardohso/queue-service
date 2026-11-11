"""A tiny HTTP server for the worker's /metrics, /healthz and /readyz.

The worker has no web framework; this is ~40 lines of asyncio instead of a second ASGI stack.
It only answers GET, one request per connection, and never reads a body.
"""

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable

log = logging.getLogger(__name__)

Response = tuple[int, str, bytes]  # status, content type, body
Handler = Callable[[], Awaitable[Response]]

_REASONS = {200: "OK", 404: "Not Found", 405: "Method Not Allowed", 503: "Service Unavailable"}


def text(status: int, body: str) -> Response:
    return status, "text/plain; charset=utf-8", body.encode()


async def _serve_one(
    routes: dict[str, Handler], reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    try:
        async with asyncio.timeout(5):
            request_line = (await reader.readline()).decode("latin-1").split()
            while (await reader.readline()).strip():  # discard headers
                pass
        if len(request_line) < 2:
            return
        method, path = request_line[0], request_line[1].split("?")[0]
        if method != "GET":
            status, ctype, body = text(405, "method not allowed\n")
        elif path in routes:
            status, ctype, body = await routes[path]()
        else:
            status, ctype, body = text(404, "not found\n")
        head = (
            f"HTTP/1.1 {status} {_REASONS.get(status, '')}\r\nContent-Type: {ctype}\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
        )
        writer.write(head.encode() + body)
        await writer.drain()
    except TimeoutError, ConnectionError:
        pass
    except Exception:
        log.exception("ops_handler_failed")
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


async def start_ops_server(host: str, port: int, routes: dict[str, Handler]) -> asyncio.Server:
    return await asyncio.start_server(lambda r, w: _serve_one(routes, r, w), host, port)
