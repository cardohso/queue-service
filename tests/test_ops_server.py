import httpx

from queue_service.ops import Response, start_ops_server, text


async def test_ops_server_routes_and_status_codes() -> None:
    state = {"ready": True}

    async def healthz() -> Response:
        return text(200, "ok\n")

    async def readyz() -> Response:
        return text(200, "ready\n") if state["ready"] else text(503, "not ready\n")

    server = await start_ops_server("127.0.0.1", 0, {"/healthz": healthz, "/readyz": readyz})
    port = server.sockets[0].getsockname()[1]
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as c:
            assert (await c.get("/healthz")).text == "ok\n"
            assert (await c.get("/readyz")).status_code == 200
            state["ready"] = False
            assert (await c.get("/readyz")).status_code == 503  # tracks live state
            assert (await c.get("/nope")).status_code == 404
            assert (await c.post("/healthz")).status_code == 405
            assert (await c.get("/healthz?probe=1")).status_code == 200  # query string ignored
    finally:
        server.close()
        await server.wait_closed()
