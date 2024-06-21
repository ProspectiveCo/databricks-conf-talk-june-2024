import asyncio
import logging
import socket
import pyarrow
import pyarrow.json
import threading
import uvicorn
from datetime import datetime, timedelta
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
from perspective import (
    PerspectiveWidget,
    Table as PerspectiveTable,
    PerspectiveManager,
    PerspectiveStarletteHandler,
)
from starlette.staticfiles import StaticFiles
from typing import Dict, Union


def find_free_port():
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]

_psp_arrow_map = {
    int: pyarrow.int64(),
    float: pyarrow.float64(),
    bool: pyarrow.bool_(),
    str: pyarrow.string(),
    datetime: pyarrow.timestamp("us", tz="UTC"),
}
def psp_schema_to_pyarrow_schema(psp_schema):
    return pyarrow.schema([(k, _psp_arrow_map[v]) for k,v in psp_schema.items()])


def perspective_spark_bridge(
    tables: Dict[str, Union[PerspectiveTable, PerspectiveWidget]],
    fastapi_app: FastAPI = None,
):
    app = fastapi_app or FastAPI()
    for table_name in tables:
        @app.websocket(f"/tables/{table_name}")
        async def ws(websocket: WebSocket, table_name=table_name):
            try:
                await websocket.accept()
                while True:
                    data = await websocket.receive_json()

                    if not data:
                        return

                    tables[table_name].update(data)
            except WebSocketDisconnect:
                pass

    return app


def make_perspective_app(manager: PerspectiveManager, app: FastAPI = None):
    """Code to create a Perspective webserver. This code is adapted from
    https://github.com/finos/perspective/blob/master/examples/python-starlette/server.py
    """

    def perspective_thread(manager):
        # This thread runs the perspective processing callback
        psp_loop = asyncio.new_event_loop()
        manager.set_loop_callback(psp_loop.call_soon_threadsafe)
        psp_loop.run_forever()

    thread = threading.Thread(target=perspective_thread, args=(manager,), daemon=True)
    thread.start()

    async def websocket_handler(websocket: WebSocket):
        handler = PerspectiveStarletteHandler(manager=manager, websocket=websocket)
        try:
            await handler.run()
        except Exception:
            ...

    app = app or FastAPI()
    app.add_api_websocket_route("/data", websocket_handler)
    app.mount("/", StaticFiles(directory=".", html=True), name="static")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    return app


def start_server(app: FastAPI, port: int = None, threaded: bool = True):
    port_to_use = port or find_free_port()
    logging.critical(f"Listening on http://localhost:{port_to_use}")
    if threaded:
        thread = threading.Thread(
            target=uvicorn.run,
            args=(app,),
            # kwargs=dict(host="0.0.0.0", port=port_to_use, log_level=50),
            kwargs=dict(host="0.0.0.0", port=port_to_use),
            daemon=True,
        )
        thread.start()
        if port is None:
            return port_to_use
        return thread
    uvicorn.run(app, host="0.0.0.0", port=port_to_use)
