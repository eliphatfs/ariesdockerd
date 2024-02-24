import json
import logging
import asyncio
import websockets
from typing import *
from .auth import run_auth
from .error import AriesError
from .protocol import command_handler, NoResponse


class CentralState:
    auth_kind: str = None
    auth_name: str = None


state_store: Dict[websockets.WebSocketServerProtocol, CentralState] = dict()
daemons: Set[websockets.WebSocketServerProtocol] = set()


async def auth_handler(ws: websockets.WebSocketServerProtocol, payload):
    decode = run_auth(payload['token'])
    cs = state_store[ws]
    cs.auth_kind = decode['kind']
    cs.auth_name = decode['name']
    return dict(user=cs.auth_name)


def check_auth(ws: websockets.WebSocketServerProtocol, expected_kind: str = 'user'):
    cs = state_store[ws]
    if cs.auth_kind != expected_kind:
        raise AriesError(7, 'no permission for command')


async def daemon_handler(ws: websockets.WebSocketServerProtocol):
    check_auth(ws, 'daemon')
    daemons.add(ws)
    await ws.wait_closed()
    daemons.remove(ws)
    raise NoResponse


dispatch = dict(
    auth=auth_handler,
    daemon=daemon_handler
)


async def handler(ws: websockets.WebSocketServerProtocol):
    state_store[ws] = CentralState()
    try:
        await command_handler(ws, dispatch)
    except Exception:
        logging.exception("Unexpected Error in Outer Loop")
    state_store.pop(ws)


async def main():
    global stop_signal
    logging.basicConfig(level=logging.INFO)
    stop_signal = asyncio.Future()
    async with websockets.serve(handler, 'localhost', 23549):
        await stop_signal


def sync_main():
    asyncio.run(main())
