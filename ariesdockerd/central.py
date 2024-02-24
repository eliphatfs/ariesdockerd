import json
import logging
import asyncio
import websockets
from typing import *
from .auth import run_auth
from .error import AriesError


class CentralState:
    auth_kind: str = None
    auth_name: str = None


state_store: Dict[websockets.WebSocketServerProtocol, CentralState] = dict()


async def auth_handler(ws: websockets.WebSocketServerProtocol, payload):
    decode = run_auth(payload['token'])
    if decode['kind'] not in ['user', 'daemon']:
        raise AriesError(5, 'unexpected kind for auth: ' + str(decode['kind']))
    cs = state_store[ws]
    cs.auth_kind = decode['kind']
    cs.auth_name = decode['name']
    return {}


dispatch = dict(
    auth=auth_handler
)


async def handler(ws: websockets.WebSocketServerProtocol):
    state_store[ws] = CentralState()
    try:
        async for message in ws:
            ticket = None
            try:
                try:
                    payload = json.loads(message)
                    ticket = payload['ticket']
                    cmd = payload['cmd']
                    if cmd not in dispatch:
                        raise AriesError(1, "unknown command `%s`" % cmd)
                    result: dict = dispatch[cmd](ws, payload)
                    await ws.send(json.dumps(dict(ticket=ticket, code=0, **result)))
                except AriesError as exc:
                    await ws.send(json.dumps(dict(ticket=ticket, code=exc.args[0], msg=exc.args[1])))
                    return None
                except Exception as exc:
                    await ws.send(dict(ticket=ticket, code=-1, msg=repr(exc)))
                    return None
            except websockets.ConnectionClosed:
                return None
    except websockets.ConnectionClosed:
        logging.warning("Caught Outer Loop Connection Closed")
    finally:
        state_store.pop(ws)


async def main():
    global stop_signal
    stop_signal = asyncio.Future()
    async with websockets.serve(handler, 'localhost', 23549):
        await stop_signal


if __name__ == "__main__":
    asyncio.run(main())
