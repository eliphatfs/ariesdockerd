import uuid
import json
import logging
import asyncio
import websockets
from .error import AriesError


class NoResponse(Exception):
    pass


async def process_command(ws: websockets.WebSocketCommonProtocol, dispatch: dict, message: str):
    ticket = None
    try:
        try:
            payload = json.loads(message)
            ticket = payload['ticket']
            cmd = payload['cmd']
            if cmd not in dispatch:
                raise AriesError(1, "unknown command `%s`" % cmd)
            result: dict = await dispatch[cmd](ws, payload)
            await ws.send(json.dumps(dict(ticket=ticket, code=0, **result)))
        except NoResponse:
            return
        except AriesError as exc:
            await ws.send(json.dumps(dict(ticket=ticket, code=exc.args[0], msg=exc.args[1])))
        except Exception as exc:
            await ws.send(dict(ticket=ticket, code=-1, msg=repr(exc)))
    except websockets.ConnectionClosed:
        return


def common_task_callback(name: str):

    def process_task_callback(task: asyncio.Task):
        if task.cancelled():
            logging.warning("Task `%s` Cancelled", name)
        elif task.exception() is not None:
            logging.error('Unexpected Exception in Task `%s`', name, exc_info=task.exception())

    return process_task_callback


async def command_handler(ws: websockets.WebSocketCommonProtocol, dispatch: dict):
    try:
        async for message in ws:
            coro = process_command(ws, dispatch, message)
            task = asyncio.create_task(coro)
            task.add_done_callback(common_task_callback('process-command'))
    except websockets.ConnectionClosed:
        logging.warning("Caught Outer Loop Connection Closed")


async def client_serial(ws: websockets.WebSocketCommonProtocol, cmd: str, args: dict):
    ticket = str(uuid.uuid4())
    await ws.send(json.dumps(dict(ticket=ticket, cmd=cmd, **args)))
    execution = json.loads(await ws.recv())
    assert execution['ticket'] == ticket, [ticket, execution['ticket']]
    return execution


# class AsyncClient(object):
#     def __init__(self, ws: websockets.WebSocketCommonProtocol) -> None:
#         self.ws = ws
#         self.futures: Dict[str, asyncio.Future] = dict()

#     async def listen(self):
#         async for message in self.ws:
#             payload = json.loads(message)
#             ticket = payload['ticket']
#             assert ticket in self.futures, ticket
#             self.futures[ticket].set_result()

#     async def issue(self, cmd: str, args: dict):
