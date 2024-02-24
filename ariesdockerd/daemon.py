import time
import socket
import logging
import asyncio
import datetime
import websockets
from .auth import issue
from .config import get_config
from .protocol import command_handler, client_serial, common_task_callback
from .executor import Executor


core = Executor()


dispatch = dict(

)


async def cleanup():
    next_cleanup = datetime.datetime.now()
    cur = next_cleanup.hour + next_cleanup.minute / 60
    dt = (4 - cur) * 3600
    while dt < 1:
        dt += 86400
    await asyncio.sleep(dt)
    while True:
        core.clean_up()
        await asyncio.sleep(86400)


async def one_pass():
    try:
        ws = await websockets.connect(get_config().central_host)
        result = await client_serial(ws, 'auth', dict(token=issue(socket.gethostname(), 'daemon')))
        assert result['code'] == 0, 'authentication failed'
        asyncio.create_task(
            client_serial(ws, 'daemon', dict()),
            name='daemon-start'
        ).add_done_callback(common_task_callback('daemon-start'))
        await command_handler(ws, dispatch)
    except Exception:
        logging.exception("Connection to Central is Lost")


async def main():
    global stop_signal
    logging.basicConfig(level=logging.INFO)
    stop_signal = asyncio.Future()
    back = 1
    asyncio.create_task(cleanup()).add_done_callback(common_task_callback('daemon-clean-up'))
    while not stop_signal.done():
        s = time.time()
        await one_pass()
        interval = time.time() - s
        if interval > back + 5 and back > 2:
            back = 2
        logging.info("Reconnecting in %d", back)
        back *= 2
