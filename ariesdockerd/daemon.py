import time
import json
import socket
import logging
import asyncio
import datetime
import functools
import threading
import subprocess
import websockets
from .auth import issue
from .error import AriesError
from .config import get_config
from .protocol import command_handler, client_serial, common_task_callback
from .executor import Executor


core = Executor()


@functools.lru_cache(maxsize=None)
def total_gpus():
    return len(subprocess.check_output(['nvidia-smi', '--query-gpu=name', '--format=csv,noheader']).splitlines())


def node_info_task(ws: websockets.WebSocketServerProtocol, payload):
    include_finalized = payload['include_finalized']
    gpus = set(range(total_gpus()))
    names = set()
    for container, info in core.scan():
        names.add(container.name)
        for gpu in info['gpu_ids']:
            gpus.remove(gpu)
    if include_finalized:
        for v in core.exit_store.values():
            names.add(v.name)
    return dict(free_gpu_ids=sorted(gpus), names=sorted(names))


def tyck(obj, ty, name):
    assert isinstance(obj, ty), '%s should be %s, got %s' % (name, ty.__name__, type(obj))


def run_container_task(ws: websockets.WebSocketServerProtocol, payload):
    info = node_info_task(ws, dict(include_finalized=True))
    gpus, names = info['free_gpu_ids'], info['names']
    gpu_ids = payload['gpu_ids']
    name = payload['name']
    image = payload['image']
    cmd = payload['exec']
    user = payload['user']
    tyck(name, str, 'name')
    tyck(image, str, 'image')
    tyck(cmd, list, 'exec')
    tyck(user, str, 'user')
    tyck(gpu_ids, list, 'gpu_ids')
    assert name not in names, 'container already exists: %s' % name
    for gpu_id in gpu_ids:
        assert gpu_id in gpus, 'gpu not found or already in use: %s' % gpu_id
    cid = core.run(name, image, cmd, gpu_ids, user)
    return dict(short_id=cid)


def get_logs_task(ws: websockets.WebSocketServerProtocol, payload):
    container = payload['container']
    tyck(container, str, 'container')
    filt = [v for k, v in core.exit_store.items() if k.startswith(container) or v.name == container]
    if len(filt) == 1:
        return dict(logs=filt[0].logs.decode(errors='replace')[-2**23:])
    elif len(filt) > 1:
        raise AriesError(15, 'container ambiguous: ' + str([v.name for v in filt]))
    return dict(logs=core.logs(container).decode(errors='replace')[-2**23:])


def list_containers_task(ws: websockets.WebSocketServerProtocol, payload):
    data_dict = dict()
    for container, info in core.scan():
        data_dict[container.short_id] = dict(
            gpu_ids=info['gpu_ids'], name=container.name, user=info['user'], status=container.status
        )
    for short_id, es in core.exit_store.items():
        data_dict[short_id] = dict(gpu_ids=[], user=es.user, status='finalized', name=es.name)
    return dict(containers=data_dict)


def stop_container_task(ws: websockets.WebSocketServerProtocol, payload):
    container = payload['container']
    tyck(container, str, 'container')
    filt = [v for k, v in core.exit_store.items() if k.startswith(container) or v.name == container]
    if len(filt) == 1:
        raise AriesError(9, 'container already stopped')
    elif len(filt) > 1:
        raise AriesError(15, 'container ambiguous: ' + str([v.name for v in filt]))
    core.stop(container)
    return dict()


def remove_container_task(ws: websockets.WebSocketServerProtocol, payload):
    container = payload['container']
    tyck(container, str, 'container')
    filt = [k for k, v in core.exit_store.items() if k.startswith(container) or v.name == container]
    if len(filt) == 0:
        raise AriesError(13, 'no finalized container found to be deleted')
    elif len(filt) > 1:
        raise AriesError(15, 'container ambiguous: ' + str([v.name for v in filt]))
    core.exit_store.pop(filt[0])
    return dict()


def threaded_handler(func):

    async def _cmd(*args, **kwargs):
        loop = asyncio.get_running_loop()

        def wrapping_func(*args, **kwargs):
            try:
                r = func(*args, **kwargs)
                loop.call_soon_threadsafe(future.set_result, r)
            except BaseException as exc:
                loop.call_soon_threadsafe(future.set_exception, exc)
    
        future = asyncio.Future()
        thread = threading.Thread(target=wrapping_func, args=args, kwargs=kwargs)
        thread.start()
        return await future

    return _cmd


dispatch = dict(
    node_info=threaded_handler(node_info_task),
    run_container=threaded_handler(run_container_task),
    list_containers=threaded_handler(list_containers_task),
    get_logs=threaded_handler(get_logs_task),
    stop_container=threaded_handler(stop_container_task),
    remove_container=threaded_handler(remove_container_task)
)


async def cleanup():
    next_cleanup = datetime.datetime.now()
    cur = next_cleanup.hour + next_cleanup.minute / 60
    dt = (4 - cur) * 3600
    while dt < 1:
        dt += 86400
    await asyncio.wait([asyncio.sleep(dt), stop_signal], return_when=asyncio.FIRST_COMPLETED)
    while not stop_signal.done():
        s = time.time()
        core.clean_up()
        dt = 86400.0 - (time.time() - s)
        if dt >= 0:
            await asyncio.wait([asyncio.sleep(dt), stop_signal], return_when=asyncio.FIRST_COMPLETED)


async def bookkeep():
    while not stop_signal.done():
        await threaded_handler(core.bookkeep)()
        await asyncio.wait([asyncio.sleep(10), stop_signal], return_when=asyncio.FIRST_COMPLETED)


async def one_pass():
    ws = None
    try:
        ws = await websockets.connect(get_config().central_host, max_size=2**24)
        result = await client_serial(ws, 'auth', dict(token=issue(socket.gethostname(), 'daemon')))
        assert result['code'] == 0, 'authentication failed: %s' % result['msg']
        logging.info("Connected to Central Server")
        await ws.send(json.dumps(dict(ticket='daemon-special', cmd='daemon')))
        await command_handler(ws, dispatch)
    except Exception:
        logging.exception("Connection to Central is Lost")
    finally:
        if ws is not None:
            await ws.close()


async def main():
    global stop_signal
    logging.basicConfig(level=logging.INFO)
    stop_signal = asyncio.Future()
    back = 1
    core.set_up()
    asyncio.create_task(cleanup()).add_done_callback(common_task_callback('daemon-clean-up'))
    asyncio.create_task(bookkeep()).add_done_callback(common_task_callback('daemon-bookkeep'))
    while not stop_signal.done():
        s = time.time()
        await one_pass()
        interval = time.time() - s
        if interval > back + 5 and back > 2:
            back = 2
        logging.info("Reconnecting in %d", back)
        await asyncio.sleep(back)
        back *= 2


def sync_main():
    asyncio.run(main())
