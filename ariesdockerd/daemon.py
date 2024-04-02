import time
import json
import socket
import base64
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
from .protocol import command_handler, client_serial, common_task_callback, NoResponse
from .executor import Executor


core = Executor()


@functools.lru_cache(maxsize=None)
def total_gpus():
    return len(subprocess.check_output(['nvidia-smi', '--query-gpu=name', '--format=csv,noheader']).splitlines())


def node_info_task(ws: websockets.WebSocketServerProtocol, payload):
    include_finalized = payload['include_finalized']
    gpus = set(range(total_gpus()))
    names = set()
    ids = set()
    for container, info in core.scan():
        names.add(container.name)
        ids.add(container.short_id)
        if container.status != 'dead' and not info.get('removed'):
            for gpu in info['gpu_ids']:
                gpus.remove(gpu)
    if include_finalized:
        for k, v in core.exit_store.items():
            names.add(v.name)
            ids.add(k)
    return dict(free_gpu_ids=sorted(gpus), names=sorted(names), ids=sorted(ids))


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
    env = payload['env']
    timeout = payload['timeout']
    tyck(name, str, 'name')
    tyck(image, str, 'image')
    tyck(cmd, list, 'exec')
    tyck(user, str, 'user')
    tyck(gpu_ids, list, 'gpu_ids')
    tyck(env, list, 'env')
    tyck(timeout, int, 'timeout')
    assert name not in names, 'container already exists: %s' % name
    for gpu_id in gpu_ids:
        assert gpu_id in gpus, 'gpu not found or already in use: %s' % gpu_id
    cid = core.run(name, image, cmd, gpu_ids, user, env, timeout)
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
        status = 'removed' if info.get('removed') else container.status
        data_dict[container.short_id] = dict(
            gpu_ids=info['gpu_ids'], name=container.name, user=info['user'], status=status,
            node=hostname
        )
    for short_id, es in core.exit_store.items():
        data_dict[short_id] = dict(gpu_ids=[], user=es.user, status='finalized', name=es.name, node=hostname)
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


def kill_container_task(ws: websockets.WebSocketServerProtocol, payload):
    container = payload['container']
    tyck(container, str, 'container')
    core.kill(container)
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


tcp_connections = dict()
READER, WRITER, MSG_ID, FLOWCONTROL = 0, 1, 2, 3


async def tcpalive(client):
    if client not in tcp_connections:
        return
    tcp = tcp_connections[client]
    while True:
        msg_id = tcp[MSG_ID]
        await asyncio.sleep(300)
        if client not in tcp_connections:
            break
        msg_id2 = tcp[MSG_ID]
        if msg_id == msg_id2:
            return tcp_connections.pop(client)


async def tcprecv_handler(ws: websockets.WebSocketServerProtocol, client: str, reader: asyncio.StreamReader):
    p = 0
    last_write = None
    tcp = tcp_connections[client]
    while True:
        nxt = await reader.read(16384)
        if not len(nxt):
            asyncio.create_task(tcpalive(client))
            break
        enc = base64.b64encode(nxt).decode('ascii')
        packet = json.dumps(dict(cmd='tcprecv', client=client, d=enc, p=p))
        p += 1
        if last_write is not None:
            await last_write
        if tcp[FLOWCONTROL] is not None:
            await tcp[FLOWCONTROL]
        last_write = asyncio.create_task(ws.send(packet))


async def tcpconn_handler(ws: websockets.WebSocketServerProtocol, payload):
    client = payload['client']
    reader, writer = await asyncio.open_connection("127.0.0.1", payload['port'])
    asyncio.create_task(tcprecv_handler(ws, client, reader))
    tcp_connections[client] = [reader, writer, 0, None]
    return dict(msg='tcp connection handled')


async def tcpsend_handler(ws: websockets.WebSocketServerProtocol, payload):
    client = payload['client']
    tcp = tcp_connections[client]
    while tcp[MSG_ID] != payload['p']:
        await asyncio.sleep(0)
    writer: asyncio.StreamWriter = tcp[WRITER]
    writer.write(base64.b64decode(payload['d']))
    tcp[MSG_ID] += 1
    raise NoResponse


async def tcpstop_handler(ws: websockets.WebSocketServerProtocol, payload):
    client = payload['client']
    tcp = tcp_connections[client]
    while tcp[MSG_ID] != payload['p']:
        await asyncio.sleep(0)
    writer: asyncio.StreamWriter = tcp[WRITER]
    await writer.drain()
    tcp_connections.pop(client)
    writer.close()
    await writer.wait_closed()
    return dict(msg='tcp connection closed')


async def tcpflowpause_handler(ws: websockets.WebSocketServerProtocol, payload):
    client = payload['client']
    tcp = tcp_connections[client]
    if tcp[FLOWCONTROL] is None:
        tcp[FLOWCONTROL] = asyncio.Future()
        return dict(changed=True)
    return dict(changed=False)


async def tcpflowresume_handler(ws: websockets.WebSocketServerProtocol, payload):
    client = payload['client']
    tcp = tcp_connections[client]
    if tcp[FLOWCONTROL] is None:
        return dict(changed=False)
    tcp[FLOWCONTROL].set_result(None)
    tcp[FLOWCONTROL] = None
    return dict(changed=True)


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
    remove_container=threaded_handler(remove_container_task),
    kill_container=threaded_handler(kill_container_task),
    tcpconn=tcpconn_handler,
    tcpsend=tcpsend_handler,
    tcpstop=tcpstop_handler,
    tcpflowpause=tcpflowpause_handler,
    tcpflowresume=tcpflowresume_handler,
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


def run_mond():
    retv = subprocess.call(['ariesmond'])
    if 0 != retv:
        logging.warning("ariesmond exit with code %d", retv)


async def mond():
    while not stop_signal.done():
        if get_config().grafana_endpoint:
            await threaded_handler(run_mond)()
        await asyncio.wait([asyncio.sleep(300), stop_signal], return_when=asyncio.FIRST_COMPLETED)


async def one_pass():
    global hostname
    hostname = socket.gethostname()
    ws = None
    try:
        ws = await websockets.connect(get_config().central_host, max_size=2**24)
        result = await client_serial(ws, 'auth', dict(token=issue(hostname, 'daemon')))
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
    import psutil
    print("I am", psutil.Process().pid)
    global stop_signal
    logging.basicConfig(level=logging.INFO)
    stop_signal = asyncio.Future()
    back = 1
    core.set_up()
    asyncio.create_task(cleanup()).add_done_callback(common_task_callback('daemon-clean-up'))
    asyncio.create_task(bookkeep()).add_done_callback(common_task_callback('daemon-bookkeep'))
    asyncio.create_task(mond()).add_done_callback(common_task_callback('daemon-mond'))
    while not stop_signal.done():
        s = time.time()
        await one_pass()
        interval = time.time() - s
        if interval > back + 5 and back > 2:
            back = 2
        if back > 900:
            back = 900
        logging.info("Reconnecting in %d", back)
        await asyncio.sleep(back)
        back *= 2


def sync_main():
    asyncio.run(main())
