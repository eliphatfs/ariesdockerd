import json
import logging
import asyncio
import websockets
from typing import *
from .auth import run_auth
from .error import AriesError
from .protocol import command_handler, NoResponse, AsyncClient
from .scheduling import schedule


class CentralState:
    auth_kind: str = None
    auth_name: str = None


state_store: Dict[websockets.WebSocketServerProtocol, CentralState] = dict()
daemons: Set[AsyncClient] = set()


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
    return cs.auth_name


async def daemon_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws, 'daemon')
    ac = AsyncClient(ws)
    daemons.add(ac)
    try:
        await ac.listen()
    finally:
        daemons.remove(ac)
    raise NoResponse


def tyck(obj, ty, name):
    if not isinstance(obj, ty):
        raise AriesError(8, 'bad request: %s should be %s, got %s' % (name, ty.__name__, type(obj)))
    

def any_aggregate(results: List[dict]):
    last_return = None
    for result in results:
        if result['code'] == 0:
            result.pop('code')
            result.pop('ticket')
            return result
        if result['code'] == -1:
            last_return = result
    if last_return is not None:
        raise AriesError(10, 'error from daemon: %d %s' % (result['code'], result['msg']))
    raise AriesError(10, 'error from daemon: %d %s' % (result['code'], result['msg']))


def cat_aggregate(results: List[dict]):
    x = dict()
    for result in results:
        if result['code'] != 0:
            raise AriesError(10, 'error from daemon: %d %s' % (result['code'], result['msg']))
        for k in result:
            if isinstance(k, list):
                if k not in x:
                    x[k] = list()
                x[k].extend(k)
            elif isinstance(k, dict):
                if k not in x:
                    x[k] = dict()
                x[k].update(k)
    return x


async def daemon_broadcast(cmd: str, args: dict, aggregator: Callable):
    tasks = [
        asyncio.create_task(daemon.issue(cmd, args))
        for daemon in daemons
    ]
    await asyncio.wait(tasks)
    return aggregator([task.result() for task in tasks])


async def logs_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws)
    container = payload['container']
    tyck(container, str)
    return daemon_broadcast('get_logs', dict(container=container), any_aggregate)


async def ps_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws)
    filt = payload.get('filt')
    if filt is not None:
        tyck(filt, str)
    res = daemon_broadcast('list_containers', dict(), cat_aggregate)
    return dict(containers={
        k: v
        for k, v in res['containers']
        if filt is None or filt in k or filt in v['name'] or filt in v['user']
    })


async def stop_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws)
    container = payload['container']
    tyck(container, str)
    return daemon_broadcast('stop_container', dict(container=container), any_aggregate)


async def remove_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws)
    container = payload['container']
    tyck(container, str)
    return daemon_broadcast('remove_container', dict(container=container), any_aggregate)


async def collect_nodes(include_finalized):
    tasks = dict()
    for daemon in daemons:
        name = state_store[daemon.ws].auth_name
        tasks[name] = asyncio.create_task(daemon.issue('node_info', dict(include_finalized=include_finalized)))
    for name, task in list(tasks.items()):
        tasks[name] = await task
    return tasks


async def nodes_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws)
    nodes = await collect_nodes(False)
    return dict(nodes=nodes)


async def run_handler(ws: websockets.WebSocketServerProtocol, payload):
    user = check_auth(ws)
    n_jobs = payload.get('n_jobs')
    n_gpus = payload['n_gpus']
    name = payload['name']
    image = payload['image']
    cmd = payload['cmd']
    nodes = await collect_nodes(True)
    available = {}
    for node, info in nodes:
        available[node] = info['gpu_ids']
        if n_jobs is None:
            if name in info['names']:
                raise AriesError(14, 'container of same name already exists!')
        else:
            for i in range(n_jobs):
                if ('%s-%d' % (name, i)) in info['names']:
                    raise AriesError(14, 'container of same name already exists!')
    sched = schedule(available, n_jobs, n_gpus)
    i = 0
    tasks = []
    for daemon in daemons:
        node = state_store[daemon.ws].auth_name
        for snode, gpus in sched:
            if node == snode:
                container_name = '%s-%d' % (name, i) if n_jobs is not None else name
                i += 1
                tasks.append(asyncio.create_task(daemon.issue('run_container', dict(
                    name=container_name,
                    gpu_ids=gpus,
                    image=image,
                    cmd=cmd,
                    user=user
                ))))
    await asyncio.wait(tasks)
    return cat_aggregate([task.result() for task in tasks])


dispatch = dict(
    auth=auth_handler,
    daemon=daemon_handler,
    logs=logs_handler,
    stop=stop_handler,
    delete=remove_handler,
    ps=ps_handler,
    nodes=nodes_handler,
    run=run_handler
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
