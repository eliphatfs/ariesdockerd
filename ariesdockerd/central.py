import json
import logging
import asyncio
import websockets
from typing import *
from collections import Counter
from .auth import run_auth
from .error import AriesError
from .protocol import command_handler, NoResponse, AsyncClient
from .scheduling import schedule


class CentralState:
    auth_kind: Optional[str] = None
    auth_name: Optional[str] = None
    callback: Optional[Callable[[str], None]] = None


state_store: Dict[websockets.WebSocketServerProtocol, CentralState] = dict()
daemons: Set[AsyncClient] = set()


async def auth_handler(ws: websockets.WebSocketServerProtocol, payload):
    decode = run_auth(payload['token'])
    cs = state_store[ws]
    cs.auth_kind = decode['kind']
    cs.auth_name = decode['user']
    return dict(user=cs.auth_name)


def check_auth(ws: websockets.WebSocketServerProtocol, expected_kind: str = 'user'):
    cs = state_store[ws]
    if cs.auth_kind != expected_kind:
        raise AriesError(7, 'no permission for command')
    return cs.auth_name


def bypass_daemon(ws: websockets.WebSocketServerProtocol, message):
    cs = state_store[ws]
    if cs.callback is not None:
        cs.callback(message)
        if cs.auth_kind == 'daemon':
            return True
    return False


async def daemon_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws, 'daemon')
    ac = AsyncClient(ws)
    daemons.add(ac)

    def daemon_callback(x):
        payload: dict = json.loads(x)
        if payload.get('cmd') == 'tcprecv':
            asyncio.create_task(tcprecv_handler(payload))
        else:
            ac.result(payload)

    state_store[ws].callback = daemon_callback
    try:
        await ws.wait_closed()
    except Exception:
        import traceback
        traceback.print_exc()
    finally:
        daemons.remove(ac)
    raise NoResponse


def tyck(obj, ty, name):
    if not isinstance(obj, ty):
        raise AriesError(8, 'bad request: %s should be %s, got %s' % (name, ty.__name__, type(obj)))
    

def any_aggregate(results: List[dict]):
    errors = Counter()
    for result in results:
        if result['code'] == 0:
            result.pop('code')
            result.pop('ticket')
            return result
        errors.update([(result['code'], result['msg'])])
    code, msg = errors.most_common()[-1][0]
    raise AriesError(10, 'error from daemon: %d %s' % (code, msg))


def cat_aggregate(results: List[dict]):
    x = dict()
    for result in results:
        if result['code'] != 0:
            raise AriesError(10, 'error from daemon: %d %s' % (result['code'], result['msg']))
        for k, v in result.items():
            if isinstance(v, list):
                if k not in x:
                    x[k] = list()
                x[k].extend(v)
            elif isinstance(v, dict):
                if k not in x:
                    x[k] = dict()
                x[k].update(v)
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
    tyck(container, str, 'container')
    return await daemon_broadcast('get_logs', dict(container=container), any_aggregate)


async def ps_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws)
    filt = payload.get('filt')
    if filt is not None:
        tyck(filt, str, 'filt')
    res = await daemon_broadcast('list_containers', dict(), cat_aggregate)
    return dict(containers={
        k: v
        for k, v in res['containers'].items()
        if filt is None or filt in k or filt in v['name'] or filt in v['user']
    })


async def stop_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws)
    container = payload['container']
    tyck(container, str, 'container')
    return await daemon_broadcast('stop_container', dict(container=container), any_aggregate)


async def kill_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws)
    container = payload['container']
    tyck(container, str, 'container')
    return await daemon_broadcast('kill_container', dict(container=container), any_aggregate)


async def jstop_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws)
    job = payload['job']
    tyck(job, str, 'job')
    nodes = await collect_nodes(False)
    tasks = []
    for daemon in daemons:
        node = state_store[daemon.ws].auth_name
        for snode, info in nodes.items():
            if snode == node:
                for name in info['names']:
                    if name.startswith(job + '-') and str.isnumeric(name[len(job) + 1:]):
                        tasks.append(asyncio.create_task(
                            daemon.issue('stop_container', dict(container=name)))
                        )
    if len(tasks) == 0:
        raise AriesError(16, 'no such job to act on')
    await asyncio.wait(tasks)
    return cat_aggregate([task.result() for task in tasks])


async def jremove_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws)
    job = payload['job']
    tyck(job, str, 'job')
    nodes = await collect_nodes(True)
    tasks = []
    for daemon in daemons:
        node = state_store[daemon.ws].auth_name
        for snode, info in nodes.items():
            if snode == node:
                for name in info['names']:
                    if name.startswith(job + '-') and str.isnumeric(name[len(job) + 1:]):
                        tasks.append(asyncio.create_task(
                            daemon.issue('remove_container', dict(container=name)))
                        )
    if len(tasks) == 0:
        raise AriesError(16, 'no such job to act on')
    await asyncio.wait(tasks)
    return cat_aggregate([task.result() for task in tasks])


async def remove_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws)
    container = payload['container']
    tyck(container, str, 'container')
    return await daemon_broadcast('remove_container', dict(container=container), any_aggregate)


async def collect_nodes(include_finalized):
    tasks = dict()
    logging.debug("# daemon: %d", len(daemons))
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


async def run_handler(ws: websockets.WebSocketServerProtocol, payload: dict):
    user = check_auth(ws)
    n_jobs = payload.get('n_jobs')
    n_gpus = payload['n_gpus']
    name = payload['name']
    image = payload['image']
    cmd = payload['exec']
    env = payload.get('env', [])
    exc = payload.get('node_exclude', '').split(',')
    inc = payload.get('node_include', '').split(',')
    exc = list(filter(None, exc))
    inc = list(filter(None, inc))
    nodes = await collect_nodes(True)
    available = {}
    for node, info in nodes.items():
        if node in exc:
            continue
        if len(inc) and node not in inc:
            continue
        available[node] = info['free_gpu_ids']
        if n_jobs is None:
            if name in info['names']:
                raise AriesError(14, 'container of same name already exists!')
        else:
            for i in range(n_jobs):
                if ('%s-%d' % (name, i)) in info['names']:
                    raise AriesError(14, 'container of same name already exists!')
    if len(available) == 0:
        raise AriesError(19, "all nodes excluded")
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
                    exec=cmd,
                    user=user,
                    env=env
                ))))
    await asyncio.wait(tasks)
    return cat_aggregate([task.result() for task in tasks])


tcp_routes: Dict[str, list] = dict()
CLIENT, DAEMON, MSG_ID, WAITING = 0, 1, 2, 3


async def tcprecv_handler(payload):
    client = payload['client']
    tcp = tcp_routes.get(client)
    if tcp is None:
        return
    p = payload['p']

    tcp[WAITING] += 1
    if tcp[WAITING] == 8:
        await tcp[DAEMON].issue('tcpflowpause', dict(client=client))
    
    while tcp[MSG_ID] != p:
        await asyncio.sleep(0)
    await tcp[CLIENT].send(json.dumps(payload))
    tcp[MSG_ID] += 1

    tcp[WAITING] -= 1
    if tcp[WAITING] == 4:
        await tcp[DAEMON].issue('tcpflowresume', dict(client=client))


async def tcpconn_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws)
    ticket = payload['ticket']
    nodes = await collect_nodes(False)
    container = payload['container']
    tyck(container, str, 'container')
    port = payload['port']
    tyck(port, int, 'port')
    for snode, info in nodes.items():
        if container in info['names'] or container in info['ids']:
            for daemon in daemons:
                node = state_store[daemon.ws].auth_name
                if snode == node:
                    res = await daemon.issue('tcpconn', dict(client=ticket, container=container, port=port))
                    tcp_routes[ticket] = [ws, daemon, 0, 0]
                    res.pop('ticket')
                    return res
            raise NoResponse
    else:
        raise AriesError(17, "container `%s` not found" % container)


async def tcpsend_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws)
    client = payload['client']
    tyck(client, str, 'client')
    p = payload['p']
    tyck(p, int, 'p')
    for _ in range(30):
        if client in tcp_routes:
            break
        await asyncio.sleep(0.05)
    else:
        if client not in tcp_routes:
            raise AriesError(18, "tcp connection `%s` not found or connection timeout" % client)
    await tcp_routes[client][DAEMON].ws.send(json.dumps(payload))
    raise NoResponse


async def tcpstop_handler(ws: websockets.WebSocketServerProtocol, payload):
    check_auth(ws)
    client = payload['client']
    tyck(client, str, 'client')
    p = payload['p']
    tyck(p, int, 'p')
    tcp = tcp_routes.pop(client, None)
    if tcp is None:
        raise AriesError(18, "tcp connection `%s` not found" % client)
    daemon: AsyncClient = tcp[DAEMON]
    res = await daemon.issue('tcpstop', dict(client=client, p=p))
    res.pop('ticket')
    return res


dispatch = dict(
    auth=auth_handler,
    daemon=daemon_handler,
    logs=logs_handler,
    stop=stop_handler,
    kill=kill_handler,
    jstop=jstop_handler,
    delete=remove_handler,
    jdelete=jremove_handler,
    ps=ps_handler,
    nodes=nodes_handler,
    run=run_handler,
    tcpconn=tcpconn_handler,
    tcpsend=tcpsend_handler,
    tcpstop=tcpstop_handler,
)


async def handler(ws: websockets.WebSocketServerProtocol):
    state_store[ws] = CentralState()
    try:
        await command_handler(ws, dispatch, bypass_daemon)
    except Exception:
        logging.exception("Unexpected Error in Outer Loop")
    state_store.pop(ws)


async def main():
    import psutil
    print("I am", psutil.Process().pid)
    global stop_signal
    logging.basicConfig(level=logging.INFO)
    stop_signal = asyncio.Future()
    async with websockets.serve(handler, '127.0.0.1', 23549, max_size=2**25, compression=None):
        await stop_signal


def sync_main():
    asyncio.run(main())
