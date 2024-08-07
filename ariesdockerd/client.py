import os
import sys
import uuid
import json
import shlex
import base64
import signal
import asyncio
import argparse
import tabulate
import websockets
from aiocmd import aiocmd
from typing import Optional, List, Dict
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.patch_stdout import patch_stdout
from .protocol import client_serial


interrupt_callbacks = []


class ResetSignal(Exception):
    pass


async def first_time_config():
    addr = input("Input Server Address> ")
    token = input("Input Token> ")
    os.makedirs(os.path.expanduser("~/.aries"), exist_ok=True)
    with open(os.path.expanduser("~/.aries/config.json"), "w") as fo:
        json.dump(dict(
            addr=addr,
            token=token
        ), fo, indent=4)
    print("Config Saved to", os.path.expanduser("~/.aries/config.json"))


def resp(result):
    if result['code'] == 0:
        print("[done]")
    else:
        print("[error]", result.get('code', '-2'), result.get('msg', 'unknown'))


async def nodes(show_jobs=False):
    r = await client_serial(ws, 'nodes', dict())
    if r['code'] == 0:
        header = ['Node', 'Free GPUs']
        if show_jobs:
            header.append('Running')
        table = []
        for name, info in r['nodes'].items():
            row = [name, ','.join(map(str, info.get('free_gpu_ids', [])))]
            if show_jobs:
                row.append('\n'.join(info['names']))
            table.append(row)
        table = sorted(table, key=lambda x: x[0])
        print(tabulate.tabulate(table, headers=header))
    return r


async def ps(filt: Optional[str] = None):
    r = await client_serial(ws, 'ps', dict(filt=filt))
    if r['code'] == 0:
        header = ['ID', 'Name', 'Status', 'User', 'Node', 'GPUs']
        table = []
        for k, v in r['containers'].items():
            table.append([k, v['name'], v['status'], v['user'], v['node'], ','.join(map(str, v['gpu_ids']))])
        table = sorted(table, key=lambda x: x[1])
        print(tabulate.tabulate(table, headers=header))
    return r


async def logs(container: str, output: str = None, follow: bool = False):
    if follow:
        r = await client_serial(ws, 'follow_logs', dict(container=container))
        while True:
            try:
                res = await client_serial(ws, 'poll_logs', dict(follower=r['follower']))
            except Exception:
                import traceback
                traceback.print_exc()
                break
            print(res['log'], end='')
        return r
    r = await client_serial(ws, 'logs', dict(container=container))
    if r['code'] == 0:
        if output is None:
            print(r['logs'])
        else:
            with open(output, "w", errors='ignore') as fo:
                print(r['logs'], file=fo)
    return r


async def stop(container: str):
    return await client_serial(ws, 'stop', dict(container=container))


async def kill(container: str):
    return await client_serial(ws, 'kill', dict(container=container))


async def jstop(job: str):
    return await client_serial(ws, 'jstop', dict(job=job))


async def delete(container: str):
    return await client_serial(ws, 'delete', dict(container=container))


async def jdelete(job: str):
    return await client_serial(ws, 'jdelete', dict(job=job))


async def run(name: str, image: str, cmd: List[str], n_gpus: int, n_jobs: Optional[int] = None, env: Optional[list] = None, node_exclude: str = '', node_include: str = '', timeout: int = 0):
    return await client_serial(ws, 'run', dict(name=name, image=image, exec=cmd, n_gpus=n_gpus, n_jobs=n_jobs, env=env, node_exclude=node_exclude, node_include=node_include, timeout=timeout))


async def ws_to_tcp(ws: websockets.WebSocketCommonProtocol, tcp: asyncio.StreamWriter):
    async for msg in ws:
        tcp.write(msg)


async def tcp_to_ws(tcp: asyncio.StreamReader, ws: websockets.WebSocketCommonProtocol):
    while True:
        try:
            nxt = await asyncio.wait_for(tcp.read(16384), 1800)
        except asyncio.TimeoutError:
            nxt = b""
        if not len(nxt):
            return
        await ws.send(nxt)


async def portfwd(container: str, port: str):
    if ':' in port:
        remoteport, localport = map(int, port.split(':'))
    else:
        remoteport, localport = int(port), int(port)
    r = await client_serial(ws, 'tcpfwd2', dict(container=container, port=remoteport))
    if r['code'] != 0:
        return r
    session = r['session']

    with open(os.path.expanduser("~/.aries/config.json")) as fi:
        cfg = json.load(fi)
    
    async def portfwd_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        print("[info] got connection on port", localport)
        c2 = await websockets.connect(cfg['addr'] + "/tcp2/c/" + session, max_size=2**22)
        print("[info] start forwarding to port", remoteport)
        try:
            t1 = asyncio.create_task(ws_to_tcp(c2, writer))
            t2 = asyncio.create_task(tcp_to_ws(reader, c2))
            await asyncio.wait([t1, t2], return_when="FIRST_COMPLETED")
        finally:
            if c2.open:
                await c2.close()
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            print("[info] handled connection on port", localport)

    server = await asyncio.start_server(portfwd_client, host='127.0.0.1', port=localport)
    print("[info] serving on port", localport)
    callback = lambda: server.close()
    interrupt_callbacks.append(callback)
    try:
        await server.wait_closed()
    finally:
        server.close()
        interrupt_callbacks.remove(callback)


async def reconnect():
    global ws
    with open(os.path.expanduser("~/.aries/config.json")) as fi:
        cfg = json.load(fi)
    try:
        if not ws.closed:
            await ws.close()
    except Exception as exc:
        print('[warn] error closing old connection', repr(exc))
    ws = await websockets.connect(cfg['addr'], max_size=2**26)
    auth = await client_serial(ws, 'auth', dict(token=cfg['token']))
    return auth


async def source(file):
    with open(file) as fi:
        for cmd in fi:
            if cmd.startswith('#'):
                continue
            if not cmd.strip():
                continue
            resp(await run_command(shlex.split(cmd)))
    return dict(code=0)


async def run_command(argv):
    argp = argparse.ArgumentParser()
    subs = argp.add_subparsers(dest='command')

    pnodes = subs.add_parser('nodes')
    pnodes.add_argument('-j', '--show_jobs', action='store_true')

    pps = subs.add_parser('ps')
    pps.add_argument('filt', nargs='?', default=None, type=str)

    subs.add_parser('reconnect')

    plogs = subs.add_parser('logs')
    plogs.add_argument('container')
    plogs.add_argument('-o', '--output', default=None, type=str)
    plogs.add_argument('-f', '--follow', action='store_true')

    pstop = subs.add_parser('stop')
    pstop.add_argument('container')

    pstop = subs.add_parser('kill')
    pstop.add_argument('container')

    pstop = subs.add_parser('jstop')
    pstop.add_argument('job')

    pdelete = subs.add_parser('delete')
    pdelete.add_argument('container')

    pdelete = subs.add_parser('jdelete')
    pdelete.add_argument('job')

    pfwd = subs.add_parser('portfwd')
    pfwd.add_argument('container')
    pfwd.add_argument('port')

    psource = subs.add_parser('source')
    psource.add_argument('file')

    prun = subs.add_parser('run')
    prun.add_argument('-x', '--node_exclude', default='', type=str)
    prun.add_argument('-n', '--node_include', default='', type=str)
    prun.add_argument('-j', '--n_jobs', default=None, type=int)
    prun.add_argument('-g', '--n_gpus', default=1, type=int)
    prun.add_argument('-t', '--timeout', default=0, type=int)
    prun.add_argument('-e', '--env', metavar="KEY=VALUE", nargs='*', default=[])
    prun.add_argument('name')
    prun.add_argument('image')
    prun.add_argument('cmd', nargs='+')

    try:
        args = argp.parse_args(argv)
    except SystemExit:
        raise ValueError("invalid command")
    kw = dict(args.__dict__)
    cmd = kw.pop('command')
    return await (globals()[cmd])(**kw)


class AriesShell(aiocmd.PromptToolkitCmd):
    prompt = "aries> "

    @property
    def command_list(self):
        return [
            'nodes', 'ps', 'logs',
            'stop', 'kill', 'jstop',
            'delete', 'jdelete',
            'portfwd', 'reconnect',
            'run', 'source',
            'q',
            '?', 'help'
        ]

    def _interrupt_handler(self, event):
        print(self.prompt + event.cli.current_buffer.text + "^C")
        event.cli.current_buffer.text = ""

    async def run(self):
        if self._ignore_sigint and sys.platform != "win32":
            asyncio.get_event_loop().add_signal_handler(signal.SIGINT, self._sigint_handler)
        self.session = PromptSession(
            enable_history_search=True, key_bindings=self._get_bindings(),
            history=FileHistory(os.path.expanduser("~/.aries/history"))
        )
        try:
            with patch_stdout():
                await self._run_prompt_forever()
        finally:
            if self._ignore_sigint and sys.platform != "win32":
                asyncio.get_event_loop().remove_signal_handler(signal.SIGINT)
            self._on_close()

    async def _run_single_command(self, command, args):
        if command == 'q':
            raise aiocmd.ExitPromptException
        if ws.closed:
            print("Connection to server lost. Reconnecting...")
            resp(await reconnect())
        try:
            resp(await run_command([command] + args))
        except Exception as exc:
            print("[error]", repr(exc))


async def main():
    global ws
    if not os.path.exists(os.path.expanduser("~/.aries/config.json")):
        await first_time_config()
    with open(os.path.expanduser("~/.aries/config.json")) as fi:
        cfg = json.load(fi)
    ws = await websockets.connect(cfg['addr'], max_size=2**26)
    try:
        auth = await client_serial(ws, 'auth', dict(token=cfg['token']))
        if auth['code'] != 0:
            print('[error] login failed:', auth['msg'])
        else:
            print('logged in as', auth['user'])
        if len(sys.argv) > 1:
            resp(await run_command(sys.argv[1:]))
        else:
            await AriesShell(False).run()
    finally:
        await ws.close()


def sync_main():
    asyncio.run(main())