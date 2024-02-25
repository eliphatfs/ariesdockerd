import os
import json
import shlex
import asyncio
import tabulate
import argparse
import aioconsole
import websockets
from typing import Optional, List
from .protocol import client_serial


async def first_time_config():
    addr = await aioconsole.ainput("Input Server Address> ")
    token = await aioconsole.ainput("Input Token> ")
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
            row = [name, ','.join(map(str, info['free_gpu_ids']))]
            if show_jobs:
                row.append('\n'.join(info['names']))
            table.append(row)
        print(tabulate.tabulate(table, headers=header))
    return r


async def ps(filt: Optional[str] = None):
    r = await client_serial(ws, 'ps', dict(filt=filt))
    if r['code'] == 0:
        header = ['ID', 'Name', 'Status', 'User', 'GPUs']
        table = []
        for k, v in r['containers']:
            table.append([k, v['name'], v['status'], v['user'], ','.join('gpu_ids')])
        print(tabulate.tabulate(table, headers=header))
    return r


async def logs(container: str):
    r = await client_serial(ws, 'logs', dict(container=container))
    if r['code'] == 0:
        print(r['logs'])
    return r


async def stop(container: str):
    return await client_serial(ws, 'stop', dict(container=container))


async def delete(container: str):
    return await client_serial(ws, 'delete', dict(container=container))


async def run(name: str, image: str, cmd: List[str], n_gpus: int, n_jobs: Optional[int] = None):
    cmd = ' '.join(cmd)
    return await client_serial(ws, 'run', dict(name=name, image=image, cmd=cmd, n_gpus=n_gpus, n_jobs=n_jobs))


async def run_command(argv):
    argp = argparse.ArgumentParser()
    subs = argp.add_subparsers(dest='command')

    pnodes = subs.add_parser('nodes')
    pnodes.add_argument('-j', '--show_jobs', action='store_true')

    pps = subs.add_parser('ps')
    pps.add_argument('filt', nargs='?', default=None, type=str)

    plogs = subs.add_parser('logs')
    plogs.add_argument('container')

    pstop = subs.add_parser('stop')
    pstop.add_argument('container')

    pdelete = subs.add_parser('delete')
    pdelete.add_argument('container')

    prun = subs.add_parser('run')
    prun.add_argument('-j', '--n_jobs', default=None, type=int)
    prun.add_argument('-g', '--n_gpus', default=1, type=int)
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


async def main():
    global ws
    if not os.path.exists(os.path.expanduser("~/.aries/config.json")):
        await first_time_config()
    with open(os.path.expanduser("~/.aries/config.json")) as fi:
        cfg = json.load(fi)
    ws = await websockets.connect(cfg['addr'])
    try:
        auth = await client_serial(ws, 'auth', dict(token=cfg['token']))
        if auth['code'] != 0:
            print('[error] login failed:', auth['msg'])
        print('logged in as', auth['user'])
        while True:
            try:
                cmd: str = await aioconsole.ainput('aries> ')
                if cmd == 'q':
                    break
                argv = shlex.split(cmd)
                resp(await run_command(argv))
            except Exception as exc:
                print("[error]", repr(exc))
    finally:
        await ws.close()


def sync_main():
    asyncio.run(main())
