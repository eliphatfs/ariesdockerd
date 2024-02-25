import os
import json
import asyncio
import tabulate
import aioconsole
import websockets
from typing import Optional
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


async def run(name: str, image: str, cmd: str, n_gpus: int, n_jobs: Optional[int] = None):
    return await client_serial(ws, 'run', dict(name=name, image=image, cmd=cmd, n_gpus=n_gpus, n_jobs=n_jobs))


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
                cmd = await aioconsole.ainput('aries> ')
                if cmd in ['nodes', 'ps']:
                    cmd = cmd + '()'
                if cmd == 'quit':
                    break
                resp(await eval(cmd))
            except Exception as exc:
                print("[error]", repr(exc))
    finally:
        await ws.close()


def sync_main():
    asyncio.run(main())
