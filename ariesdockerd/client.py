import os
import json
import asyncio
import aioconsole
import websockets
from .protocol import client_serial


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


async def main():
    global ws
    if not os.path.exists(os.path.expanduser("~/.aries/config.json")):
        await first_time_config()
    with open(os.path.expanduser("~/.aries/config.json")) as fi:
        cfg = json.load(fi)
    ws = await websockets.connect(cfg['addr'])
    await client_serial(ws, 'auth', dict(token=cfg['token']))
    while True:
        await eval(await aioconsole.ainput('aries> '))


def sync_main():
    asyncio.run(main())
