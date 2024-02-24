import asyncio
import websockets


async def handler(ws: websockets.WebSocketServerProtocol):
    pass


async def main():
    global stop_signal
    stop_signal = asyncio.Future()
    async with websockets.serve(handler, 'localhost', 23549):
        await stop_signal
