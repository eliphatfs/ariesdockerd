import asyncio


async def wait_any(awaitables):
    return await asyncio.wait([
        (asyncio.create_task(x) if asyncio.iscoroutine(x) else x) for x in awaitables
    ], return_when=asyncio.FIRST_COMPLETED)
