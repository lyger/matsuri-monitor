import functools

import aiohttp


def http_session_method(f):
    """Decorator for a method that uses an async http session"""

    @functools.wraps(f)
    async def wrapper(self, *args, **kwargs):
        async with aiohttp.ClientSession() as session:
            return await f(self, session, *args, **kwargs)

    return wrapper
