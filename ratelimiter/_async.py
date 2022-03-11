# -*- coding: utf-8 -*-

""" Async support for 3.5+ """

import time
import asyncio

from ._sync import RateLimiter


class AsyncRateLimiter(RateLimiter):
    def _init_async_lock(self):
        with self._init_lock:
            if self._alock is None:
                self._alock = asyncio.Lock()

    async def __aenter__(self):
        if self._alock is None:
            self._init_async_lock()

        with await self._alock:
            # We want to ensure that no more than max_calls were run in the allowed
            # period. For this, we store the last timestamps of each call and run
            # the rate verification upon each __enter__ call.
            if len(self.calls) >= self.max_calls:
                until = time.time() + self.period - self._timespan
                if self.callback:
                    asyncio.ensure_future(self.callback(until))
                sleeptime = until - time.time()
                if sleeptime > 0:
                    await asyncio.sleep(sleeptime)
            return self

    __aexit__ = asyncio.coroutine(RateLimiter.__exit__)
