# -*- coding: utf-8 -*-
"""Utilities."""

import asyncio
import concurrent.futures

import isodate


def convert_duration_to_seconds(duration):
    """Convert ISO 8601 duration to float seconds."""
    return isodate.parse_duration(duration).total_seconds()


class AsyncHelper:
    """Collect asyncio helpers that require the same loop and executor."""

    def __init__(self, loop, executor=None):
        self.loop = loop
        self.executor = executor

    async def ensure_future(self, coro_or_future, *args):
        """Create a task."""
        return await asyncio.ensure_future(
            coro_or_future, *args, loop=self.loop)

    async def run_in_executor(self, func, *args):
        """Use asyncio.run_in_executor() easily."""
        return await self.loop.run_in_executor(self.executor, func, *args)

    async def wait_for_first(self, futures, *args):
        """Wait until the first future finishes."""
        return await asyncio.wait(
            futures,
            *args,
            loop=self.loop,
            timeout=None,
            return_when=concurrent.futures.FIRST_COMPLETED)

    async def wait_forever(self, future):
        """Wait for a future until it is done."""
        return await asyncio.wait_for(future, timeout=None, loop=self.loop)

    async def wait_for_event(self, event, *args):
        """Use threading.Event.wait() with asyncio."""
        return await self.run_in_executor(event.wait, *args)

    async def sleep(self, *args, **kwargs):
        """Sleep in the right loop."""
        return await asyncio.sleep(*args, **kwargs, loop=self.loop)
