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

    def _replace_loop(self, func_or_coro, *args, **kwargs):
        kwargs['loop'] = self.loop
        return func_or_coro(*args, **kwargs)

    def ensure_future(self, *args, **kwargs):
        """Create a task."""
        return self._replace_loop(asyncio.ensure_future, *args, **kwargs)

    def run_in_executor(self, *args, **kwargs):
        """Use loop.run_in_executor() easily."""
        return self.loop.run_in_executor(self.executor, *args, **kwargs)

    def wait(self, *args, **kwargs):
        """Wrap asyncio.wait()."""
        return self._replace_loop(asyncio.wait, *args, **kwargs)

    def wait_for_first(self, *args, **kwargs):
        """Wait until the first future completes."""
        kwargs['return_when'] = concurrent.futures.FIRST_COMPLETED
        return self.wait(*args, **kwargs)

    def wait_forever(self, *args, **kwargs):
        """Wait for a future until it is done."""
        kwargs['timeout'] = None
        return self._replace_loop(asyncio.wait_for, *args, **kwargs)

    def call_soon_threadsafe(self, *args, **kwargs):
        """Use call_soon_threadsafe from the right loop."""
        return self.loop.call_soon_threadsafe(*args, **kwargs)

    def sleep(self, *args, **kwargs):
        """Sleep in the right loop."""
        return self._replace_loop(asyncio.sleep, *args, **kwargs)
