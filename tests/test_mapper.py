# -*- coding: utf-8 -*-
"""Test timestamp formatting."""

import asyncio
import functools

import pytest

import roisto.mapper


@pytest.fixture
def mapper():
    def create_generator():
        yield {1: 1, 2: 2, 3: 3}
        yield {2: 2, 3: 3, 4: 4}

    async def update_coro(generator):
        return next(generator)

    update = functools.partial(update_coro, create_generator())
    parse = lambda x: x
    return roisto.mapper.Mapper(update, parse)


def test_mapper_update(mapper):
    loop = asyncio.get_event_loop()

    # Empty before first update.
    result = mapper.get(0)
    assert result is None
    result = mapper.get(1)
    assert result is None

    # Update only once if no get() in between.
    did_update = loop.run_until_complete(mapper.update())
    assert did_update
    did_update = loop.run_until_complete(mapper.update())
    assert not did_update

    # 0 is still not found.
    result = mapper.get(0)
    assert result is None

    # 0 should not trigger an update because it was missing already before the
    # update.
    did_update = loop.run_until_complete(mapper.update())
    assert not did_update

    # The update caused 1 to resolve.
    result = mapper.get(1)
    assert result == 1
    # The update found the other values.
    result = mapper.get(2)
    assert result == 2
    result = mapper.get(3)
    assert result == 3

    # Successful calls to get() should not trigger an update.
    did_update = loop.run_until_complete(mapper.update())
    assert not did_update

    # 4 should not be found in the first update.
    result = mapper.get(4)
    assert result is None

    # 4 should trigger an update.
    did_update = loop.run_until_complete(mapper.update())
    assert did_update

    # 4 should be found after second update.
    result = mapper.get(4)
    assert result == 4
