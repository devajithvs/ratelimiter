import pytest

from ratelimiter import RateLimiter


@pytest.mark.asyncio
async def test_alock(event_loop):
    rl = RateLimiter(max_calls=10, period=0.01)

    assert rl._alock is None

    async with rl:
        pass

    alock = rl._alock
    assert alock

    async with rl:
        pass

    assert rl._alock is alock
