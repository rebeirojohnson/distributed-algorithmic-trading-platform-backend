from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from .domain import MatchResult, Order, expire, match_quote


async def quote_worker(
    receive: Callable[[], Awaitable[tuple[UUID, str, Decimal, Decimal, int, int]]],
    load_orders: Callable[[str], Awaitable[list[Order]]],
    persist_match: Callable[[UUID, Order, MatchResult], Awaitable[None]],
    *,
    stop: asyncio.Event | None = None,
) -> None:
    """Transport-neutral at-least-once quote worker skeleton."""
    stop = stop or asyncio.Event()
    while not stop.is_set():
        quote_event_id, symbol, bid, ask, bid_size, ask_size = await receive()
        for order in await load_orders(symbol):
            result = match_quote(order, bid=bid, ask=ask, bid_size=bid_size, ask_size=ask_size)
            if result.matched or result.order != order:
                await persist_match(quote_event_id, order, result)


async def expiry_worker(
    load_day_orders: Callable[[datetime], Awaitable[list[Order]]],
    persist_expiry: Callable[[Order], Awaitable[None]],
    *,
    interval_seconds: float = 5.0,
    stop: asyncio.Event | None = None,
) -> None:
    stop = stop or asyncio.Event()
    while not stop.is_set():
        now = datetime.now(UTC)
        for order in await load_day_orders(now):
            expired = expire(order, now=now)
            if expired != order:
                await persist_expiry(expired)
        try:
            await asyncio.wait_for(stop.wait(), interval_seconds)
        except TimeoutError:
            pass
