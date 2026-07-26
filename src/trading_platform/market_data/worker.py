from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Awaitable, Callable
from typing import Protocol

from .domain import Quote, QuoteGenerator
from .snapshots import QuoteSnapshotStore


class QuotePublisher(Protocol):
    def send(self, topic: str, *, key: bytes, value: bytes) -> Awaitable[object] | object: ...


async def publish_quote(
    publisher: QuotePublisher, quote: Quote, *, topic: str = "market.quotes.v1"
) -> None:
    result = publisher.send(
        topic,
        key=quote.symbol.encode(),
        value=json.dumps(quote.as_dict(), separators=(",", ":")).encode(),
    )
    if inspect.isawaitable(result):
        await result


async def quote_loop(
    generator: QuoteGenerator,
    snapshots: QuoteSnapshotStore,
    publisher: QuotePublisher,
    *,
    interval_seconds: float = 1.0,
    stop: asyncio.Event | None = None,
    on_error: Callable[[Exception], None] | None = None,
) -> None:
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    stop = stop or asyncio.Event()
    while not stop.is_set():
        for quote in generator.next_all():
            try:
                await snapshots.put(quote)
                await publish_quote(publisher, quote)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if on_error:
                    on_error(exc)
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        except TimeoutError:
            pass
