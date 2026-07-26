from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from .schemas import ExecutionEvent, ValidationRequest


async def validation_worker(
    receive: Callable[[], Awaitable[ValidationRequest]],
    handle: Callable[[ValidationRequest], Awaitable[None]],
    *,
    stop: asyncio.Event | None = None,
) -> None:
    stop = stop or asyncio.Event()
    while not stop.is_set():
        event = await receive()
        await handle(event)


async def execution_worker(
    receive: Callable[[], Awaitable[ExecutionEvent]],
    handle: Callable[[ExecutionEvent], Awaitable[None]],
    *,
    stop: asyncio.Event | None = None,
) -> None:
    stop = stop or asyncio.Event()
    while not stop.is_set():
        event = await receive()
        await handle(event)
