from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

from .domain import Quote


class RedisLike(Protocol):
    async def set(self, key: str, value: str, *, ex: int) -> object: ...

    async def get(self, key: str) -> str | bytes | None: ...

    async def mget(self, keys: list[str]) -> list[str | bytes | None]: ...


class QuoteSnapshotStore:
    def __init__(
        self, redis: RedisLike, *, ttl_seconds: int = 30, prefix: str = "quotes:latest:"
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self.redis = redis
        self.ttl_seconds = ttl_seconds
        self.prefix = prefix

    async def put(self, quote: Quote) -> None:
        await self.redis.set(
            self.prefix + quote.symbol,
            json.dumps(quote.as_dict(), separators=(",", ":")),
            ex=self.ttl_seconds,
        )

    async def get(self, symbol: str) -> Quote | None:
        raw = await self.redis.get(self.prefix + symbol)
        return self._decode(raw)

    async def get_many(self, symbols: Iterable[str]) -> list[Quote]:
        symbol_list = list(symbols)
        values = await self.redis.mget([self.prefix + symbol for symbol in symbol_list])
        return [quote for value in values if (quote := self._decode(value)) is not None]

    @staticmethod
    def _decode(raw: str | bytes | None) -> Quote | None:
        if raw is None:
            return None
        payload = json.loads(raw)
        return Quote(
            symbol=payload["symbol"],
            sequence=int(payload["sequence"]),
            bid=Decimal(payload["bid"]),
            ask=Decimal(payload["ask"]),
            last=Decimal(payload["last"]),
            bid_size=int(payload["bid_size"]),
            ask_size=int(payload["ask_size"]),
            timestamp=datetime.fromisoformat(payload["timestamp"]).astimezone(UTC),
        )
