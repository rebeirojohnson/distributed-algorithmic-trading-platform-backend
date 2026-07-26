from __future__ import annotations

import random
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal

PRICE_QUANTUM = Decimal("0.0001")


@dataclass(frozen=True, slots=True)
class Instrument:
    symbol: str
    name: str
    initial_price: Decimal
    seed: int
    tick_size: Decimal = Decimal("0.01")
    currency: str = "USD"


@dataclass(frozen=True, slots=True)
class Quote:
    symbol: str
    sequence: int
    bid: Decimal
    ask: Decimal
    last: Decimal
    bid_size: int
    ask_size: int
    timestamp: datetime

    def as_dict(self) -> dict[str, str | int]:
        return {
            "symbol": self.symbol,
            "sequence": self.sequence,
            "bid": str(self.bid),
            "ask": str(self.ask),
            "last": str(self.last),
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "timestamp": self.timestamp.isoformat(),
        }


INSTRUMENTS: tuple[Instrument, ...] = (
    Instrument("AAPL", "Apple Inc.", Decimal("195.00"), 1101),
    Instrument("MSFT", "Microsoft Corporation", Decimal("425.00"), 1102),
    Instrument("GOOGL", "Alphabet Inc.", Decimal("175.00"), 1103),
    Instrument("AMZN", "Amazon.com, Inc.", Decimal("185.00"), 1104),
    Instrument("META", "Meta Platforms, Inc.", Decimal("500.00"), 1105),
    Instrument("NVDA", "NVIDIA Corporation", Decimal("125.00"), 1106),
    Instrument("TSLA", "Tesla, Inc.", Decimal("250.00"), 1107),
    Instrument("JPM", "JPMorgan Chase & Co.", Decimal("205.00"), 1108),
    Instrument("V", "Visa Inc.", Decimal("275.00"), 1109),
    Instrument("JNJ", "Johnson & Johnson", Decimal("155.00"), 1110),
    Instrument("WMT", "Walmart Inc.", Decimal("70.00"), 1111),
    Instrument("PG", "Procter & Gamble Co.", Decimal("165.00"), 1112),
    Instrument("XOM", "Exxon Mobil Corporation", Decimal("115.00"), 1113),
    Instrument("KO", "The Coca-Cola Company", Decimal("63.00"), 1114),
    Instrument("DIS", "The Walt Disney Company", Decimal("100.00"), 1115),
)


def normalize_symbol(value: str) -> str:
    symbol = value.strip().upper()
    if symbol not in {instrument.symbol for instrument in INSTRUMENTS}:
        raise ValueError(f"unknown symbol: {value}")
    return symbol


def _round_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick


class QuoteGenerator:
    """Independent per-symbol PRNGs make output stable regardless of iteration order."""

    def __init__(
        self,
        instruments: Iterable[Instrument] = INSTRUMENTS,
        *,
        clock: Callable[[], datetime] | None = None,
        max_move_bps: int = 18,
    ) -> None:
        if max_move_bps <= 0:
            raise ValueError("max_move_bps must be positive")
        self.instruments = {item.symbol: item for item in instruments}
        self._random = {
            symbol: random.Random(item.seed) for symbol, item in self.instruments.items()
        }
        self._mid = {symbol: item.initial_price for symbol, item in self.instruments.items()}
        self._sequence = {symbol: 0 for symbol in self.instruments}
        self._clock = clock or (lambda: datetime.now(UTC))
        self._max_move_bps = max_move_bps

    def next(self, symbol: str, *, timestamp: datetime | None = None) -> Quote:
        symbol = normalize_symbol(symbol)
        instrument = self.instruments[symbol]
        rng = self._random[symbol]
        move_bps = rng.randint(-self._max_move_bps, self._max_move_bps)
        moved = self._mid[symbol] * (Decimal("1") + Decimal(move_bps) / Decimal("10000"))
        mid = max(instrument.tick_size, _round_to_tick(moved, instrument.tick_size))
        spread_ticks = 1 + rng.randrange(3)
        half_spread = instrument.tick_size * spread_ticks / 2
        bid = max(instrument.tick_size, _round_to_tick(mid - half_spread, instrument.tick_size))
        ask = max(
            bid + instrument.tick_size, _round_to_tick(mid + half_spread, instrument.tick_size)
        )
        last = bid if rng.randrange(2) == 0 else ask
        self._sequence[symbol] += 1
        self._mid[symbol] = (bid + ask) / 2
        observed_at = timestamp or self._clock()
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("quote timestamp must be timezone-aware")
        return Quote(
            symbol=symbol,
            sequence=self._sequence[symbol],
            bid=bid.quantize(PRICE_QUANTUM),
            ask=ask.quantize(PRICE_QUANTUM),
            last=last.quantize(PRICE_QUANTUM),
            bid_size=(1 + rng.randrange(20)) * 25,
            ask_size=(1 + rng.randrange(20)) * 25,
            timestamp=observed_at.astimezone(UTC),
        )

    def next_all(self, *, timestamp: datetime | None = None) -> list[Quote]:
        observed_at = timestamp or self._clock()
        return [self.next(symbol, timestamp=observed_at) for symbol in self.instruments]
