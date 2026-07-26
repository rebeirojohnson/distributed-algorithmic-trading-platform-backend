from datetime import UTC, datetime
from decimal import Decimal

import pytest

from trading_platform.market_data.domain import INSTRUMENTS, QuoteGenerator

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def test_generator_has_fifteen_symbols_and_is_reproducible() -> None:
    assert len(INSTRUMENTS) == 15
    first = QuoteGenerator(clock=lambda: NOW)
    second = QuoteGenerator(clock=lambda: NOW)

    assert first.next_all() == second.next_all()
    assert first.next_all() == second.next_all()


def test_symbol_stream_does_not_depend_on_other_symbols() -> None:
    isolated = QuoteGenerator(clock=lambda: NOW)
    interleaved = QuoteGenerator(clock=lambda: NOW)

    expected = [isolated.next("AAPL") for _ in range(3)]
    actual = []
    for _ in range(3):
        actual.append(interleaved.next("AAPL"))
        interleaved.next("MSFT")
    assert actual == expected


def test_quotes_preserve_market_invariants() -> None:
    generator = QuoteGenerator(clock=lambda: NOW)

    for quote in generator.next_all():
        assert Decimal("0") < quote.bid < quote.ask
        assert quote.last in {quote.bid, quote.ask}
        assert quote.bid_size > 0
        assert quote.ask_size > 0


def test_naive_timestamp_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        QuoteGenerator().next("AAPL", timestamp=datetime(2026, 1, 1))
