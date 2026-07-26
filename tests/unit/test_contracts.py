from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from trading_platform.common.contracts import EventEnvelope, QuoteData
from trading_platform.common.idempotency import request_fingerprint
from trading_platform.common.models import Position
from trading_platform.common.retry import RetryPolicy


def test_event_envelope_round_trip_uses_string_decimals_and_utc() -> None:
    quote = QuoteData(
        symbol="AAPL",
        bid=Decimal("209.9900"),
        ask=Decimal("210.0100"),
        last=Decimal("210.0000"),
        bid_size=250,
        ask_size=300,
        sequence=42,
        quoted_at=datetime(2026, 7, 26, 12, tzinfo=UTC),
    )
    envelope = EventEnvelope[QuoteData](
        event_type="market.quote.v1",
        producer="market-data",
        correlation_id=uuid4(),
        data=quote,
    )

    serialized = envelope.model_dump_json()
    restored = EventEnvelope[QuoteData].model_validate_json(serialized)

    assert '"bid":"209.9900"' in serialized
    assert restored == envelope
    assert restored.occurred_at.tzinfo is not None


def test_contract_rejects_naive_timestamps_and_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        QuoteData.model_validate(
            {
                "symbol": "AAPL",
                "bid": "1.0000",
                "ask": "1.0100",
                "last": "1.0000",
                "bid_size": 1,
                "ask_size": 1,
                "sequence": 0,
                "quoted_at": datetime(2026, 1, 1),
                "unexpected": True,
            }
        )


def test_idempotency_fingerprint_is_canonical_and_scoped() -> None:
    first = request_fingerprint(
        operation="create-order",
        path="/v1/orders",
        body={"symbol": "AAPL", "quantity": 2},
    )
    reordered = request_fingerprint(
        operation="create-order",
        path="/v1/orders",
        body={"quantity": 2, "symbol": "AAPL"},
    )
    other_path = request_fingerprint(
        operation="create-order",
        path="/internal/orders",
        body={"symbol": "AAPL", "quantity": 2},
    )

    assert first == reordered
    assert first != other_path


def test_retry_delay_is_bounded_and_deterministic_with_sample() -> None:
    policy = RetryPolicy(
        max_attempts=5,
        base_delay_seconds=1,
        max_delay_seconds=3,
        jitter_ratio=0.2,
    )

    assert policy.delay(1, random_value=0.5) == 1
    assert policy.delay(4, random_value=0.5) == 3


def test_new_position_has_immediate_financial_defaults() -> None:
    position = Position(account_id=uuid4(), symbol="AAPL")

    position.quantity += 2

    assert position.quantity == 2
    assert position.reserved_quantity == 0
    assert position.average_cost == Decimal("0")
    assert position.realized_pnl == Decimal("0")
