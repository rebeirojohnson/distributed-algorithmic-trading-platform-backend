from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from trading_platform.common.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from trading_platform.orders.domain import (
    InvalidTransition,
    Order,
    activate,
    apply_fill,
    cancel,
    expire,
    match_quote,
    transition,
)
from trading_platform.orders.reservations import reservation_required


def order(**changes: object) -> Order:
    values: dict[str, object] = {
        "id": uuid4(),
        "account_id": uuid4(),
        "symbol": "AAPL",
        "side": OrderSide.BUY,
        "order_type": OrderType.MARKET,
        "time_in_force": TimeInForce.DAY,
        "quantity": 10,
    }
    values.update(changes)
    return Order(**values)  # type: ignore[arg-type]


def test_lifecycle_and_terminal_immutability() -> None:
    value = activate(order(), at=datetime(2026, 1, 1, tzinfo=UTC))
    value = apply_fill(value, 4, Decimal("10"))
    assert value.status == OrderStatus.PARTIALLY_FILLED
    value = apply_fill(value, 6, Decimal("20"))
    assert value.status == OrderStatus.FILLED
    assert value.average_fill_price == Decimal("16")

    with pytest.raises(InvalidTransition):
        transition(value, OrderStatus.ACTIVE)


def test_limit_stop_and_partial_liquidity_matching() -> None:
    limit = activate(order(order_type=OrderType.LIMIT, limit_price=Decimal("100"), quantity=12))
    assert not match_quote(
        limit, bid=Decimal("100"), ask=Decimal("101"), bid_size=20, ask_size=5
    ).matched
    result = match_quote(limit, bid=Decimal("98"), ask=Decimal("99"), bid_size=20, ask_size=5)
    assert result.quantity == 5
    assert result.price == Decimal("99")

    stop = activate(order(order_type=OrderType.STOP, stop_price=Decimal("110")))
    assert not match_quote(
        stop, bid=Decimal("100"), ask=Decimal("101"), bid_size=10, ask_size=10
    ).matched
    result = match_quote(stop, bid=Decimal("110"), ask=Decimal("111"), bid_size=10, ask_size=10)
    assert result.matched
    assert result.order.triggered


def test_reservation_caps_fill_and_terminates_exhausted_remainder() -> None:
    value = activate(order(quantity=10))
    result = match_quote(
        value,
        bid=Decimal("9"),
        ask=Decimal("10"),
        bid_size=100,
        ask_size=100,
        reservation_remaining=Decimal("35"),
    )
    assert result.quantity == 3
    partial = apply_fill(result.order, result.quantity, result.price or Decimal("0"))
    exhausted = match_quote(
        partial,
        bid=Decimal("9"),
        ask=Decimal("10"),
        bid_size=100,
        ask_size=100,
        reservation_remaining=Decimal("5"),
    )
    assert exhausted.order.status == OrderStatus.CANCELLED


def test_cancel_is_idempotent_and_day_order_expires() -> None:
    accepted_at = datetime(2026, 1, 1, tzinfo=UTC)
    value = activate(order(), at=accepted_at)
    pending = cancel(value)
    assert cancel(pending) is pending

    expired = expire(value, now=accepted_at + timedelta(hours=24))
    assert expired.status == OrderStatus.EXPIRED


def test_reservation_formulas() -> None:
    assert reservation_required(
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10,
        limit_price=Decimal("12.5"),
    ) == Decimal("125")
    assert reservation_required(
        side=OrderSide.BUY,
        order_type=OrderType.STOP,
        quantity=2,
        current_ask=Decimal("100"),
        stop_price=Decimal("110"),
    ) == Decimal("231.00")
    assert reservation_required(side=OrderSide.SELL, order_type=OrderType.MARKET, quantity=7) == 7
