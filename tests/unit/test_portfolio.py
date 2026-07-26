from decimal import Decimal
from uuid import uuid4

import pytest

from trading_platform.common.enums import OrderSide, ReservationStatus
from trading_platform.portfolio.domain import (
    CashBalance,
    PortfolioState,
    Position,
    release_reservation,
    reserve_cash,
    reserve_shares,
    settle_fill,
    unrealized_pnl,
)


def test_buy_settlement_uses_weighted_average_cost_and_deduplicates() -> None:
    state = PortfolioState(
        cash=CashBalance(Decimal("1000")),
        positions={"AAPL": Position("AAPL", 10, Decimal("10"))},
    )
    order_id = uuid4()
    event_id = uuid4()
    reserve_cash(state, order_id, Decimal("240"))

    result = settle_fill(
        state,
        event_id=event_id,
        order_id=order_id,
        symbol="AAPL",
        side=OrderSide.BUY,
        quantity=10,
        price=Decimal("20"),
    )
    assert result.position is not None
    assert result.position.quantity == 20
    assert result.position.average_cost == Decimal("15")
    assert state.cash.total == Decimal("800")
    assert state.cash.reserved == Decimal("40")

    duplicate = settle_fill(
        state,
        event_id=event_id,
        order_id=order_id,
        symbol="AAPL",
        side=OrderSide.BUY,
        quantity=10,
        price=Decimal("20"),
    )
    assert duplicate.duplicate
    assert state.cash.total == Decimal("800")


def test_sell_settlement_realizes_profit_and_preserves_cash_invariants() -> None:
    state = PortfolioState(
        cash=CashBalance(Decimal("500")),
        positions={"AAPL": Position("AAPL", 10, Decimal("10"))},
    )
    order_id = uuid4()
    reserve_shares(state, order_id, "AAPL", 6)
    result = settle_fill(
        state,
        event_id=uuid4(),
        order_id=order_id,
        symbol="AAPL",
        side=OrderSide.SELL,
        quantity=6,
        price=Decimal("15"),
    )
    assert result.realized_pnl_delta == Decimal("30")
    assert result.position is not None
    assert result.position.quantity == 4
    assert result.position.realized_pnl == Decimal("30")
    assert state.cash.total == Decimal("590")
    assert result.reservation.state == ReservationStatus.CONSUMED


def test_release_restores_available_resources() -> None:
    state = PortfolioState(cash=CashBalance(Decimal("100")))
    order_id = uuid4()
    reserve_cash(state, order_id, Decimal("40"))
    released = release_reservation(state, order_id)
    assert released.state == ReservationStatus.RELEASED
    assert state.cash.available == Decimal("100")


def test_insufficient_reservations_are_rejected() -> None:
    state = PortfolioState(cash=CashBalance(Decimal("100")))
    with pytest.raises(ValueError, match="insufficient available cash"):
        reserve_cash(state, uuid4(), Decimal("101"))
    with pytest.raises(ValueError, match="insufficient available shares"):
        reserve_shares(state, uuid4(), "AAPL", 1)


def test_unrealized_pnl() -> None:
    position = Position("AAPL", 4, Decimal("10"))
    assert unrealized_pnl(position, Decimal("12.5")) == Decimal("10.0")
