from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from hashlib import sha256
from typing import Any
from uuid import UUID

from trading_platform.common.enums import (
    TERMINAL_ORDER_STATUSES,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)

TERMINAL_STATUSES = TERMINAL_ORDER_STATUSES

ALLOWED_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.PENDING_VALIDATION: frozenset(
        {OrderStatus.ACCEPTED, OrderStatus.REJECTED, OrderStatus.FAILED}
    ),
    OrderStatus.ACCEPTED: frozenset(
        {OrderStatus.ACTIVE, OrderStatus.CANCEL_PENDING, OrderStatus.FAILED}
    ),
    OrderStatus.ACTIVE: frozenset(
        {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCEL_PENDING,
            OrderStatus.EXPIRED,
            OrderStatus.REJECTED,
            OrderStatus.FAILED,
        }
    ),
    OrderStatus.PARTIALLY_FILLED: frozenset(
        {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCEL_PENDING,
            OrderStatus.EXPIRED,
            OrderStatus.FAILED,
        }
    ),
    OrderStatus.CANCEL_PENDING: frozenset(
        {OrderStatus.CANCELLED, OrderStatus.FILLED, OrderStatus.FAILED}
    ),
    **{status: frozenset() for status in TERMINAL_STATUSES},
}


class InvalidTransition(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class Order:
    id: UUID
    account_id: UUID
    symbol: str
    side: OrderSide
    order_type: OrderType
    time_in_force: TimeInForce
    quantity: int
    status: OrderStatus = OrderStatus.PENDING_VALIDATION
    filled_quantity: int = 0
    average_fill_price: Decimal | None = None
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    triggered: bool = False
    version: int = 1
    accepted_at: datetime | None = None
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if not 0 <= self.filled_quantity <= self.quantity:
            raise ValueError("filled quantity must be between zero and quantity")
        if self.order_type in {OrderType.LIMIT, OrderType.STOP_LIMIT} and not _positive(
            self.limit_price
        ):
            raise ValueError("limit_price is required and must be positive")
        if self.order_type in {OrderType.STOP, OrderType.STOP_LIMIT} and not _positive(
            self.stop_price
        ):
            raise ValueError("stop_price is required and must be positive")
        if self.accepted_at and (
            self.accepted_at.tzinfo is None or self.accepted_at.utcoffset() is None
        ):
            raise ValueError("accepted_at must be timezone-aware")

    @property
    def remaining_quantity(self) -> int:
        return self.quantity - self.filled_quantity

    @property
    def terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES


@dataclass(frozen=True, slots=True)
class MatchResult:
    order: Order
    quantity: int = 0
    price: Decimal | None = None
    reason: str | None = None

    @property
    def matched(self) -> bool:
        return self.quantity > 0 and self.price is not None


def _positive(value: Decimal | None) -> bool:
    return value is not None and value > 0


def transition(order: Order, target: OrderStatus, *, at: datetime | None = None) -> Order:
    if target == order.status and target in TERMINAL_STATUSES:
        return order
    if target not in ALLOWED_TRANSITIONS[order.status]:
        raise InvalidTransition(f"cannot transition {order.status} to {target}")
    values: dict[str, Any] = {"status": target, "version": order.version + 1}
    if target == OrderStatus.ACCEPTED:
        accepted_at = at or datetime.now(UTC)
        values["accepted_at"] = accepted_at
        if order.time_in_force == TimeInForce.DAY:
            values["expires_at"] = accepted_at + timedelta(hours=24)
    return replace(order, **values)


def activate(order: Order, *, at: datetime | None = None) -> Order:
    if order.status == OrderStatus.PENDING_VALIDATION:
        order = transition(order, OrderStatus.ACCEPTED, at=at)
    return transition(order, OrderStatus.ACTIVE)


def cancel(order: Order) -> Order:
    if order.status in {OrderStatus.CANCEL_PENDING, OrderStatus.CANCELLED}:
        return order
    if order.terminal:
        raise InvalidTransition(f"cannot cancel terminal order in {order.status}")
    if order.status == OrderStatus.PENDING_VALIDATION:
        raise InvalidTransition("order cannot be cancelled before validation completes")
    return transition(order, OrderStatus.CANCEL_PENDING)


def expire(order: Order, *, now: datetime | None = None) -> Order:
    now = now or datetime.now(UTC)
    if order.time_in_force != TimeInForce.DAY or order.expires_at is None or now < order.expires_at:
        return order
    if order.status not in {OrderStatus.ACTIVE, OrderStatus.PARTIALLY_FILLED}:
        return order
    return transition(order, OrderStatus.EXPIRED)


def match_quote(
    order: Order,
    *,
    bid: Decimal,
    ask: Decimal,
    bid_size: int,
    ask_size: int,
    reservation_remaining: Decimal | int | None = None,
) -> MatchResult:
    if order.status not in {OrderStatus.ACTIVE, OrderStatus.PARTIALLY_FILLED}:
        return MatchResult(order, reason="order is not matchable")
    if bid <= 0 or ask <= 0 or ask < bid:
        raise ValueError("invalid quote")
    execution_price = ask if order.side == OrderSide.BUY else bid
    available_size = ask_size if order.side == OrderSide.BUY else bid_size
    if available_size < 0:
        raise ValueError("available size cannot be negative")

    triggered = order.triggered
    if order.order_type in {OrderType.STOP, OrderType.STOP_LIMIT} and not triggered:
        if order.stop_price is None:
            raise ValueError("stop price is required")
        triggered = (
            execution_price >= order.stop_price
            if order.side == OrderSide.BUY
            else execution_price <= order.stop_price
        )
        if not triggered:
            return MatchResult(order, reason="stop not triggered")
        order = replace(order, triggered=True, version=order.version + 1)

    is_limit = order.order_type == OrderType.LIMIT or order.order_type == OrderType.STOP_LIMIT
    if is_limit:
        if order.limit_price is None:
            raise ValueError("limit price is required")
        crosses = (
            execution_price <= order.limit_price
            if order.side == OrderSide.BUY
            else execution_price >= order.limit_price
        )
        if not crosses:
            return MatchResult(order, reason="limit not crossed")

    quantity = min(order.remaining_quantity, available_size)
    if reservation_remaining is not None:
        if order.side == OrderSide.BUY:
            affordable = int(
                (Decimal(reservation_remaining) / execution_price).to_integral_value(
                    rounding=ROUND_DOWN
                )
            )
            quantity = min(quantity, affordable)
        else:
            quantity = min(quantity, int(reservation_remaining))
    if quantity <= 0:
        if order.filled_quantity:
            order = transition(order, OrderStatus.CANCEL_PENDING)
            order = transition(order, OrderStatus.CANCELLED)
        else:
            order = transition(order, OrderStatus.REJECTED)
        return MatchResult(order, reason="reservation exhausted")
    return MatchResult(order, quantity, execution_price)


def apply_fill(order: Order, quantity: int, price: Decimal) -> Order:
    if order.status not in {
        OrderStatus.ACTIVE,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.CANCEL_PENDING,
    }:
        raise InvalidTransition(f"cannot fill order in {order.status}")
    if quantity <= 0 or quantity > order.remaining_quantity:
        raise ValueError("fill quantity must be positive and no greater than remaining quantity")
    if price <= 0:
        raise ValueError("fill price must be positive")
    old_notional = (order.average_fill_price or Decimal("0")) * order.filled_quantity
    filled_quantity = order.filled_quantity + quantity
    average = (old_notional + price * quantity) / filled_quantity
    status = (
        OrderStatus.FILLED if filled_quantity == order.quantity else OrderStatus.PARTIALLY_FILLED
    )
    if status not in ALLOWED_TRANSITIONS[order.status]:
        raise InvalidTransition(f"cannot transition {order.status} to {status}")
    return replace(
        order,
        filled_quantity=filled_quantity,
        average_fill_price=average,
        status=status,
        version=order.version + 1,
    )


def canonical_request_hash(method: str, path: str, payload: Any) -> str:
    body = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str
    )
    return sha256(f"{method.upper()}\n{path}\n{body}".encode()).hexdigest()


def deterministic_execution_id(order_id: UUID, quote_event_id: UUID) -> UUID:
    from uuid import uuid5

    return uuid5(order_id, str(quote_event_id))
