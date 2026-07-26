from __future__ import annotations

from dataclasses import dataclass, field, replace
from decimal import Decimal
from uuid import UUID

from trading_platform.common.enums import OrderSide, ReservationKind, ReservationStatus

MONEY_QUANTUM = Decimal("0.0001")


@dataclass(frozen=True, slots=True)
class CashBalance:
    total: Decimal
    reserved: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        if self.total < 0 or self.reserved < 0 or self.reserved > self.total:
            raise ValueError("cash balance invariant violated")

    @property
    def available(self) -> Decimal:
        return self.total - self.reserved


@dataclass(frozen=True, slots=True)
class Position:
    symbol: str
    quantity: int
    average_cost: Decimal
    realized_pnl: Decimal = Decimal("0")
    reserved_quantity: int = 0

    def __post_init__(self) -> None:
        if self.quantity < 0 or not 0 <= self.reserved_quantity <= self.quantity:
            raise ValueError("position quantity invariant violated")
        if self.average_cost < 0:
            raise ValueError("average cost cannot be negative")

    @property
    def available_quantity(self) -> int:
        return self.quantity - self.reserved_quantity


@dataclass(frozen=True, slots=True)
class Reservation:
    order_id: UUID
    kind: ReservationKind
    original_amount: Decimal
    remaining_amount: Decimal
    state: ReservationStatus = ReservationStatus.ACTIVE

    def __post_init__(self) -> None:
        if self.original_amount <= 0 or not 0 <= self.remaining_amount <= self.original_amount:
            raise ValueError("reservation amount invariant violated")


@dataclass(slots=True)
class PortfolioState:
    cash: CashBalance
    positions: dict[str, Position] = field(default_factory=dict)
    reservations: dict[UUID, Reservation] = field(default_factory=dict)
    processed_event_ids: set[UUID] = field(default_factory=set)


@dataclass(frozen=True, slots=True)
class SettlementResult:
    duplicate: bool
    cash: CashBalance
    position: Position | None
    reservation: Reservation
    realized_pnl_delta: Decimal = Decimal("0")


def reserve_cash(state: PortfolioState, order_id: UUID, amount: Decimal) -> Reservation:
    if amount <= 0:
        raise ValueError("reservation amount must be positive")
    existing = state.reservations.get(order_id)
    if existing:
        if existing.kind != ReservationKind.CASH or existing.original_amount != amount:
            raise ValueError("order already has a different reservation")
        return existing
    if amount > state.cash.available:
        raise ValueError("insufficient available cash")
    reservation = Reservation(order_id, ReservationKind.CASH, amount, amount)
    state.cash = replace(state.cash, reserved=state.cash.reserved + amount)
    state.reservations[order_id] = reservation
    return reservation


def reserve_shares(
    state: PortfolioState, order_id: UUID, symbol: str, quantity: int
) -> Reservation:
    if quantity <= 0:
        raise ValueError("reservation quantity must be positive")
    existing = state.reservations.get(order_id)
    if existing:
        if existing.kind != ReservationKind.SHARES or existing.original_amount != quantity:
            raise ValueError("order already has a different reservation")
        return existing
    position = state.positions.get(symbol)
    if position is None or quantity > position.available_quantity:
        raise ValueError("insufficient available shares")
    reservation = Reservation(
        order_id, ReservationKind.SHARES, Decimal(quantity), Decimal(quantity)
    )
    state.positions[symbol] = replace(
        position, reserved_quantity=position.reserved_quantity + quantity
    )
    state.reservations[order_id] = reservation
    return reservation


def settle_fill(
    state: PortfolioState,
    *,
    event_id: UUID,
    order_id: UUID,
    symbol: str,
    side: OrderSide,
    quantity: int,
    price: Decimal,
) -> SettlementResult:
    reservation = state.reservations.get(order_id)
    if reservation is None or reservation.state != ReservationStatus.ACTIVE:
        raise ValueError("active reservation not found")
    position = state.positions.get(symbol)
    if event_id in state.processed_event_ids:
        return SettlementResult(True, state.cash, position, reservation)
    if quantity <= 0 or price <= 0:
        raise ValueError("fill quantity and price must be positive")

    if side == OrderSide.BUY:
        if reservation.kind != ReservationKind.CASH:
            raise ValueError("buy fill requires a cash reservation")
        cost = price * quantity
        if cost > reservation.remaining_amount or cost > state.cash.total:
            raise ValueError("fill exceeds reserved or total cash")
        old_quantity = position.quantity if position else 0
        old_average = position.average_cost if position else Decimal("0")
        new_quantity = old_quantity + quantity
        average_cost = ((old_average * old_quantity) + cost) / new_quantity
        position = Position(
            symbol=symbol,
            quantity=new_quantity,
            average_cost=average_cost,
            realized_pnl=position.realized_pnl if position else Decimal("0"),
            reserved_quantity=position.reserved_quantity if position else 0,
        )
        state.cash = CashBalance(state.cash.total - cost, state.cash.reserved - cost)
        remaining = reservation.remaining_amount - cost
        realized_delta = Decimal("0")
    else:
        if reservation.kind != ReservationKind.SHARES or position is None:
            raise ValueError("sell fill requires a share reservation and position")
        amount = Decimal(quantity)
        if amount > reservation.remaining_amount or quantity > position.quantity:
            raise ValueError("fill exceeds reserved shares")
        proceeds = price * quantity
        realized_delta = (price - position.average_cost) * quantity
        remaining_quantity = position.quantity - quantity
        position = Position(
            symbol=symbol,
            quantity=remaining_quantity,
            average_cost=position.average_cost if remaining_quantity else Decimal("0"),
            realized_pnl=position.realized_pnl + realized_delta,
            reserved_quantity=position.reserved_quantity - quantity,
        )
        state.cash = CashBalance(state.cash.total + proceeds, state.cash.reserved)
        remaining = reservation.remaining_amount - amount

    reservation = replace(
        reservation,
        remaining_amount=remaining,
        state=ReservationStatus.CONSUMED if remaining == 0 else ReservationStatus.ACTIVE,
    )
    state.positions[symbol] = position
    state.reservations[order_id] = reservation
    state.processed_event_ids.add(event_id)
    return SettlementResult(False, state.cash, position, reservation, realized_delta)


def release_reservation(
    state: PortfolioState, order_id: UUID, *, symbol: str | None = None
) -> Reservation:
    reservation = state.reservations.get(order_id)
    if reservation is None:
        raise ValueError("reservation not found")
    if reservation.state != ReservationStatus.ACTIVE:
        return reservation
    if reservation.kind == ReservationKind.CASH:
        state.cash = replace(
            state.cash, reserved=state.cash.reserved - reservation.remaining_amount
        )
    else:
        if symbol is None or symbol not in state.positions:
            raise ValueError("symbol is required to release a share reservation")
        position = state.positions[symbol]
        state.positions[symbol] = replace(
            position,
            reserved_quantity=position.reserved_quantity - int(reservation.remaining_amount),
        )
    reservation = replace(
        reservation, remaining_amount=Decimal("0"), state=ReservationStatus.RELEASED
    )
    state.reservations[order_id] = reservation
    return reservation


def unrealized_pnl(position: Position, mark_price: Decimal) -> Decimal:
    if mark_price <= 0:
        raise ValueError("mark price must be positive")
    return (mark_price - position.average_cost) * position.quantity
