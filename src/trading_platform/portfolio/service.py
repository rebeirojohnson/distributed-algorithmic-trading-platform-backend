from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from trading_platform.common.contracts import (
    PORTFOLIO_EVENTS_TOPIC,
    RISK_VALIDATION_COMPLETED_TOPIC,
    EventEnvelope,
    PortfolioEventData,
    RiskValidationCompletedData,
)
from trading_platform.common.enums import OrderSide, OrderType, ReservationKind, ReservationStatus
from trading_platform.common.models import (
    CashBalance,
    Order,
    OutboxEvent,
    PortfolioProcessedEvent,
    Position,
    Reservation,
)
from trading_platform.orders.reservations import reservation_required

from .schemas import ExecutionEvent, ValidationRequest, ValidationResult


async def validate_and_reserve(
    session: AsyncSession, request: ValidationRequest
) -> ValidationResult:
    existing = await session.scalar(
        select(Reservation).where(Reservation.order_id == request.order_id)
    )
    if existing:
        return ValidationResult(
            order_id=request.order_id,
            account_id=request.account_id,
            accepted=existing.status == ReservationStatus.ACTIVE,
            reservation_kind=existing.kind.value,
            reserved_amount=existing.original_amount,
        )
    side = OrderSide(request.side)
    order_type = OrderType(request.order_type)
    required = reservation_required(
        side=side,
        order_type=order_type,
        quantity=request.quantity,
        current_ask=request.current_ask,
        limit_price=request.limit_price,
        stop_price=request.stop_price,
    )
    approved = False
    reason: str | None = None
    reservation: Reservation | None = None
    if side == OrderSide.BUY:
        cash = await session.scalar(
            select(CashBalance)
            .where(CashBalance.account_id == request.account_id, CashBalance.currency == "USD")
            .with_for_update()
        )
        amount = Decimal(required)
        if cash is None or cash.available_amount < amount:
            reason = "INSUFFICIENT_CASH"
        else:
            cash.available_amount -= amount
            cash.reserved_amount += amount
            reservation = Reservation(
                account_id=request.account_id,
                order_id=request.order_id,
                kind=ReservationKind.CASH,
                original_amount=amount,
                remaining_amount=amount,
            )
            approved = True
    else:
        position = await session.scalar(
            select(Position)
            .where(Position.account_id == request.account_id, Position.symbol == request.symbol)
            .with_for_update()
        )
        quantity = int(required)
        if position is None or position.quantity - position.reserved_quantity < quantity:
            reason = "INSUFFICIENT_SHARES"
        else:
            position.reserved_quantity += quantity
            reservation = Reservation(
                account_id=request.account_id,
                order_id=request.order_id,
                symbol=request.symbol,
                kind=ReservationKind.SHARES,
                original_amount=Decimal(quantity),
                remaining_amount=Decimal(quantity),
            )
            approved = True
    if reservation:
        session.add(reservation)
        await session.flush()
    result = ValidationResult(
        order_id=request.order_id,
        account_id=request.account_id,
        accepted=approved,
        reservation_kind=reservation.kind.value if reservation else None,
        reserved_amount=reservation.original_amount if reservation else None,
        reason=reason,
    )
    envelope = EventEnvelope[RiskValidationCompletedData](
        event_type="risk.validation.completed",
        producer="portfolio",
        correlation_id=request.event_id,
        causation_id=request.event_id,
        account_id=request.account_id,
        order_id=request.order_id,
        data=RiskValidationCompletedData(
            account_id=request.account_id,
            order_id=request.order_id,
            approved=approved,
            reservation_id=reservation.id if reservation else None,
            reserved_amount=reservation.original_amount if reservation else None,
            reason_code=reason,
            reason=reason,
        ),
    )
    session.add(
        OutboxEvent(
            event_id=envelope.event_id,
            event_type=envelope.event_type,
            aggregate_type="order",
            aggregate_id=request.order_id,
            topic=RISK_VALIDATION_COMPLETED_TOPIC,
            message_key=str(request.order_id),
            payload=envelope.model_dump(mode="json"),
        )
    )
    await session.flush()
    return result


async def settle_execution(session: AsyncSession, event: ExecutionEvent) -> bool:
    duplicate = await session.scalar(
        select(PortfolioProcessedEvent.id).where(
            PortfolioProcessedEvent.consumer_name == "portfolio-executions",
            PortfolioProcessedEvent.event_id == event.event_id,
        )
    )
    if duplicate:
        return False
    reservation = await session.scalar(
        select(Reservation).where(Reservation.order_id == event.order_id).with_for_update()
    )
    cash = await session.scalar(
        select(CashBalance)
        .where(CashBalance.account_id == event.account_id, CashBalance.currency == "USD")
        .with_for_update()
    )
    if reservation is None or cash is None or reservation.status != ReservationStatus.ACTIVE:
        raise ValueError("active reservation and cash balance are required")
    position = await session.scalar(
        select(Position)
        .where(Position.account_id == event.account_id, Position.symbol == event.symbol)
        .with_for_update()
    )
    if event.side == OrderSide.BUY:
        cost = event.price * event.quantity
        if reservation.kind != ReservationKind.CASH or cost > reservation.remaining_amount:
            raise ValueError("execution exceeds cash reservation")
        old_quantity = position.quantity if position else 0
        old_cost = (position.average_cost * old_quantity) if position else Decimal("0")
        if position is None:
            position = Position(account_id=event.account_id, symbol=event.symbol)
            session.add(position)
        position.quantity += event.quantity
        position.average_cost = (old_cost + cost) / position.quantity
        cash.total_amount -= cost
        cash.reserved_amount -= cost
        reservation.remaining_amount -= cost
    else:
        amount = Decimal(event.quantity)
        if (
            reservation.kind != ReservationKind.SHARES
            or position is None
            or amount > reservation.remaining_amount
        ):
            raise ValueError("execution exceeds share reservation")
        position.quantity -= event.quantity
        position.reserved_quantity -= event.quantity
        position.realized_pnl += (event.price - position.average_cost) * event.quantity
        if position.quantity == 0:
            position.average_cost = Decimal("0")
        cash.total_amount += event.price * event.quantity
        cash.available_amount += event.price * event.quantity
        reservation.remaining_amount -= amount
    if reservation.remaining_amount == 0:
        reservation.status = ReservationStatus.CONSUMED
    order = await session.scalar(select(Order).where(Order.id == event.order_id))
    if order is not None and order.status.value == "FILLED" and reservation.remaining_amount > 0:
        if reservation.kind == ReservationKind.CASH:
            cash.reserved_amount -= reservation.remaining_amount
            cash.available_amount += reservation.remaining_amount
        elif position is not None:
            position.reserved_quantity -= int(reservation.remaining_amount)
        reservation.remaining_amount = Decimal("0")
        reservation.status = ReservationStatus.CONSUMED
    session.add(
        PortfolioProcessedEvent(consumer_name="portfolio-executions", event_id=event.event_id)
    )
    envelope = EventEnvelope[PortfolioEventData](
        event_type="portfolio.executed",
        producer="portfolio",
        causation_id=event.event_id,
        account_id=event.account_id,
        order_id=event.order_id,
        data=PortfolioEventData(
            account_id=event.account_id,
            reason="EXECUTED",
            as_of=datetime.now(UTC),
            cash_total=cash.total_amount,
            cash_available=cash.available_amount,
            cash_reserved=cash.reserved_amount,
            symbol=event.symbol,
            position_quantity=position.quantity,
            position_average_cost=position.average_cost,
            realized_pnl=position.realized_pnl,
        ),
    )
    session.add(
        OutboxEvent(
            event_id=envelope.event_id,
            event_type=envelope.event_type,
            aggregate_type="account",
            aggregate_id=event.account_id,
            topic=PORTFOLIO_EVENTS_TOPIC,
            message_key=str(event.account_id),
            payload=envelope.model_dump(mode="json"),
        )
    )
    await session.flush()
    return True
