from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from trading_platform.common.contracts import (
    RISK_VALIDATION_REQUESTED_TOPIC,
    EventEnvelope,
    OrderEventData,
    RiskValidationRequestedData,
)
from trading_platform.common.enums import OrderStatus
from trading_platform.common.errors import ConflictError, IdempotencyConflictError, NotFoundError
from trading_platform.common.idempotency import request_fingerprint
from trading_platform.common.ids import new_id
from trading_platform.common.models import (
    IdempotencyRecord,
    Instrument,
    Order,
    OrderEvent,
    OutboxEvent,
)

from .domain import ALLOWED_TRANSITIONS, InvalidTransition
from .schemas import OrderCreate


def serialize_order(order: Order) -> dict[str, Any]:
    return {
        "id": order.id,
        "account_id": order.account_id,
        "symbol": order.symbol,
        "side": order.side,
        "order_type": order.order_type,
        "time_in_force": order.time_in_force,
        "quantity": order.quantity,
        "filled_quantity": order.filled_quantity,
        "status": order.status,
        "average_fill_price": order.average_fill_price,
        "limit_price": order.limit_price,
        "stop_price": order.stop_price,
        "triggered": order.is_triggered,
        "version": order.version,
        "accepted_at": order.accepted_at,
        "expires_at": order.expires_at,
    }


async def _idempotency_replay(
    session: AsyncSession,
    *,
    account_id: UUID,
    operation: str,
    key: str,
    request_hash: str,
) -> tuple[int, dict[str, Any]] | None:
    record = await session.scalar(
        select(IdempotencyRecord).where(
            IdempotencyRecord.account_id == account_id,
            IdempotencyRecord.operation == operation,
            IdempotencyRecord.idempotency_key == key,
        )
    )
    if record is None:
        return None
    if record.request_hash != request_hash:
        raise IdempotencyConflictError("idempotency key was already used for a different request")
    if record.response_status is None or record.response_body is None:
        raise ConflictError("an identical request is currently being processed")
    return record.response_status, record.response_body


async def create_order(
    session: AsyncSession,
    *,
    account_id: UUID,
    command: OrderCreate,
    idempotency_key: str,
) -> tuple[int, dict[str, Any]]:
    path = "/v1/orders"
    request_body = command.model_dump(mode="json")
    request_hash = request_fingerprint(operation="CREATE_ORDER", path=path, body=request_body)
    replay = await _idempotency_replay(
        session,
        account_id=account_id,
        operation="CREATE_ORDER",
        key=idempotency_key,
        request_hash=request_hash,
    )
    if replay:
        return replay

    instrument = await session.scalar(
        select(Instrument).where(
            Instrument.symbol == command.symbol, Instrument.is_active.is_(True)
        )
    )
    if instrument is None:
        raise NotFoundError("instrument not found", details={"symbol": command.symbol})

    order = Order(
        id=new_id(),
        account_id=account_id,
        symbol=command.symbol,
        side=command.side,
        order_type=command.order_type,
        time_in_force=command.time_in_force,
        quantity=command.quantity,
        limit_price=command.limit_price,
        stop_price=command.stop_price,
        status=OrderStatus.PENDING_VALIDATION,
        version=1,
    )
    response = {"order_id": str(order.id), "status": order.status.value}
    idempotency = IdempotencyRecord(
        account_id=account_id,
        operation="CREATE_ORDER",
        path=path,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        response_status=202,
        response_body=response,
        expires_at=datetime.now(UTC) + timedelta(hours=24),
    )
    event_id = new_id()
    audit = OrderEvent(
        event_id=event_id,
        order_id=order.id,
        account_id=account_id,
        order_version=1,
        status=order.status,
        details={},
    )
    envelope = EventEnvelope[RiskValidationRequestedData](
        event_id=new_id(),
        event_type="risk.validation.requested",
        producer="orders",
        account_id=account_id,
        order_id=order.id,
        data=RiskValidationRequestedData(
            account_id=account_id,
            order_id=order.id,
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            time_in_force=order.time_in_force,
            quantity=order.quantity,
            limit_price=order.limit_price,
            stop_price=order.stop_price,
        ),
    )
    outbox = OutboxEvent(
        event_id=envelope.event_id,
        event_type=envelope.event_type,
        aggregate_type="order",
        aggregate_id=order.id,
        topic=RISK_VALIDATION_REQUESTED_TOPIC,
        message_key=str(order.id),
        payload=envelope.model_dump(mode="json"),
    )
    session.add_all([order, audit, idempotency, outbox])
    await session.flush()
    return 202, response


async def cancel_order(
    session: AsyncSession,
    *,
    account_id: UUID,
    order_id: UUID,
    idempotency_key: str,
) -> tuple[int, dict[str, Any]]:
    path = f"/v1/orders/{order_id}/cancel"
    request_hash = request_fingerprint(operation="CANCEL_ORDER", path=path, body={})
    replay = await _idempotency_replay(
        session,
        account_id=account_id,
        operation="CANCEL_ORDER",
        key=idempotency_key,
        request_hash=request_hash,
    )
    if replay:
        return replay
    order = await session.scalar(
        select(Order).where(Order.id == order_id, Order.account_id == account_id).with_for_update()
    )
    if order is None:
        raise NotFoundError("order not found")
    if order.status == OrderStatus.CANCELLED:
        response = {"order_id": str(order.id), "status": order.status.value}
    else:
        if OrderStatus.CANCEL_PENDING not in ALLOWED_TRANSITIONS[order.status]:
            raise InvalidTransition(f"cannot cancel order in {order.status}")
        order.status = OrderStatus.CANCEL_PENDING
        order.version += 1
        response = {"order_id": str(order.id), "status": order.status.value}
        event_id = new_id()
        audit = OrderEvent(
            event_id=event_id,
            order_id=order.id,
            account_id=account_id,
            order_version=order.version,
            status=order.status,
            details={},
        )
        event = EventEnvelope[OrderEventData](
            event_id=event_id,
            event_type="order.cancel.requested",
            producer="orders",
            account_id=account_id,
            order_id=order.id,
            data=OrderEventData(
                account_id=account_id,
                order_id=order.id,
                symbol=order.symbol,
                status=order.status,
                version=order.version,
                filled_quantity=order.filled_quantity,
                average_fill_price=order.average_fill_price,
            ),
        )
        session.add(
            OutboxEvent(
                event_id=event_id,
                event_type=event.event_type,
                aggregate_type="order",
                aggregate_id=order.id,
                topic="orders.events.v1",
                message_key=str(order.id),
                payload=event.model_dump(mode="json"),
            )
        )
        session.add(audit)
        order.status = OrderStatus.CANCELLED
        order.version += 1
        cancelled_id = new_id()
        cancelled = OrderEvent(
            event_id=cancelled_id,
            order_id=order.id,
            account_id=account_id,
            order_version=order.version,
            status=order.status,
            details={},
        )
        cancelled_event = EventEnvelope[OrderEventData](
            event_id=cancelled_id,
            event_type="order.status.changed",
            producer="orders",
            account_id=account_id,
            order_id=order.id,
            data=OrderEventData(
                account_id=account_id,
                order_id=order.id,
                symbol=order.symbol,
                status=order.status,
                version=order.version,
                filled_quantity=order.filled_quantity,
                average_fill_price=order.average_fill_price,
            ),
        )
        session.add(cancelled)
        session.add(
            OutboxEvent(
                event_id=cancelled_id,
                event_type=cancelled_event.event_type,
                aggregate_type="order",
                aggregate_id=order.id,
                topic="orders.events.v1",
                message_key=str(order.id),
                payload=cancelled_event.model_dump(mode="json"),
            )
        )
        response = {"order_id": str(order.id), "status": order.status.value}
    session.add(
        IdempotencyRecord(
            account_id=account_id,
            operation="CANCEL_ORDER",
            path=path,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            response_status=202,
            response_body=response,
            expires_at=datetime.now(UTC) + timedelta(hours=24),
        )
    )
    await session.flush()
    return 202, response
