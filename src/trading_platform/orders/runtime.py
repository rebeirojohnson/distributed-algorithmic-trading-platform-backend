from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select

from trading_platform.common.contracts import (
    EXECUTIONS_TOPIC,
    MARKET_QUOTES_TOPIC,
    ORDER_EVENTS_TOPIC,
    RISK_VALIDATION_COMPLETED_TOPIC,
    EventEnvelope,
    ExecutionData,
    OrderEventData,
)
from trading_platform.common.enums import OrderStatus
from trading_platform.common.ids import new_id
from trading_platform.common.kafka import KafkaJsonConsumer, KafkaJsonProducer
from trading_platform.common.models import (
    Execution,
    Order,
    OrderEvent,
    OrderProcessedEvent,
    OutboxEvent,
    Reservation,
)
from trading_platform.common.outbox import OutboxWorker

from .domain import Order as DomainOrder
from .domain import apply_fill, deterministic_execution_id, match_quote


class OrderRuntime:
    def __init__(self, database: Any, settings: Any) -> None:
        self.database = database
        self.settings = settings
        self.stop = asyncio.Event()
        self.producer = KafkaJsonProducer(settings)
        self.consumers = [
            KafkaJsonConsumer(
                settings=settings,
                topics=[RISK_VALIDATION_COMPLETED_TOPIC],
                group_id="orders-validation-v1",
                handler=self._validation,
                dlq_publisher=self.producer,
            ),
            KafkaJsonConsumer(
                settings=settings,
                topics=[MARKET_QUOTES_TOPIC],
                group_id="orders-quotes-v1",
                handler=self._quote,
                dlq_publisher=self.producer,
            ),
        ]
        self.outbox = OutboxWorker(
            service_name="orders",
            sessions=database.sessions,
            publisher=self.producer,
            batch_size=settings.outbox_batch_size,
            poll_interval_seconds=settings.outbox_poll_interval_seconds,
            max_attempts=settings.outbox_max_attempts,
        )
        self.tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        await self.producer.start()
        for consumer in self.consumers:
            await consumer.start()
            self.tasks.append(asyncio.create_task(consumer.consume(self.stop)))
        self.tasks.append(asyncio.create_task(self.outbox.run(self.stop)))
        self.tasks.append(asyncio.create_task(self._expiry_loop()))

    async def close(self) -> None:
        self.stop.set()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for consumer in self.consumers:
            await consumer.stop()
        await self.producer.stop()

    async def _validation(self, payload: dict[str, Any], _record: Any) -> None:
        event_id = payload["event_id"]
        data = payload["data"]
        async with self.database.sessions() as session, session.begin():
            order = await session.scalar(
                select(Order).where(Order.id == data["order_id"]).with_for_update()
            )
            if order is None or order.status != OrderStatus.PENDING_VALIDATION:
                return
            marker = await session.scalar(
                select(OrderProcessedEvent.id).where(
                    OrderProcessedEvent.consumer_name == "orders-validation",
                    OrderProcessedEvent.event_id == event_id,
                )
            )
            if marker:
                return
            session.add(OrderProcessedEvent(consumer_name="orders-validation", event_id=event_id))
            if not data["approved"]:
                self._set_status(session, order, OrderStatus.REJECTED, data.get("reason_code"))
                return
            now = datetime.now(UTC)
            order.accepted_at = now
            if order.time_in_force.value == "DAY":
                from datetime import timedelta

                order.expires_at = now + timedelta(hours=24)
            self._set_status(session, order, OrderStatus.ACCEPTED)
            self._set_status(session, order, OrderStatus.ACTIVE)

    async def _quote(self, payload: dict[str, Any], _record: Any) -> None:
        quote_id = payload["event_id"]
        quote = payload["data"]
        async with self.database.sessions() as session, session.begin():
            orders = list(
                (
                    await session.scalars(
                        select(Order)
                        .where(
                            Order.symbol == quote["symbol"],
                            Order.status.in_([OrderStatus.ACTIVE, OrderStatus.PARTIALLY_FILLED]),
                        )
                        .order_by(Order.created_at, Order.id)
                        .with_for_update(skip_locked=True)
                    )
                ).all()
            )
            remaining_bid = int(quote["bid_size"])
            remaining_ask = int(quote["ask_size"])
            for order in orders:
                consumer_name = f"orders-quote:{order.id}"
                seen = await session.scalar(
                    select(OrderProcessedEvent.id).where(
                        OrderProcessedEvent.consumer_name == consumer_name,
                        OrderProcessedEvent.event_id == quote_id,
                    )
                )
                if seen:
                    continue
                reservation = await session.scalar(
                    select(Reservation).where(Reservation.order_id == order.id)
                )
                domain = _domain(order)
                result = match_quote(
                    domain,
                    bid=Decimal(quote["bid"]),
                    ask=Decimal(quote["ask"]),
                    bid_size=remaining_bid,
                    ask_size=remaining_ask,
                    reservation_remaining=(
                        reservation.remaining_amount if reservation is not None else 0
                    ),
                )
                session.add(OrderProcessedEvent(consumer_name=consumer_name, event_id=quote_id))
                if result.order.triggered != order.is_triggered:
                    order.is_triggered = True
                    order.version += 1
                if not result.matched or result.price is None:
                    if result.order.status != order.status:
                        self._set_status(session, order, result.order.status, result.reason)
                    continue
                filled = apply_fill(_domain(order), result.quantity, result.price)
                order.filled_quantity = filled.filled_quantity
                order.average_fill_price = filled.average_fill_price
                self._set_status(session, order, filled.status)
                execution_id = deterministic_execution_id(order.id, quote_id)
                execution = Execution(
                    execution_id=execution_id,
                    order_id=order.id,
                    account_id=order.account_id,
                    symbol=order.symbol,
                    side=order.side,
                    quantity=result.quantity,
                    price=result.price,
                    executed_at=datetime.now(UTC),
                )
                session.add(execution)
                envelope = EventEnvelope[ExecutionData](
                    event_id=execution_id,
                    event_type="execution.created",
                    producer="orders",
                    causation_id=quote_id,
                    account_id=order.account_id,
                    order_id=order.id,
                    data=ExecutionData(
                        execution_id=execution_id,
                        account_id=order.account_id,
                        order_id=order.id,
                        symbol=order.symbol,
                        side=order.side,
                        quantity=result.quantity,
                        price=result.price,
                        executed_at=execution.executed_at,
                    ),
                )
                session.add(_outbox(envelope, EXECUTIONS_TOPIC, order.id))
                if order.side.value == "BUY":
                    remaining_ask -= result.quantity
                else:
                    remaining_bid -= result.quantity

    def _set_status(
        self, session: Any, order: Order, status: OrderStatus, reason: str | None = None
    ) -> None:
        order.status = status
        order.version += 1
        event_id = new_id()
        session.add(
            OrderEvent(
                event_id=event_id,
                order_id=order.id,
                account_id=order.account_id,
                order_version=order.version,
                status=status,
                reason_code=reason,
                details={},
            )
        )
        envelope = EventEnvelope[OrderEventData](
            event_id=event_id,
            event_type="order.status.changed",
            producer="orders",
            account_id=order.account_id,
            order_id=order.id,
            data=OrderEventData(
                account_id=order.account_id,
                order_id=order.id,
                symbol=order.symbol,
                status=status,
                version=order.version,
                filled_quantity=order.filled_quantity,
                average_fill_price=order.average_fill_price,
                reason_code=reason,
            ),
        )
        session.add(_outbox(envelope, ORDER_EVENTS_TOPIC, order.id))

    async def _expiry_loop(self) -> None:
        while not self.stop.is_set():
            async with self.database.sessions() as session, session.begin():
                now = datetime.now(UTC)
                orders = (
                    await session.scalars(
                        select(Order)
                        .where(
                            Order.status.in_([OrderStatus.ACTIVE, OrderStatus.PARTIALLY_FILLED]),
                            Order.expires_at.is_not(None),
                            Order.expires_at <= now,
                        )
                        .with_for_update(skip_locked=True)
                    )
                ).all()
                for order in orders:
                    self._set_status(session, order, OrderStatus.EXPIRED, "DAY_EXPIRED")
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=5)
            except TimeoutError:
                continue


def _domain(order: Order) -> DomainOrder:
    return DomainOrder(
        id=order.id,
        account_id=order.account_id,
        symbol=order.symbol,
        side=order.side,
        order_type=order.order_type,
        time_in_force=order.time_in_force,
        quantity=order.quantity,
        status=order.status,
        filled_quantity=order.filled_quantity,
        average_fill_price=order.average_fill_price,
        limit_price=order.limit_price,
        stop_price=order.stop_price,
        triggered=order.is_triggered,
        version=order.version,
        accepted_at=order.accepted_at,
        expires_at=order.expires_at,
    )


def _outbox(envelope: Any, topic: str, order_id: Any) -> OutboxEvent:
    return OutboxEvent(
        event_id=envelope.event_id,
        event_type=envelope.event_type,
        aggregate_type="order",
        aggregate_id=order_id,
        topic=topic,
        message_key=str(order_id),
        payload=envelope.model_dump(mode="json"),
    )
