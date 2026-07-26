from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import select

from trading_platform.common.contracts import (
    EXECUTIONS_TOPIC,
    ORDER_EVENTS_TOPIC,
    RISK_VALIDATION_REQUESTED_TOPIC,
)
from trading_platform.common.enums import OrderStatus, ReservationKind, ReservationStatus
from trading_platform.common.kafka import KafkaJsonConsumer, KafkaJsonProducer
from trading_platform.common.models import (
    CashBalance,
    PortfolioProcessedEvent,
    Position,
    Reservation,
)
from trading_platform.common.outbox import OutboxWorker

from .schemas import ExecutionEvent, ValidationRequest
from .service import settle_execution, validate_and_reserve

TERMINAL = {
    OrderStatus.CANCELLED.value,
    OrderStatus.REJECTED.value,
    OrderStatus.EXPIRED.value,
    OrderStatus.FAILED.value,
}


class PortfolioRuntime:
    def __init__(self, database: Any, settings: Any) -> None:
        self.database = database
        self.settings = settings
        self.stop = asyncio.Event()
        self.redis = Redis.from_url(settings.redis_url, decode_responses=False)
        self.producer = KafkaJsonProducer(settings)
        self.consumers = [
            KafkaJsonConsumer(
                settings=settings,
                topics=[RISK_VALIDATION_REQUESTED_TOPIC],
                group_id="portfolio-validation-v1",
                handler=self._validation,
                dlq_publisher=self.producer,
            ),
            KafkaJsonConsumer(
                settings=settings,
                topics=[EXECUTIONS_TOPIC],
                group_id="portfolio-executions-v1",
                handler=self._execution,
                dlq_publisher=self.producer,
            ),
            KafkaJsonConsumer(
                settings=settings,
                topics=[ORDER_EVENTS_TOPIC],
                group_id="portfolio-orders-v1",
                handler=self._order_event,
                dlq_publisher=self.producer,
            ),
        ]
        self.outbox = OutboxWorker(
            service_name="portfolio",
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

    async def close(self) -> None:
        self.stop.set()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for consumer in self.consumers:
            await consumer.stop()
        await self.producer.stop()
        await self.redis.aclose()

    async def _validation(self, payload: dict[str, Any], _record: Any) -> None:
        data = payload["data"]
        raw_quote = await self.redis.get(f"quotes:latest:{data['symbol']}")
        current_ask: Decimal | None = None
        if raw_quote:
            quote = json.loads(raw_quote)
            current_ask = Decimal(quote["ask"])
        request = ValidationRequest(
            event_id=payload["event_id"],
            order_id=data["order_id"],
            account_id=data["account_id"],
            symbol=data["symbol"],
            side=data["side"],
            order_type=data["order_type"],
            quantity=data["quantity"],
            current_ask=current_ask,
            limit_price=data.get("limit_price"),
            stop_price=data.get("stop_price"),
        )
        async with self.database.sessions() as session, session.begin():
            await validate_and_reserve(session, request)

    async def _execution(self, payload: dict[str, Any], _record: Any) -> None:
        data = payload["data"]
        event = ExecutionEvent(event_id=payload["event_id"], **data)
        async with self.database.sessions() as session, session.begin():
            await settle_execution(session, event)

    async def _order_event(self, payload: dict[str, Any], _record: Any) -> None:
        data = payload["data"]
        if data["status"] not in TERMINAL:
            return
        event_id = payload["event_id"]
        async with self.database.sessions() as session, session.begin():
            seen = await session.scalar(
                select(PortfolioProcessedEvent.id).where(
                    PortfolioProcessedEvent.consumer_name == "portfolio-terminal",
                    PortfolioProcessedEvent.event_id == event_id,
                )
            )
            if seen:
                return
            reservation = await session.scalar(
                select(Reservation)
                .where(Reservation.order_id == data["order_id"])
                .with_for_update()
            )
            if reservation is not None and reservation.status == ReservationStatus.ACTIVE:
                if reservation.kind == ReservationKind.CASH:
                    cash = await session.scalar(
                        select(CashBalance)
                        .where(
                            CashBalance.account_id == data["account_id"],
                            CashBalance.currency == "USD",
                        )
                        .with_for_update()
                    )
                    if cash is not None:
                        cash.reserved_amount -= reservation.remaining_amount
                        cash.available_amount += reservation.remaining_amount
                else:
                    position = await session.scalar(
                        select(Position)
                        .where(
                            Position.account_id == data["account_id"],
                            Position.symbol == data["symbol"],
                        )
                        .with_for_update()
                    )
                    if position is not None:
                        position.reserved_quantity -= int(reservation.remaining_amount)
                reservation.remaining_amount = Decimal("0")
                reservation.status = ReservationStatus.RELEASED
            session.add(
                PortfolioProcessedEvent(consumer_name="portfolio-terminal", event_id=event_id)
            )
