import asyncio
import logging
from datetime import timedelta

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trading_platform.common.enums import OutboxStatus
from trading_platform.common.kafka import EventPublisher
from trading_platform.common.metrics import OUTBOX_FAILURES
from trading_platform.common.models import OutboxEvent
from trading_platform.common.retry import RetryPolicy
from trading_platform.common.time import utc_now

logger = logging.getLogger(__name__)


class OutboxWorker:
    def __init__(
        self,
        *,
        service_name: str,
        sessions: async_sessionmaker[AsyncSession],
        publisher: EventPublisher,
        batch_size: int = 100,
        poll_interval_seconds: float = 0.5,
        max_attempts: int = 10,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self._service_name = service_name
        self._sessions = sessions
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval = poll_interval_seconds
        self._max_attempts = max_attempts
        self._retry = retry_policy or RetryPolicy(max_attempts=max_attempts)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            published = await self.process_batch()
            if published == 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self._poll_interval)
                except TimeoutError:
                    pass

    async def process_batch(self) -> int:
        completed = 0
        async with self._sessions() as session, session.begin():
            events = list((await session.scalars(self._claim_statement())).all())
            for event in events:
                event.attempts += 1
                try:
                    await self._publisher.publish(
                        event.topic,
                        event.message_key,
                        event.payload,
                        event.headers,
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:
                    self._record_failure(event, exc)
                else:
                    event.status = OutboxStatus.PUBLISHED
                    event.published_at = utc_now()
                    event.last_error = None
                    completed += 1
        return completed

    def _claim_statement(self) -> Select[tuple[OutboxEvent]]:
        return (
            select(OutboxEvent)
            .where(
                OutboxEvent.status == OutboxStatus.PENDING,
                OutboxEvent.next_attempt_at <= utc_now(),
            )
            .order_by(OutboxEvent.created_at)
            .limit(self._batch_size)
            .with_for_update(skip_locked=True)
        )

    def _record_failure(self, event: OutboxEvent, exc: BaseException) -> None:
        event.last_error = f"{type(exc).__name__}: {exc}"[:4000]
        if event.attempts >= self._max_attempts:
            event.status = OutboxStatus.FAILED
        else:
            delay = self._retry.delay(event.attempts)
            event.next_attempt_at = utc_now() + timedelta(seconds=delay)
        OUTBOX_FAILURES.labels(service=self._service_name, topic=event.topic).inc()
        logger.exception(
            "outbox publish failed",
            extra={"event_id": str(event.event_id), "topic": event.topic},
            exc_info=exc,
        )
