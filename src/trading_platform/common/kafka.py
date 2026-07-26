import asyncio
import base64
import json
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition

from trading_platform.common.retry import RetryPolicy, retry_async
from trading_platform.common.settings import Settings
from trading_platform.common.time import utc_now


class EventPublisher(Protocol):
    async def publish(
        self,
        topic: str,
        key: str,
        payload: Mapping[str, Any],
        headers: Mapping[str, str] | None = None,
    ) -> None: ...


class KafkaJsonProducer:
    def __init__(self, settings: Settings) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=settings.kafka_client_id,
            security_protocol=settings.kafka_security_protocol,
            enable_idempotence=True,
            acks="all",
            key_serializer=lambda value: value.encode("utf-8"),
            value_serializer=lambda value: json.dumps(
                value, separators=(",", ":"), ensure_ascii=True, default=str
            ).encode("utf-8"),
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(
        self,
        topic: str,
        key: str,
        payload: Mapping[str, Any],
        headers: Mapping[str, str] | None = None,
    ) -> None:
        encoded_headers = [(name, value.encode("utf-8")) for name, value in (headers or {}).items()]
        await self._producer.send_and_wait(
            topic,
            key=key,
            value=dict(payload),
            headers=encoded_headers,
        )


MessageHandler = Callable[[dict[str, Any], ConsumerRecord], Awaitable[None]]


class KafkaJsonConsumer:
    def __init__(
        self,
        *,
        settings: Settings,
        topics: list[str],
        group_id: str,
        handler: MessageHandler,
        dlq_publisher: EventPublisher,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=settings.kafka_client_id,
            group_id=group_id,
            security_protocol=settings.kafka_security_protocol,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self._handler = handler
        self._dlq = dlq_publisher
        self._retry = retry_policy or RetryPolicy()

    async def start(self) -> None:
        await self._consumer.start()

    async def stop(self) -> None:
        await self._consumer.stop()

    async def consume(self, stop: asyncio.Event) -> None:
        async for message in self._consumer:
            if stop.is_set():
                break
            await self._consume_one(message)

    async def _consume_one(self, message: ConsumerRecord) -> None:
        try:
            payload = json.loads(message.value)
            if not isinstance(payload, dict):
                raise ValueError("event payload must be a JSON object")

            async def handle() -> None:
                await self._handler(payload, message)

            await retry_async(handle, self._retry)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            await self._publish_dlq(message, exc)

        partition = TopicPartition(message.topic, message.partition)
        await self._consumer.commit({partition: message.offset + 1})

    async def _publish_dlq(self, message: ConsumerRecord, exc: BaseException) -> None:
        raw_value = message.value
        if isinstance(raw_value, str):
            raw_value = raw_value.encode("utf-8")
        payload = {
            "source_topic": message.topic,
            "source_partition": message.partition,
            "source_offset": message.offset,
            "source_timestamp": message.timestamp,
            "failed_at": utc_now().isoformat(),
            "error_type": type(exc).__name__,
            "error": str(exc)[:2000],
            "original_value_base64": base64.b64encode(raw_value).decode("ascii"),
        }
        key = message.key.decode("utf-8", errors="replace") if message.key else "no-key"
        await self._dlq.publish(f"{message.topic}.dlq", key, payload)
