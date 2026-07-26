import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from aiokafka.errors import KafkaError
from sqlalchemy.exc import DBAPIError, OperationalError

from trading_platform.common.errors import PlatformError


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay_seconds: float = 0.1
    max_delay_seconds: float = 10.0
    jitter_ratio: float = 0.2

    def delay(self, attempt: int, *, random_value: float | None = None) -> float:
        if attempt < 1:
            raise ValueError("attempt must be at least one")
        base = min(self.max_delay_seconds, self.base_delay_seconds * (2 ** (attempt - 1)))
        sample = random.random() if random_value is None else random_value
        return float(max(0.0, base * (1 + self.jitter_ratio * ((sample * 2) - 1))))


def is_transient(exc: BaseException) -> bool:
    if isinstance(exc, PlatformError):
        return exc.retryable
    if isinstance(exc, DBAPIError):
        return isinstance(exc, OperationalError) or exc.connection_invalidated
    return isinstance(exc, (KafkaError, TimeoutError, ConnectionError, OSError))


async def retry_async[T](
    operation: Callable[[], Awaitable[T]],
    policy: RetryPolicy,
    *,
    retry_if: Callable[[BaseException], bool] = is_transient,
) -> T:
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return await operation()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            if attempt == policy.max_attempts or not retry_if(exc):
                raise
            await asyncio.sleep(policy.delay(attempt))
    raise RuntimeError("unreachable")
