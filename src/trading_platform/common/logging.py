import contextvars
import logging
from collections.abc import Mapping
from typing import Any

from pythonjsonlogger.json import JsonFormatter

_context: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "log_context", default=None
)
_SENSITIVE_KEYS = frozenset(
    {"password", "password_hash", "token", "jwt", "authorization", "idempotency_key"}
)


def bind_log_context(**values: Any) -> contextvars.Token[dict[str, Any] | None]:
    return _context.set({**(_context.get() or {}), **values})


def reset_log_context(token: contextvars.Token[dict[str, Any] | None]) -> None:
    _context.reset(token)


def clear_log_context() -> None:
    _context.set({})


def redact(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): "[REDACTED]" if str(key).casefold() in _SENSITIVE_KEYS else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact(item) for item in value)
    return value


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in redact(_context.get() or {}).items():
            setattr(record, key, value)
        if hasattr(record, "data"):
            record.data = redact(record.data)
        return True


def configure_logging(*, service: str, environment: str, level: str = "INFO") -> None:
    handler = logging.StreamHandler()
    handler.addFilter(ContextFilter())
    handler.setFormatter(
        JsonFormatter(
            "%(levelname)s %(name)s %(message)s %(service)s %(environment)s",
            rename_fields={"levelname": "level"},
            static_fields={"service": service, "environment": environment},
            timestamp=True,
        )
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
