import hashlib
import json
from collections.abc import Mapping
from typing import Any


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("utf-8")


def request_fingerprint(*, operation: str, path: str, body: Mapping[str, Any]) -> str:
    payload = {"body": body, "operation": operation, "path": path}
    return hashlib.sha256(canonical_json(payload)).hexdigest()


def redact_idempotency_key(value: str) -> str:
    if len(value) <= 8:
        return "[REDACTED]"
    return f"{value[:4]}...{value[-4:]}"
