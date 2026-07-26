import os
import socket
from urllib.parse import urlparse

import pytest


def _endpoint(variable: str, default_port: int) -> tuple[str, int]:
    value = os.getenv(variable)
    if not value:
        pytest.skip(f"{variable} is not set; integration infrastructure is unavailable")

    if variable == "TRADING_KAFKA_BOOTSTRAP_SERVERS":
        host, separator, port = value.split(",", maxsplit=1)[0].partition(":")
        return host, int(port) if separator else default_port

    parsed = urlparse(value)
    if not parsed.hostname:
        pytest.skip(f"{variable} does not contain a reachable infrastructure endpoint")
    return parsed.hostname, parsed.port or default_port


@pytest.mark.parametrize(
    ("variable", "default_port"),
    [
        ("TRADING_DATABASE_URL", 5432),
        ("TRADING_REDIS_URL", 6379),
        ("TRADING_KAFKA_BOOTSTRAP_SERVERS", 9092),
    ],
)
def test_infrastructure_accepts_connections(variable: str, default_port: int) -> None:
    host, port = _endpoint(variable, default_port)
    with socket.create_connection((host, port), timeout=5):
        pass
