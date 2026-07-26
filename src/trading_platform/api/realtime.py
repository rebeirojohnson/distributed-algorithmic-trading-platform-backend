from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from fastapi import WebSocket, WebSocketDisconnect
from sqlalchemy import select

from trading_platform.common.models import Account
from trading_platform.common.security import decode_access_token
from trading_platform.market_data.domain import INSTRUMENTS

ACCOUNT_CHANNELS = frozenset({"orders", "executions", "portfolio"})
SYMBOLS = frozenset(item.symbol for item in INSTRUMENTS)


@dataclass(eq=False, slots=True)
class Connection:
    account_id: UUID
    subscriptions: set[str] = field(default_factory=set)
    queue: asyncio.Queue[dict[str, Any]] = field(default_factory=lambda: asyncio.Queue(maxsize=100))


class RealtimeHub:
    def __init__(self) -> None:
        self.connections: set[Connection] = set()

    def add(self, account_id: UUID) -> Connection:
        connection = Connection(account_id)
        self.connections.add(connection)
        return connection

    def remove(self, connection: Connection) -> None:
        self.connections.discard(connection)

    async def publish(self, payload: dict[str, Any], _record: Any) -> None:
        data = payload.get("data", {})
        event_type = str(payload.get("event_type", ""))
        if event_type.startswith("market.quote"):
            channel = f"quotes:{data.get('symbol')}"
        elif event_type.startswith("execution"):
            channel = "executions"
        elif event_type.startswith("portfolio"):
            channel = "portfolio"
        else:
            channel = "orders"
        account_id = payload.get("account_id")
        message = {
            "event_id": payload.get("event_id"),
            "type": event_type,
            "schema_version": payload.get("schema_version", 1),
            "timestamp": payload.get("occurred_at"),
            "channel": channel,
            "data": data,
        }
        for connection in tuple(self.connections):
            if channel not in connection.subscriptions:
                continue
            if account_id is not None and str(connection.account_id) != str(account_id):
                continue
            try:
                connection.queue.put_nowait(message)
            except asyncio.QueueFull:
                self.remove(connection)


async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        message = await asyncio.wait_for(websocket.receive_json(), timeout=10)
        if message.get("type") != "authenticate" or not isinstance(message.get("token"), str):
            await websocket.close(code=4401, reason="authentication required")
            return
        claims = decode_access_token(message["token"], websocket.app.state.settings)
        async with websocket.app.state.database.session() as session:
            account_id = await session.scalar(
                select(Account.id).where(Account.user_id == claims.sub)
            )
        if account_id is None:
            await websocket.close(code=4403, reason="account not found")
            return
        hub: RealtimeHub = websocket.app.state.realtime_hub
        connection = hub.add(account_id)
        await websocket.send_json(
            {"type": "authenticated", "account_id": str(account_id), "schema_version": 1}
        )
        sender = asyncio.create_task(_send_messages(websocket, connection))
        while True:
            message = await asyncio.wait_for(websocket.receive_json(), timeout=45)
            message_type = message.get("type")
            channel = message.get("channel")
            if message_type == "ping":
                await websocket.send_json({"type": "pong"})
                continue
            if not isinstance(channel, str) or not _allowed_channel(channel):
                await websocket.send_json({"type": "error", "code": "INVALID_CHANNEL"})
                continue
            if message_type == "subscribe":
                if len(connection.subscriptions) >= 20 and channel not in connection.subscriptions:
                    await websocket.send_json({"type": "error", "code": "SUBSCRIPTION_LIMIT"})
                    continue
                connection.subscriptions.add(channel)
                await websocket.send_json({"type": "subscribed", "channel": channel})
            elif message_type == "unsubscribe":
                connection.subscriptions.discard(channel)
                await websocket.send_json({"type": "unsubscribed", "channel": channel})
            else:
                await websocket.send_json({"type": "error", "code": "INVALID_MESSAGE"})
    except TimeoutError:
        await websocket.close(code=4408, reason="heartbeat timeout")
    except WebSocketDisconnect:
        return
    except Exception:
        await websocket.close(code=4401, reason="authentication failed")
    finally:
        if "connection" in locals():
            websocket.app.state.realtime_hub.remove(connection)
        if "sender" in locals():
            sender.cancel()


async def _send_messages(websocket: WebSocket, connection: Connection) -> None:
    while True:
        message = await connection.queue.get()
        await websocket.send_json(message)


def _allowed_channel(channel: str) -> bool:
    if channel in ACCOUNT_CHANNELS:
        return True
    prefix, separator, symbol = channel.partition(":")
    return bool(
        separator and prefix == "quotes" and symbol.upper() in SYMBOLS and symbol == symbol.upper()
    )
