from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from trading_platform.common.enums import OrderSide


class ValidationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    order_id: UUID
    account_id: UUID
    symbol: str
    side: OrderSide
    order_type: str
    quantity: int = Field(gt=0)
    current_ask: Decimal | None = Field(default=None, gt=0)
    limit_price: Decimal | None = Field(default=None, gt=0)
    stop_price: Decimal | None = Field(default=None, gt=0)


class ValidationResult(BaseModel):
    order_id: UUID
    account_id: UUID
    accepted: bool
    reservation_kind: str | None = None
    reserved_amount: Decimal | None = None
    reason: str | None = None


class ExecutionEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    execution_id: UUID
    order_id: UUID
    account_id: UUID
    symbol: str
    side: OrderSide
    quantity: int = Field(gt=0)
    price: Decimal = Field(gt=0)
    executed_at: datetime


class PositionRead(BaseModel):
    symbol: str
    quantity: int
    available_quantity: int
    average_cost: Decimal
    market_price: Decimal | None = None
    market_value: Decimal | None = None
    realized_pnl: Decimal
    unrealized_pnl: Decimal | None = None
    quote_stale: bool = False


class PortfolioRead(BaseModel):
    account_id: UUID
    cash_total: Decimal
    cash_reserved: Decimal
    cash_available: Decimal
    positions: list[PositionRead]
    as_of: datetime
    quote_stale: bool
