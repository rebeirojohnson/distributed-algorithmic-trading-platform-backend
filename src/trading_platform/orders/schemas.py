from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from trading_platform.common.enums import OrderSide, OrderStatus, OrderType, TimeInForce


class OrderCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    symbol: str = Field(min_length=1, max_length=10)
    side: OrderSide
    order_type: OrderType
    time_in_force: TimeInForce = TimeInForce.DAY
    quantity: int = Field(gt=0, le=1_000_000)
    limit_price: Decimal | None = Field(default=None, gt=0, decimal_places=4)
    stop_price: Decimal | None = Field(default=None, gt=0, decimal_places=4)

    @model_validator(mode="after")
    def validate_prices(self) -> OrderCreate:
        needs_limit = self.order_type in {OrderType.LIMIT, OrderType.STOP_LIMIT}
        needs_stop = self.order_type in {OrderType.STOP, OrderType.STOP_LIMIT}
        if needs_limit != (self.limit_price is not None):
            raise ValueError("limit_price must be supplied only for LIMIT and STOP_LIMIT orders")
        if needs_stop != (self.stop_price is not None):
            raise ValueError("stop_price must be supplied only for STOP and STOP_LIMIT orders")
        self.symbol = self.symbol.upper()
        return self


class OrderRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    account_id: UUID
    symbol: str
    side: OrderSide
    order_type: OrderType
    time_in_force: TimeInForce
    quantity: int
    filled_quantity: int
    status: OrderStatus
    average_fill_price: Decimal | None = None
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    triggered: bool
    version: int
    accepted_at: datetime | None = None
    expires_at: datetime | None = None


class OrderCommandAccepted(BaseModel):
    order_id: UUID
    status: OrderStatus


class OrderEventRead(BaseModel):
    event_id: UUID
    order_id: UUID
    version: int
    status: OrderStatus
    reason: str | None = None
    occurred_at: datetime


class ExecutionRead(BaseModel):
    id: UUID
    order_id: UUID
    account_id: UUID
    symbol: str
    side: OrderSide
    quantity: int
    price: Decimal
    executed_at: datetime
