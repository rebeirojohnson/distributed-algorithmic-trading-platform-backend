from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from trading_platform.common.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from trading_platform.common.ids import new_id
from trading_platform.common.time import require_aware, utc_now

MARKET_QUOTES_TOPIC = "market.quotes.v1"
RISK_VALIDATION_REQUESTED_TOPIC = "risk.validation.requested.v1"
RISK_VALIDATION_COMPLETED_TOPIC = "risk.validation.completed.v1"
ORDER_EVENTS_TOPIC = "orders.events.v1"
EXECUTIONS_TOPIC = "executions.v1"
PORTFOLIO_EVENTS_TOPIC = "portfolio.events.v1"

PositiveDecimal = Annotated[Decimal, Field(gt=0, max_digits=20, decimal_places=4)]
NonNegativeDecimal = Annotated[Decimal, Field(ge=0, max_digits=20, decimal_places=4)]
PositiveQuantity = Annotated[int, Field(gt=0)]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, ser_json_timedelta="iso8601")

    @field_serializer("*", when_used="json", check_fields=False)
    def serialize_decimals(self, value: Any) -> Any:
        return str(value) if isinstance(value, Decimal) else value


class QuoteData(Contract):
    symbol: str = Field(min_length=1, max_length=16, pattern=r"^[A-Z.]+$")
    bid: PositiveDecimal
    ask: PositiveDecimal
    last: PositiveDecimal
    bid_size: PositiveQuantity
    ask_size: PositiveQuantity
    sequence: Annotated[int, Field(ge=0)]
    quoted_at: datetime

    @field_validator("quoted_at")
    @classmethod
    def quoted_at_is_aware(cls, value: datetime) -> datetime:
        return require_aware(value)


class RiskValidationRequestedData(Contract):
    account_id: UUID
    order_id: UUID
    symbol: str
    side: OrderSide
    order_type: OrderType
    time_in_force: TimeInForce
    quantity: PositiveQuantity
    limit_price: PositiveDecimal | None = None
    stop_price: PositiveDecimal | None = None


class RiskValidationCompletedData(Contract):
    account_id: UUID
    order_id: UUID
    approved: bool
    reservation_id: UUID | None = None
    reserved_amount: NonNegativeDecimal | None = None
    reason_code: str | None = None
    reason: str | None = None


class OrderEventData(Contract):
    account_id: UUID
    order_id: UUID
    symbol: str
    status: OrderStatus
    version: Annotated[int, Field(ge=1)]
    filled_quantity: Annotated[int, Field(ge=0)]
    average_fill_price: PositiveDecimal | None = None
    reason_code: str | None = None


class ExecutionData(Contract):
    execution_id: UUID
    account_id: UUID
    order_id: UUID
    symbol: str
    side: OrderSide
    quantity: PositiveQuantity
    price: PositiveDecimal
    executed_at: datetime

    @field_validator("executed_at")
    @classmethod
    def executed_at_is_aware(cls, value: datetime) -> datetime:
        return require_aware(value)


class PortfolioEventData(Contract):
    account_id: UUID
    reason: Literal["RESERVED", "EXECUTED", "RELEASED"]
    as_of: datetime
    cash_total: NonNegativeDecimal
    cash_available: NonNegativeDecimal
    cash_reserved: NonNegativeDecimal
    symbol: str | None = None
    position_quantity: Annotated[int, Field(ge=0)] | None = None
    position_average_cost: NonNegativeDecimal | None = None
    realized_pnl: Decimal | None = None

    @field_validator("as_of")
    @classmethod
    def as_of_is_aware(cls, value: datetime) -> datetime:
        return require_aware(value)


EventData = (
    QuoteData
    | RiskValidationRequestedData
    | RiskValidationCompletedData
    | OrderEventData
    | ExecutionData
    | PortfolioEventData
)


class EventEnvelope[T: Contract](Contract):
    event_id: UUID = Field(default_factory=new_id)
    event_type: str = Field(min_length=1, max_length=128)
    schema_version: Literal[1] = 1
    occurred_at: datetime = Field(default_factory=utc_now)
    producer: str = Field(min_length=1, max_length=128)
    correlation_id: UUID = Field(default_factory=new_id)
    causation_id: UUID | None = None
    traceparent: str | None = Field(default=None, max_length=512)
    account_id: UUID | None = None
    order_id: UUID | None = None
    data: T

    @field_validator("occurred_at")
    @classmethod
    def occurred_at_is_aware(cls, value: datetime) -> datetime:
        return require_aware(value)


AnyEventEnvelope = EventEnvelope[EventData]


class ErrorResponse(Contract):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)
    request_id: UUID
