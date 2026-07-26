from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from trading_platform.common.database import Base
from trading_platform.common.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    OutboxStatus,
    ReservationKind,
    ReservationStatus,
    TimeInForce,
)
from trading_platform.common.ids import new_id

MONEY = Numeric(20, 4)


def enum_type(enum_class: type[Any], name: str) -> Enum:
    return Enum(
        enum_class,
        name=name,
        values_callable=lambda members: [member.value for member in members],
        validate_strings=True,
    )


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class User(TimestampMixin, Base):
    __tablename__ = "users"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=new_id)
    email: Mapped[str] = mapped_column(String(320), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(
        Boolean, default=True, server_default="true", nullable=False
    )


class Account(TimestampMixin, Base):
    __tablename__ = "accounts"
    __table_args__ = (
        CheckConstraint("initial_cash >= 0", name="initial_cash_nonnegative"),
        CheckConstraint("current_cash >= 0", name="current_cash_nonnegative"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=new_id)
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), unique=True, nullable=False
    )
    base_currency: Mapped[str] = mapped_column(String(3), default="USD", server_default="USD")
    initial_cash: Mapped[Decimal] = mapped_column(
        MONEY, default=Decimal("100000.0000"), server_default="100000.0000"
    )
    current_cash: Mapped[Decimal] = mapped_column(
        MONEY, default=Decimal("100000.0000"), server_default="100000.0000"
    )


class Instrument(TimestampMixin, Base):
    __tablename__ = "instruments"
    __table_args__ = (
        CheckConstraint("tick_size > 0", name="tick_size_positive"),
        CheckConstraint("initial_price > 0", name="initial_price_positive"),
    )

    symbol: Mapped[str] = mapped_column(String(16), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(128), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), default="USD", server_default="USD")
    tick_size: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    initial_price: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    random_seed: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)


class Order(TimestampMixin, Base):
    __tablename__ = "orders"
    __table_args__ = (
        CheckConstraint("quantity > 0", name="quantity_positive"),
        CheckConstraint("filled_quantity >= 0", name="filled_quantity_nonnegative"),
        CheckConstraint("filled_quantity <= quantity", name="filled_not_above_quantity"),
        CheckConstraint("limit_price IS NULL OR limit_price > 0", name="limit_price_positive"),
        CheckConstraint("stop_price IS NULL OR stop_price > 0", name="stop_price_positive"),
        CheckConstraint(
            "average_fill_price IS NULL OR average_fill_price > 0",
            name="average_fill_price_positive",
        ),
        CheckConstraint("version >= 1", name="version_positive"),
        Index("ix_orders_account_status_created", "account_id", "status", "created_at"),
        Index("ix_orders_symbol_status", "symbol", "status"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=new_id)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    symbol: Mapped[str] = mapped_column(ForeignKey("instruments.symbol"), nullable=False)
    side: Mapped[OrderSide] = mapped_column(enum_type(OrderSide, "order_side"), nullable=False)
    order_type: Mapped[OrderType] = mapped_column(
        enum_type(OrderType, "order_type"), nullable=False
    )
    time_in_force: Mapped[TimeInForce] = mapped_column(
        enum_type(TimeInForce, "time_in_force"), nullable=False
    )
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    filled_quantity: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    limit_price: Mapped[Decimal | None] = mapped_column(MONEY)
    stop_price: Mapped[Decimal | None] = mapped_column(MONEY)
    is_triggered: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    status: Mapped[OrderStatus] = mapped_column(
        enum_type(OrderStatus, "order_status"),
        default=OrderStatus.PENDING_VALIDATION,
        server_default=OrderStatus.PENDING_VALIDATION.value,
        nullable=False,
    )
    average_fill_price: Mapped[Decimal | None] = mapped_column(MONEY)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    version: Mapped[int] = mapped_column(Integer, default=1, server_default="1")


class OrderEvent(Base):
    __tablename__ = "order_events"
    __table_args__ = (
        UniqueConstraint("order_id", "order_version", name="uq_order_events_order_version"),
        CheckConstraint("order_version >= 1", name="order_version_positive"),
        Index("ix_order_events_order_created", "order_id", "created_at"),
    )

    event_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=new_id)
    order_id: Mapped[UUID] = mapped_column(ForeignKey("orders.id"), nullable=False)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    order_version: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[OrderStatus] = mapped_column(enum_type(OrderStatus, "order_status"))
    reason_code: Mapped[str | None] = mapped_column(String(64))
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class Execution(Base):
    __tablename__ = "executions"
    __table_args__ = (
        CheckConstraint("quantity > 0", name="quantity_positive"),
        CheckConstraint("price > 0", name="price_positive"),
        Index("ix_executions_account_created", "account_id", "executed_at"),
        Index("ix_executions_order_created", "order_id", "executed_at"),
    )

    execution_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    order_id: Mapped[UUID] = mapped_column(ForeignKey("orders.id"), nullable=False)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    symbol: Mapped[str] = mapped_column(ForeignKey("instruments.symbol"), nullable=False)
    side: Mapped[OrderSide] = mapped_column(enum_type(OrderSide, "order_side"), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    executed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class IdempotencyRecord(TimestampMixin, Base):
    __tablename__ = "idempotency_records"
    __table_args__ = (
        UniqueConstraint(
            "account_id", "operation", "idempotency_key", name="uq_idempotency_scope_key"
        ),
        Index("ix_idempotency_expires", "expires_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=new_id)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    operation: Mapped[str] = mapped_column(String(64), nullable=False)
    path: Mapped[str] = mapped_column(String(255), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    response_status: Mapped[int | None] = mapped_column(Integer)
    response_body: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CashBalance(TimestampMixin, Base):
    __tablename__ = "cash_balances"
    __table_args__ = (
        CheckConstraint("total_amount >= 0", name="total_nonnegative"),
        CheckConstraint("available_amount >= 0", name="available_nonnegative"),
        CheckConstraint("reserved_amount >= 0", name="reserved_nonnegative"),
        CheckConstraint(
            "total_amount = available_amount + reserved_amount", name="amounts_balance"
        ),
        UniqueConstraint("account_id", "currency", name="uq_cash_account_currency"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=new_id)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), default="USD", server_default="USD")
    total_amount: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    available_amount: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    reserved_amount: Mapped[Decimal] = mapped_column(
        MONEY, default=Decimal("0"), server_default="0"
    )


class Position(TimestampMixin, Base):
    __tablename__ = "positions"
    __table_args__ = (
        CheckConstraint("quantity >= 0", name="quantity_nonnegative"),
        CheckConstraint("reserved_quantity >= 0", name="reserved_quantity_nonnegative"),
        CheckConstraint("reserved_quantity <= quantity", name="reserved_not_above_quantity"),
        CheckConstraint("average_cost >= 0", name="average_cost_nonnegative"),
        UniqueConstraint("account_id", "symbol", name="uq_positions_account_symbol"),
        Index("ix_positions_account", "account_id"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=new_id)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    symbol: Mapped[str] = mapped_column(ForeignKey("instruments.symbol"), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    reserved_quantity: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    average_cost: Mapped[Decimal] = mapped_column(MONEY, default=Decimal("0"), server_default="0")
    realized_pnl: Mapped[Decimal] = mapped_column(MONEY, default=Decimal("0"), server_default="0")

    def __init__(
        self,
        *,
        account_id: UUID,
        symbol: str,
        quantity: int = 0,
        reserved_quantity: int = 0,
        average_cost: Decimal = Decimal("0"),
        realized_pnl: Decimal = Decimal("0"),
    ) -> None:
        self.account_id = account_id
        self.symbol = symbol
        self.quantity = quantity
        self.reserved_quantity = reserved_quantity
        self.average_cost = average_cost
        self.realized_pnl = realized_pnl


class Reservation(TimestampMixin, Base):
    __tablename__ = "reservations"
    __table_args__ = (
        CheckConstraint("original_amount > 0", name="original_amount_positive"),
        CheckConstraint("remaining_amount >= 0", name="remaining_amount_nonnegative"),
        CheckConstraint("remaining_amount <= original_amount", name="remaining_not_above_original"),
        UniqueConstraint("order_id", name="uq_reservations_order"),
        Index("ix_reservations_account_status", "account_id", "status"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=new_id)
    account_id: Mapped[UUID] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    order_id: Mapped[UUID] = mapped_column(ForeignKey("orders.id"), nullable=False)
    symbol: Mapped[str | None] = mapped_column(ForeignKey("instruments.symbol"))
    kind: Mapped[ReservationKind] = mapped_column(
        enum_type(ReservationKind, "reservation_kind"), nullable=False
    )
    status: Mapped[ReservationStatus] = mapped_column(
        enum_type(ReservationStatus, "reservation_status"),
        default=ReservationStatus.ACTIVE,
        server_default=ReservationStatus.ACTIVE.value,
        nullable=False,
    )
    original_amount: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    remaining_amount: Mapped[Decimal] = mapped_column(MONEY, nullable=False)


class ProcessedEventMixin:
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=new_id)
    consumer_name: Mapped[str] = mapped_column(String(128), nullable=False)
    event_id: Mapped[UUID] = mapped_column(Uuid, nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class OrderProcessedEvent(ProcessedEventMixin, Base):
    __tablename__ = "order_processed_events"
    __table_args__ = (
        UniqueConstraint("consumer_name", "event_id", name="uq_order_processed_consumer_event"),
    )


class PortfolioProcessedEvent(ProcessedEventMixin, Base):
    __tablename__ = "portfolio_processed_events"
    __table_args__ = (
        UniqueConstraint("consumer_name", "event_id", name="uq_portfolio_processed_consumer_event"),
    )


class ApiProcessedEvent(ProcessedEventMixin, Base):
    __tablename__ = "api_processed_events"
    __table_args__ = (
        UniqueConstraint("consumer_name", "event_id", name="uq_api_processed_consumer_event"),
    )


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint("attempts >= 0", name="attempts_nonnegative"),
        Index("ix_outbox_claim", "status", "next_attempt_at", "created_at"),
        Index("ix_outbox_aggregate", "aggregate_type", "aggregate_id", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=new_id)
    event_id: Mapped[UUID] = mapped_column(Uuid, unique=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    aggregate_type: Mapped[str | None] = mapped_column(String(64))
    aggregate_id: Mapped[UUID | None] = mapped_column(Uuid)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    message_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    headers: Mapped[dict[str, str]] = mapped_column(JSON, default=dict)
    status: Mapped[OutboxStatus] = mapped_column(
        enum_type(OutboxStatus, "outbox_status"),
        default=OutboxStatus.PENDING,
        server_default=OutboxStatus.PENDING.value,
        nullable=False,
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    last_error: Mapped[str | None] = mapped_column(Text)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
