"""Create the initial trading schema and seed instruments."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260726_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UUID = sa.Uuid()
MONEY = sa.Numeric(20, 4)
JSON = sa.JSON()

ORDER_SIDE = postgresql.ENUM("BUY", "SELL", name="order_side", create_type=False)
ORDER_TYPE = postgresql.ENUM(
    "MARKET", "LIMIT", "STOP", "STOP_LIMIT", name="order_type", create_type=False
)
TIME_IN_FORCE = postgresql.ENUM("DAY", "GTC", name="time_in_force", create_type=False)
ORDER_STATUS = postgresql.ENUM(
    "PENDING_VALIDATION",
    "ACCEPTED",
    "ACTIVE",
    "PARTIALLY_FILLED",
    "FILLED",
    "REJECTED",
    "CANCEL_PENDING",
    "CANCELLED",
    "EXPIRED",
    "FAILED",
    name="order_status",
    create_type=False,
)
RESERVATION_KIND = postgresql.ENUM("CASH", "SHARES", name="reservation_kind", create_type=False)
RESERVATION_STATUS = postgresql.ENUM(
    "ACTIVE", "RELEASED", "CONSUMED", name="reservation_status", create_type=False
)
OUTBOX_STATUS = postgresql.ENUM(
    "PENDING", "PUBLISHED", "FAILED", name="outbox_status", create_type=False
)


def _timestamps() -> list[sa.Column]:
    return [
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    ]


def upgrade() -> None:
    bind = op.get_bind()
    for enum in (
        ORDER_SIDE,
        ORDER_TYPE,
        TIME_IN_FORCE,
        ORDER_STATUS,
        RESERVATION_KIND,
        RESERVATION_STATUS,
        OUTBOX_STATUS,
    ):
        enum.create(bind, checkfirst=True)

    op.create_table(
        "users",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("email", sa.String(320), nullable=False),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.true(), nullable=False),
        *_timestamps(),
        sa.UniqueConstraint("email", name="uq_users_email"),
    )
    op.create_table(
        "accounts",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("user_id", UUID, sa.ForeignKey("users.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("base_currency", sa.String(3), server_default="USD", nullable=False),
        sa.Column("initial_cash", MONEY, server_default="100000.0000", nullable=False),
        sa.Column("current_cash", MONEY, server_default="100000.0000", nullable=False),
        *_timestamps(),
        sa.CheckConstraint("initial_cash >= 0", name="ck_accounts_initial_cash_nonnegative"),
        sa.CheckConstraint("current_cash >= 0", name="ck_accounts_current_cash_nonnegative"),
        sa.UniqueConstraint("user_id", name="uq_accounts_user_id"),
    )
    instruments = op.create_table(
        "instruments",
        sa.Column("symbol", sa.String(16), primary_key=True),
        sa.Column("display_name", sa.String(128), nullable=False),
        sa.Column("currency", sa.String(3), server_default="USD", nullable=False),
        sa.Column("tick_size", MONEY, nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.true(), nullable=False),
        sa.Column("initial_price", MONEY, nullable=False),
        sa.Column("random_seed", sa.Integer(), nullable=False),
        *_timestamps(),
        sa.CheckConstraint("tick_size > 0", name="ck_instruments_tick_size_positive"),
        sa.CheckConstraint("initial_price > 0", name="ck_instruments_initial_price_positive"),
        sa.UniqueConstraint("random_seed", name="uq_instruments_random_seed"),
    )
    op.bulk_insert(
        instruments,
        [
            {
                "symbol": "AAPL",
                "display_name": "Apple Inc.",
                "currency": "USD",
                "tick_size": "0.0100",
                "initial_price": "210.0000",
                "random_seed": 1101,
            },
            {
                "symbol": "MSFT",
                "display_name": "Microsoft Corporation",
                "currency": "USD",
                "tick_size": "0.0100",
                "initial_price": "440.0000",
                "random_seed": 1102,
            },
            {
                "symbol": "GOOGL",
                "display_name": "Alphabet Inc.",
                "currency": "USD",
                "tick_size": "0.0100",
                "initial_price": "185.0000",
                "random_seed": 1103,
            },
            {
                "symbol": "AMZN",
                "display_name": "Amazon.com Inc.",
                "currency": "USD",
                "tick_size": "0.0100",
                "initial_price": "200.0000",
                "random_seed": 1104,
            },
            {
                "symbol": "META",
                "display_name": "Meta Platforms Inc.",
                "currency": "USD",
                "tick_size": "0.0100",
                "initial_price": "575.0000",
                "random_seed": 1105,
            },
            {
                "symbol": "NVDA",
                "display_name": "NVIDIA Corporation",
                "currency": "USD",
                "tick_size": "0.0100",
                "initial_price": "140.0000",
                "random_seed": 1106,
            },
            {
                "symbol": "TSLA",
                "display_name": "Tesla Inc.",
                "currency": "USD",
                "tick_size": "0.0100",
                "initial_price": "325.0000",
                "random_seed": 1107,
            },
            {
                "symbol": "JPM",
                "display_name": "JPMorgan Chase & Co.",
                "currency": "USD",
                "tick_size": "0.0100",
                "initial_price": "245.0000",
                "random_seed": 1108,
            },
            {
                "symbol": "V",
                "display_name": "Visa Inc.",
                "currency": "USD",
                "tick_size": "0.0100",
                "initial_price": "315.0000",
                "random_seed": 1109,
            },
            {
                "symbol": "JNJ",
                "display_name": "Johnson & Johnson",
                "currency": "USD",
                "tick_size": "0.0100",
                "initial_price": "155.0000",
                "random_seed": 1110,
            },
            {
                "symbol": "WMT",
                "display_name": "Walmart Inc.",
                "currency": "USD",
                "tick_size": "0.0100",
                "initial_price": "95.0000",
                "random_seed": 1111,
            },
            {
                "symbol": "PG",
                "display_name": "Procter & Gamble Co.",
                "currency": "USD",
                "tick_size": "0.0100",
                "initial_price": "170.0000",
                "random_seed": 1112,
            },
            {
                "symbol": "XOM",
                "display_name": "Exxon Mobil Corporation",
                "currency": "USD",
                "tick_size": "0.0100",
                "initial_price": "115.0000",
                "random_seed": 1113,
            },
            {
                "symbol": "KO",
                "display_name": "The Coca-Cola Company",
                "currency": "USD",
                "tick_size": "0.0100",
                "initial_price": "68.0000",
                "random_seed": 1114,
            },
            {
                "symbol": "DIS",
                "display_name": "The Walt Disney Company",
                "currency": "USD",
                "tick_size": "0.0100",
                "initial_price": "115.0000",
                "random_seed": 1115,
            },
        ],
    )
    op.create_table(
        "orders",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("account_id", UUID, sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("symbol", sa.String(16), sa.ForeignKey("instruments.symbol"), nullable=False),
        sa.Column("side", ORDER_SIDE, nullable=False),
        sa.Column("order_type", ORDER_TYPE, nullable=False),
        sa.Column("time_in_force", TIME_IN_FORCE, nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("filled_quantity", sa.Integer(), server_default="0", nullable=False),
        sa.Column("limit_price", MONEY),
        sa.Column("stop_price", MONEY),
        sa.Column("is_triggered", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("status", ORDER_STATUS, server_default="PENDING_VALIDATION", nullable=False),
        sa.Column("average_fill_price", MONEY),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("accepted_at", sa.DateTime(timezone=True)),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        *_timestamps(),
        sa.CheckConstraint("quantity > 0", name="ck_orders_quantity_positive"),
        sa.CheckConstraint("filled_quantity >= 0", name="ck_orders_filled_quantity_nonnegative"),
        sa.CheckConstraint(
            "filled_quantity <= quantity", name="ck_orders_filled_not_above_quantity"
        ),
        sa.CheckConstraint(
            "limit_price IS NULL OR limit_price > 0", name="ck_orders_limit_price_positive"
        ),
        sa.CheckConstraint(
            "stop_price IS NULL OR stop_price > 0", name="ck_orders_stop_price_positive"
        ),
        sa.CheckConstraint(
            "average_fill_price IS NULL OR average_fill_price > 0",
            name="ck_orders_average_fill_price_positive",
        ),
        sa.CheckConstraint("version >= 1", name="ck_orders_version_positive"),
    )
    op.create_index(
        "ix_orders_account_status_created", "orders", ["account_id", "status", "created_at"]
    )
    op.create_index("ix_orders_symbol_status", "orders", ["symbol", "status"])
    op.create_table(
        "order_events",
        sa.Column("event_id", UUID, primary_key=True),
        sa.Column("order_id", UUID, sa.ForeignKey("orders.id"), nullable=False),
        sa.Column("account_id", UUID, sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("order_version", sa.Integer(), nullable=False),
        sa.Column("status", ORDER_STATUS, nullable=False),
        sa.Column("reason_code", sa.String(64)),
        sa.Column("details", JSON, nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("order_version >= 1", name="ck_order_events_order_version_positive"),
        sa.UniqueConstraint("order_id", "order_version", name="uq_order_events_order_version"),
    )
    op.create_index("ix_order_events_order_created", "order_events", ["order_id", "created_at"])
    op.create_table(
        "executions",
        sa.Column("execution_id", UUID, primary_key=True),
        sa.Column("order_id", UUID, sa.ForeignKey("orders.id"), nullable=False),
        sa.Column("account_id", UUID, sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("symbol", sa.String(16), sa.ForeignKey("instruments.symbol"), nullable=False),
        sa.Column("side", ORDER_SIDE, nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("price", MONEY, nullable=False),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("quantity > 0", name="ck_executions_quantity_positive"),
        sa.CheckConstraint("price > 0", name="ck_executions_price_positive"),
    )
    op.create_index("ix_executions_account_created", "executions", ["account_id", "executed_at"])
    op.create_index("ix_executions_order_created", "executions", ["order_id", "executed_at"])
    op.create_table(
        "idempotency_records",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("account_id", UUID, sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("operation", sa.String(64), nullable=False),
        sa.Column("path", sa.String(255), nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("response_status", sa.Integer()),
        sa.Column("response_body", JSON),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        *_timestamps(),
        sa.UniqueConstraint(
            "account_id", "operation", "idempotency_key", name="uq_idempotency_scope_key"
        ),
    )
    op.create_index("ix_idempotency_expires", "idempotency_records", ["expires_at"])
    op.create_table(
        "cash_balances",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("account_id", UUID, sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("currency", sa.String(3), server_default="USD", nullable=False),
        sa.Column("total_amount", MONEY, nullable=False),
        sa.Column("available_amount", MONEY, nullable=False),
        sa.Column("reserved_amount", MONEY, server_default="0", nullable=False),
        *_timestamps(),
        sa.CheckConstraint("total_amount >= 0", name="ck_cash_balances_total_nonnegative"),
        sa.CheckConstraint("available_amount >= 0", name="ck_cash_balances_available_nonnegative"),
        sa.CheckConstraint("reserved_amount >= 0", name="ck_cash_balances_reserved_nonnegative"),
        sa.CheckConstraint(
            "total_amount = available_amount + reserved_amount",
            name="ck_cash_balances_amounts_balance",
        ),
        sa.UniqueConstraint("account_id", "currency", name="uq_cash_account_currency"),
    )
    op.create_table(
        "positions",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("account_id", UUID, sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("symbol", sa.String(16), sa.ForeignKey("instruments.symbol"), nullable=False),
        sa.Column("quantity", sa.Integer(), server_default="0", nullable=False),
        sa.Column("reserved_quantity", sa.Integer(), server_default="0", nullable=False),
        sa.Column("average_cost", MONEY, server_default="0", nullable=False),
        sa.Column("realized_pnl", MONEY, server_default="0", nullable=False),
        *_timestamps(),
        sa.CheckConstraint("quantity >= 0", name="ck_positions_quantity_nonnegative"),
        sa.CheckConstraint(
            "reserved_quantity >= 0", name="ck_positions_reserved_quantity_nonnegative"
        ),
        sa.CheckConstraint(
            "reserved_quantity <= quantity", name="ck_positions_reserved_not_above_quantity"
        ),
        sa.CheckConstraint("average_cost >= 0", name="ck_positions_average_cost_nonnegative"),
        sa.UniqueConstraint("account_id", "symbol", name="uq_positions_account_symbol"),
    )
    op.create_index("ix_positions_account", "positions", ["account_id"])
    op.create_table(
        "reservations",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("account_id", UUID, sa.ForeignKey("accounts.id"), nullable=False),
        sa.Column("order_id", UUID, sa.ForeignKey("orders.id"), nullable=False),
        sa.Column("symbol", sa.String(16), sa.ForeignKey("instruments.symbol")),
        sa.Column("kind", RESERVATION_KIND, nullable=False),
        sa.Column("status", RESERVATION_STATUS, server_default="ACTIVE", nullable=False),
        sa.Column("original_amount", MONEY, nullable=False),
        sa.Column("remaining_amount", MONEY, nullable=False),
        *_timestamps(),
        sa.CheckConstraint("original_amount > 0", name="ck_reservations_original_amount_positive"),
        sa.CheckConstraint(
            "remaining_amount >= 0", name="ck_reservations_remaining_amount_nonnegative"
        ),
        sa.CheckConstraint(
            "remaining_amount <= original_amount",
            name="ck_reservations_remaining_not_above_original",
        ),
        sa.UniqueConstraint("order_id", name="uq_reservations_order"),
    )
    op.create_index("ix_reservations_account_status", "reservations", ["account_id", "status"])
    for table_name, constraint_name in (
        ("order_processed_events", "uq_order_processed_consumer_event"),
        ("portfolio_processed_events", "uq_portfolio_processed_consumer_event"),
        ("api_processed_events", "uq_api_processed_consumer_event"),
    ):
        op.create_table(
            table_name,
            sa.Column("id", UUID, primary_key=True),
            sa.Column("consumer_name", sa.String(128), nullable=False),
            sa.Column("event_id", UUID, nullable=False),
            sa.Column(
                "processed_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.UniqueConstraint("consumer_name", "event_id", name=constraint_name),
        )
    op.create_table(
        "outbox_events",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("event_id", UUID, nullable=False),
        sa.Column("event_type", sa.String(128), nullable=False),
        sa.Column("aggregate_type", sa.String(64)),
        sa.Column("aggregate_id", UUID),
        sa.Column("topic", sa.String(255), nullable=False),
        sa.Column("message_key", sa.String(255), nullable=False),
        sa.Column("payload", JSON, nullable=False),
        sa.Column("headers", JSON, nullable=False),
        sa.Column("status", OUTBOX_STATUS, server_default="PENDING", nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("last_error", sa.Text()),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("attempts >= 0", name="ck_outbox_events_attempts_nonnegative"),
        sa.UniqueConstraint("event_id", name="uq_outbox_events_event_id"),
    )
    op.create_index("ix_outbox_claim", "outbox_events", ["status", "next_attempt_at", "created_at"])
    op.create_index(
        "ix_outbox_aggregate", "outbox_events", ["aggregate_type", "aggregate_id", "created_at"]
    )


def downgrade() -> None:
    for table_name in (
        "outbox_events",
        "api_processed_events",
        "portfolio_processed_events",
        "order_processed_events",
        "reservations",
        "positions",
        "cash_balances",
        "idempotency_records",
        "executions",
        "order_events",
        "orders",
        "instruments",
        "accounts",
        "users",
    ):
        op.drop_table(table_name)
    bind = op.get_bind()
    for enum in reversed(
        (
            ORDER_SIDE,
            ORDER_TYPE,
            TIME_IN_FORCE,
            ORDER_STATUS,
            RESERVATION_KIND,
            RESERVATION_STATUS,
            OUTBOX_STATUS,
        )
    ):
        enum.drop(bind, checkfirst=True)
