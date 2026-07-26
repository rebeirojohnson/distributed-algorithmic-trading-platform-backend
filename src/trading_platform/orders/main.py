# ruff: noqa: B008
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from trading_platform.common.database import Database
from trading_platform.common.errors import PlatformError
from trading_platform.common.models import Account, Execution, Order, OrderEvent
from trading_platform.common.security import decode_access_token
from trading_platform.common.settings import get_settings

from .domain import InvalidTransition
from .runtime import OrderRuntime
from .schemas import OrderCreate, OrderEventRead, OrderRead
from .service import cancel_order, create_order, serialize_order


async def session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.database.session() as value:
        yield value


async def account_id(
    request: Request,
    db: AsyncSession = Depends(session),
    authorization: str | None = Header(default=None),
) -> UUID:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Bearer access token required")
    claims = decode_access_token(authorization[7:], request.app.state.settings)
    account = await db.scalar(select(Account).where(Account.user_id == claims.sub))
    if account is None:
        raise HTTPException(403, "account not found")
    return account.id


def create_app() -> FastAPI:
    settings = get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
        app.state.settings = settings
        app.state.database = Database(settings)
        app.state.runtime = OrderRuntime(app.state.database, settings)
        await app.state.runtime.start()
        try:
            yield
        finally:
            await app.state.runtime.close()
            await app.state.database.dispose()

    app = FastAPI(title="Trading Platform Orders", version="1.0.0", lifespan=lifespan)

    @app.exception_handler(PlatformError)
    async def platform_error(request: Request, exc: PlatformError) -> JSONResponse:
        from trading_platform.common.ids import new_id

        return JSONResponse(
            status_code=exc.status_code, content=exc.response(new_id()).model_dump(mode="json")
        )

    @app.exception_handler(InvalidTransition)
    async def invalid_transition(request: Request, exc: InvalidTransition) -> JSONResponse:
        return JSONResponse(
            status_code=409, content={"code": "INVALID_ORDER_STATE", "message": str(exc)}
        )

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "live", "service": "orders"}

    @app.get("/health/ready")
    async def ready(db: AsyncSession = Depends(session)) -> dict[str, str]:
        try:
            await db.execute(text("SELECT 1"))
        except Exception as exc:
            raise HTTPException(503, "database unavailable") from exc
        return {"status": "ready", "service": "orders"}

    @app.post("/internal/v1/orders", status_code=202)
    async def submit(
        command: OrderCreate,
        db: AsyncSession = Depends(session),
        owner: UUID = Depends(account_id),
        idempotency_key: str = Header(min_length=1, max_length=255, alias="Idempotency-Key"),
    ) -> Response:
        try:
            status, body = await create_order(
                db, account_id=owner, command=command, idempotency_key=idempotency_key
            )
            await db.commit()
        except IntegrityError:
            await db.rollback()
            status, body = await create_order(
                db, account_id=owner, command=command, idempotency_key=idempotency_key
            )
        return JSONResponse(status_code=status, content=body)

    @app.get("/internal/v1/orders", response_model=list[OrderRead])
    async def list_orders(
        db: AsyncSession = Depends(session),
        owner: UUID = Depends(account_id),
        status: str | None = None,
        symbol: str | None = None,
        limit: int = Query(default=50, ge=1, le=200),
    ) -> list[dict[str, object]]:
        statement = (
            select(Order)
            .where(Order.account_id == owner)
            .order_by(Order.created_at.desc())
            .limit(limit)
        )
        if status:
            statement = statement.where(Order.status == status.upper())
        if symbol:
            statement = statement.where(Order.symbol == symbol.upper())
        values = (await db.scalars(statement)).all()
        return [serialize_order(value) for value in values]

    @app.get("/internal/v1/orders/{order_id}", response_model=OrderRead)
    async def get_order(
        order_id: UUID,
        db: AsyncSession = Depends(session),
        owner: UUID = Depends(account_id),
    ) -> dict[str, object]:
        value = await db.scalar(
            select(Order).where(Order.id == order_id, Order.account_id == owner)
        )
        if value is None:
            raise HTTPException(404, "order not found")
        return serialize_order(value)

    @app.post("/internal/v1/orders/{order_id}/cancel", status_code=202)
    async def cancel_command(
        order_id: UUID,
        db: AsyncSession = Depends(session),
        owner: UUID = Depends(account_id),
        idempotency_key: str = Header(min_length=1, max_length=255, alias="Idempotency-Key"),
    ) -> Response:
        try:
            status, body = await cancel_order(
                db, account_id=owner, order_id=order_id, idempotency_key=idempotency_key
            )
            await db.commit()
        except IntegrityError:
            await db.rollback()
            status, body = await cancel_order(
                db, account_id=owner, order_id=order_id, idempotency_key=idempotency_key
            )
        return JSONResponse(status_code=status, content=body)

    @app.get("/internal/v1/orders/{order_id}/events", response_model=list[OrderEventRead])
    async def order_events(
        order_id: UUID,
        db: AsyncSession = Depends(session),
        owner: UUID = Depends(account_id),
    ) -> list[dict[str, object]]:
        owned = await db.scalar(
            select(Order.id).where(Order.id == order_id, Order.account_id == owner)
        )
        if owned is None:
            raise HTTPException(404, "order not found")
        events = (
            await db.scalars(
                select(OrderEvent)
                .where(OrderEvent.order_id == order_id)
                .order_by(OrderEvent.order_version)
            )
        ).all()
        return [
            {
                "event_id": event.event_id,
                "order_id": event.order_id,
                "version": event.order_version,
                "status": event.status,
                "reason": event.reason_code,
                "occurred_at": event.created_at,
            }
            for event in events
        ]

    @app.get("/internal/v1/executions")
    async def executions(
        db: AsyncSession = Depends(session),
        owner: UUID = Depends(account_id),
        limit: int = Query(default=50, ge=1, le=200),
    ) -> list[dict[str, object]]:
        values = (
            await db.scalars(
                select(Execution)
                .where(Execution.account_id == owner)
                .order_by(Execution.executed_at.desc())
                .limit(limit)
            )
        ).all()
        return [_execution(value) for value in values]

    @app.get("/internal/v1/executions/{execution_id}")
    async def execution(
        execution_id: UUID,
        db: AsyncSession = Depends(session),
        owner: UUID = Depends(account_id),
    ) -> dict[str, object]:
        value = await db.scalar(
            select(Execution).where(
                Execution.execution_id == execution_id, Execution.account_id == owner
            )
        )
        if value is None:
            raise HTTPException(404, "execution not found")
        return _execution(value)

    return app


def _execution(value: Execution) -> dict[str, object]:
    return {
        "id": value.execution_id,
        "order_id": value.order_id,
        "account_id": value.account_id,
        "symbol": value.symbol,
        "side": value.side,
        "quantity": value.quantity,
        "price": value.price,
        "executed_at": value.executed_at,
    }


app = create_app()
