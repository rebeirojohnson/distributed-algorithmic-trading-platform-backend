# ruff: noqa: B008
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from trading_platform.common.database import Database
from trading_platform.common.errors import PlatformError
from trading_platform.common.models import Account, CashBalance, Position
from trading_platform.common.security import decode_access_token
from trading_platform.common.settings import get_settings

from .runtime import PortfolioRuntime
from .schemas import (
    ExecutionEvent,
    PortfolioRead,
    PositionRead,
    ValidationRequest,
    ValidationResult,
)
from .service import settle_execution, validate_and_reserve


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
        app.state.runtime = PortfolioRuntime(app.state.database, settings)
        await app.state.runtime.start()
        try:
            yield
        finally:
            await app.state.runtime.close()
            await app.state.database.dispose()

    app = FastAPI(title="Trading Platform Portfolio", version="1.0.0", lifespan=lifespan)

    @app.exception_handler(PlatformError)
    async def platform_error(request: Request, exc: PlatformError) -> JSONResponse:
        from trading_platform.common.ids import new_id

        return JSONResponse(
            status_code=exc.status_code, content=exc.response(new_id()).model_dump(mode="json")
        )

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "live", "service": "portfolio"}

    @app.get("/health/ready")
    async def ready(db: AsyncSession = Depends(session)) -> dict[str, str]:
        try:
            await db.execute(text("SELECT 1"))
        except Exception as exc:
            raise HTTPException(503, "database unavailable") from exc
        return {"status": "ready", "service": "portfolio"}

    @app.get("/internal/v1/portfolio", response_model=PortfolioRead)
    async def portfolio(
        db: AsyncSession = Depends(session), owner: UUID = Depends(account_id)
    ) -> PortfolioRead:
        cash = await _cash(db, owner)
        positions = await _positions(db, owner)
        return PortfolioRead(
            account_id=owner,
            cash_total=cash.total_amount,
            cash_reserved=cash.reserved_amount,
            cash_available=cash.available_amount,
            positions=[_position(value) for value in positions],
            as_of=datetime.now(UTC),
            quote_stale=bool(positions),
        )

    @app.get("/internal/v1/positions", response_model=list[PositionRead])
    async def positions(
        db: AsyncSession = Depends(session),
        owner: UUID = Depends(account_id),
        limit: int = Query(default=100, ge=1, le=200),
    ) -> list[PositionRead]:
        return [_position(value) for value in (await _positions(db, owner, limit=limit))]

    @app.get("/internal/v1/positions/{symbol}", response_model=PositionRead)
    async def position(
        symbol: str,
        db: AsyncSession = Depends(session),
        owner: UUID = Depends(account_id),
    ) -> PositionRead:
        value = await db.scalar(
            select(Position).where(Position.account_id == owner, Position.symbol == symbol.upper())
        )
        if value is None:
            raise HTTPException(404, "position not found")
        return _position(value)

    @app.post("/internal/v1/workers/validate", response_model=ValidationResult)
    async def validate(
        command: ValidationRequest,
        db: AsyncSession = Depends(session),
        owner: UUID = Depends(account_id),
    ) -> ValidationResult:
        if command.account_id != owner:
            raise HTTPException(403, "account mismatch")
        result = await validate_and_reserve(db, command)
        await db.commit()
        return result

    @app.post("/internal/v1/workers/executions", status_code=204)
    async def execution(
        event: ExecutionEvent,
        db: AsyncSession = Depends(session),
        owner: UUID = Depends(account_id),
    ) -> None:
        if event.account_id != owner:
            raise HTTPException(403, "account mismatch")
        await settle_execution(db, event)
        await db.commit()

    return app


async def _cash(db: AsyncSession, owner: UUID) -> CashBalance:
    cash = await db.scalar(
        select(CashBalance).where(CashBalance.account_id == owner, CashBalance.currency == "USD")
    )
    if cash is None:
        raise HTTPException(404, "cash balance not found")
    return cash


async def _positions(db: AsyncSession, owner: UUID, *, limit: int = 200) -> list[Position]:
    return list(
        (
            await db.scalars(
                select(Position)
                .where(Position.account_id == owner, Position.quantity > 0)
                .order_by(Position.symbol)
                .limit(limit)
            )
        ).all()
    )


def _position(value: Position) -> PositionRead:
    return PositionRead(
        symbol=value.symbol,
        quantity=value.quantity,
        available_quantity=value.quantity - value.reserved_quantity,
        average_cost=value.average_cost,
        market_price=None,
        market_value=None,
        realized_pnl=value.realized_pnl,
        unrealized_pnl=None,
        quote_stale=True,
    )


app = create_app()
