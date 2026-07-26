# ruff: noqa: B008
from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from trading_platform.common.contracts import (
    EXECUTIONS_TOPIC,
    MARKET_QUOTES_TOPIC,
    ORDER_EVENTS_TOPIC,
    PORTFOLIO_EVENTS_TOPIC,
)
from trading_platform.common.database import Database
from trading_platform.common.errors import PlatformError
from trading_platform.common.kafka import KafkaJsonConsumer, KafkaJsonProducer
from trading_platform.common.models import Account, CashBalance, User
from trading_platform.common.security import (
    create_access_token,
    decode_access_token,
    hash_password,
    verify_password,
)
from trading_platform.common.settings import get_settings

from .realtime import RealtimeHub, websocket_endpoint
from .schemas import LoginRequest, RegisterRequest, TokenResponse, UserResponse


@dataclass(frozen=True, slots=True)
class Identity:
    user: User
    account: Account


async def session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.database.session() as value:
        yield value


async def identity(
    request: Request,
    db: AsyncSession = Depends(session),
    authorization: str | None = Header(default=None),
) -> Identity:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            401, "Bearer access token required", headers={"WWW-Authenticate": "Bearer"}
        )
    claims = decode_access_token(authorization[7:], request.app.state.settings)
    row = (
        await db.execute(
            select(User, Account)
            .join(Account, Account.user_id == User.id)
            .where(User.id == claims.sub, User.is_active.is_(True))
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(401, "user is inactive or unavailable")
    return Identity(row[0], row[1])


def create_app() -> FastAPI:
    settings = get_settings()
    service_urls = {
        "market": os.getenv("TRADING_MARKET_DATA_URL", "http://localhost:8001"),
        "orders": os.getenv("TRADING_ORDERS_URL", "http://localhost:8002"),
        "portfolio": os.getenv("TRADING_PORTFOLIO_URL", "http://localhost:8003"),
    }

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
        app.state.settings = settings
        app.state.database = Database(settings)
        app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(5.0, connect=2.0))
        app.state.service_urls = service_urls
        app.state.realtime_hub = RealtimeHub()
        app.state.realtime_stop = asyncio.Event()
        app.state.realtime_producer = KafkaJsonProducer(settings)
        await app.state.realtime_producer.start()
        app.state.realtime_consumer = KafkaJsonConsumer(
            settings=settings,
            topics=[
                MARKET_QUOTES_TOPIC,
                ORDER_EVENTS_TOPIC,
                EXECUTIONS_TOPIC,
                PORTFOLIO_EVENTS_TOPIC,
            ],
            group_id=f"gateway-realtime-{uuid4()}",
            handler=app.state.realtime_hub.publish,
            dlq_publisher=app.state.realtime_producer,
        )
        await app.state.realtime_consumer.start()
        app.state.realtime_task = asyncio.create_task(
            app.state.realtime_consumer.consume(app.state.realtime_stop)
        )
        try:
            yield
        finally:
            app.state.realtime_stop.set()
            app.state.realtime_task.cancel()
            await asyncio.gather(app.state.realtime_task, return_exceptions=True)
            await app.state.realtime_consumer.stop()
            await app.state.realtime_producer.stop()
            await app.state.http.aclose()
            await app.state.database.dispose()

    app = FastAPI(title="Trading Platform API", version="1.0.0", lifespan=lifespan)
    origins = [
        value.strip() for value in os.getenv("TRADING_CORS_ORIGINS", "").split(",") if value.strip()
    ]
    if origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=False,
            allow_methods=["GET", "POST"],
            allow_headers=["Authorization", "Content-Type", "Idempotency-Key"],
        )

    @app.exception_handler(PlatformError)
    async def platform_error(request: Request, exc: PlatformError) -> JSONResponse:
        from trading_platform.common.ids import new_id

        return JSONResponse(
            status_code=exc.status_code, content=exc.response(new_id()).model_dump(mode="json")
        )

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "live", "service": "api"}

    @app.get("/health/ready")
    async def ready(request: Request, db: AsyncSession = Depends(session)) -> Any:
        dependencies: dict[str, str] = {}
        try:
            await db.execute(text("SELECT 1"))
            dependencies["database"] = "ready"
        except Exception:
            dependencies["database"] = "unavailable"
        checks = await asyncio.gather(
            *(
                request.app.state.http.get(f"{url}/health/ready")
                for url in request.app.state.service_urls.values()
            ),
            return_exceptions=True,
        )
        for name, check in zip(request.app.state.service_urls, checks, strict=True):
            dependencies[name] = (
                "ready"
                if isinstance(check, httpx.Response) and check.status_code == 200
                else "unavailable"
            )
        if "unavailable" in dependencies.values():
            return JSONResponse(
                status_code=503, content={"status": "not_ready", "dependencies": dependencies}
            )
        return {"status": "ready", "service": "api", "dependencies": dependencies}

    @app.post("/v1/auth/register", response_model=TokenResponse, status_code=201)
    async def register(
        command: RegisterRequest, db: AsyncSession = Depends(session)
    ) -> TokenResponse:
        user = User(email=command.email, password_hash=hash_password(command.password))
        db.add(user)
        try:
            await db.flush()
            account = Account(user_id=user.id)
            db.add(account)
            await db.flush()
            db.add(
                CashBalance(
                    account_id=account.id,
                    currency="USD",
                    total_amount=account.current_cash,
                    available_amount=account.current_cash,
                    reserved_amount=0,
                )
            )
            await db.commit()
        except IntegrityError as exc:
            await db.rollback()
            raise HTTPException(409, "email is already registered") from exc
        return _token(user.id, settings)

    @app.post("/v1/auth/login", response_model=TokenResponse)
    async def login(command: LoginRequest, db: AsyncSession = Depends(session)) -> TokenResponse:
        user = await db.scalar(select(User).where(User.email == command.email))
        if (
            user is None
            or not user.is_active
            or not verify_password(command.password, user.password_hash)
        ):
            raise HTTPException(
                401, "invalid email or password", headers={"WWW-Authenticate": "Bearer"}
            )
        return _token(user.id, settings)

    @app.get("/v1/auth/me", response_model=UserResponse)
    async def me(current: Identity = Depends(identity)) -> UserResponse:
        return UserResponse(
            user_id=current.user.id,
            account_id=current.account.id,
            email=current.user.email,
            base_currency=current.account.base_currency,
            cash=current.account.current_cash,
        )

    async def public_market(request: Request, path: str) -> Response:
        return await _proxy(request, "market", path)

    @app.get("/v1/instruments")
    async def instruments(request: Request) -> Response:
        return await public_market(request, "/internal/v1/instruments")

    @app.get("/v1/market/quotes")
    async def quotes(request: Request) -> Response:
        return await public_market(request, "/internal/v1/market/quotes")

    @app.get("/v1/market/quotes/{symbol}")
    async def quote(symbol: str, request: Request) -> Response:
        return await public_market(request, f"/internal/v1/market/quotes/{symbol}")

    @app.post("/v1/orders", status_code=202)
    async def create_order_proxy(
        request: Request, current: Identity = Depends(identity)
    ) -> Response:
        return await _proxy(request, "orders", "/internal/v1/orders")

    @app.get("/v1/orders")
    async def orders_proxy(request: Request, current: Identity = Depends(identity)) -> Response:
        return await _proxy(request, "orders", "/internal/v1/orders")

    @app.get("/v1/orders/{order_id}")
    async def order_proxy(
        order_id: UUID, request: Request, current: Identity = Depends(identity)
    ) -> Response:
        return await _proxy(request, "orders", f"/internal/v1/orders/{order_id}")

    @app.post("/v1/orders/{order_id}/cancel", status_code=202)
    async def cancel_proxy(
        order_id: UUID, request: Request, current: Identity = Depends(identity)
    ) -> Response:
        return await _proxy(request, "orders", f"/internal/v1/orders/{order_id}/cancel")

    @app.get("/v1/orders/{order_id}/events")
    async def events_proxy(
        order_id: UUID, request: Request, current: Identity = Depends(identity)
    ) -> Response:
        return await _proxy(request, "orders", f"/internal/v1/orders/{order_id}/events")

    @app.get("/v1/executions")
    async def executions_proxy(request: Request, current: Identity = Depends(identity)) -> Response:
        return await _proxy(request, "orders", "/internal/v1/executions")

    @app.get("/v1/executions/{execution_id}")
    async def execution_proxy(
        execution_id: UUID, request: Request, current: Identity = Depends(identity)
    ) -> Response:
        return await _proxy(request, "orders", f"/internal/v1/executions/{execution_id}")

    @app.get("/v1/portfolio")
    async def portfolio_proxy(request: Request, current: Identity = Depends(identity)) -> Response:
        return await _proxy(request, "portfolio", "/internal/v1/portfolio")

    @app.get("/v1/positions")
    async def positions_proxy(request: Request, current: Identity = Depends(identity)) -> Response:
        return await _proxy(request, "portfolio", "/internal/v1/positions")

    @app.get("/v1/positions/{symbol}")
    async def position_proxy(
        symbol: str, request: Request, current: Identity = Depends(identity)
    ) -> Response:
        return await _proxy(request, "portfolio", f"/internal/v1/positions/{symbol}")

    @app.websocket("/v1/ws")
    async def websocket(websocket: WebSocket) -> None:
        await websocket_endpoint(websocket)

    return app


async def _proxy(request: Request, service: str, path: str) -> Response:
    headers = {"accept": "application/json"}
    for name in ("authorization", "content-type", "idempotency-key", "traceparent"):
        if value := request.headers.get(name):
            headers[name] = value
    body = await request.body()
    if len(body) > 1_048_576:
        raise HTTPException(413, "request body too large")
    try:
        response = await request.app.state.http.request(
            request.method,
            request.app.state.service_urls[service] + path,
            params=request.query_params,
            headers=headers,
            content=body,
        )
    except httpx.RequestError as exc:
        raise HTTPException(503, f"{service} service unavailable") from exc
    excluded = {"content-encoding", "content-length", "transfer-encoding", "connection"}
    response_headers = {
        key: value for key, value in response.headers.items() if key.lower() not in excluded
    }
    return Response(
        content=response.content,
        status_code=response.status_code,
        headers=response_headers,
        media_type=response.headers.get("content-type"),
    )


def _token(user_id: UUID, settings: Any) -> TokenResponse:
    return TokenResponse(
        access_token=create_access_token(user_id, settings),
        expires_in=settings.jwt_access_token_minutes * 60,
    )


app = create_app()
