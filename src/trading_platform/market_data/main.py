# ruff: noqa: B008
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict
from redis.asyncio import Redis

from trading_platform.common.contracts import MARKET_QUOTES_TOPIC, EventEnvelope, QuoteData
from trading_platform.common.kafka import KafkaJsonProducer
from trading_platform.common.settings import get_settings

from .domain import INSTRUMENTS, Quote, QuoteGenerator, normalize_symbol
from .snapshots import QuoteSnapshotStore


class QuoteResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    symbol: str
    sequence: int
    bid: str
    ask: str
    last: str
    bid_size: int
    ask_size: int
    timestamp: datetime


class MemoryMarket:
    """Useful standalone store; production lifespan may replace it with Redis/Kafka adapters."""

    def __init__(self) -> None:
        self.generator = QuoteGenerator()
        self.quotes = {quote.symbol: quote for quote in self.generator.next_all()}

    def tick(self) -> list[Quote]:
        quotes = self.generator.next_all()
        self.quotes.update((quote.symbol, quote) for quote in quotes)
        return quotes


def _quote_response(quote: Quote) -> QuoteResponse:
    return QuoteResponse(
        symbol=quote.symbol,
        sequence=quote.sequence,
        bid=str(quote.bid),
        ask=str(quote.ask),
        last=str(quote.last),
        bid_size=quote.bid_size,
        ask_size=quote.ask_size,
        timestamp=quote.timestamp,
    )


def create_app() -> FastAPI:
    settings = get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
        app.state.market = MemoryMarket()
        app.state.redis = Redis.from_url(settings.redis_url, decode_responses=False)
        app.state.snapshots = QuoteSnapshotStore(
            app.state.redis, ttl_seconds=settings.quote_ttl_seconds
        )
        app.state.publisher = KafkaJsonProducer(settings)
        await app.state.publisher.start()
        app.state.stop = asyncio.Event()
        app.state.quote_task = asyncio.create_task(
            _quote_runtime(app, settings.quote_interval_seconds)
        )
        try:
            yield
        finally:
            app.state.stop.set()
            app.state.quote_task.cancel()
            with suppress(asyncio.CancelledError):
                await app.state.quote_task
            await app.state.publisher.stop()
            await app.state.redis.aclose()

    app = FastAPI(title="Trading Platform Market Data", version="1.0.0", lifespan=lifespan)

    @app.get("/health/live", tags=["health"])
    async def live() -> dict[str, str]:
        return {"status": "live", "service": "market-data"}

    @app.get("/health/ready", tags=["health"])
    async def ready(request: Request) -> dict[str, str]:
        if not hasattr(request.app.state, "market"):
            raise HTTPException(503, "market data store is not initialized")
        try:
            await request.app.state.redis.ping()
        except Exception as exc:
            raise HTTPException(503, "redis unavailable") from exc
        return {"status": "ready", "service": "market-data"}

    @app.get("/internal/v1/instruments")
    async def instruments() -> list[dict[str, Any]]:
        return [
            {
                "symbol": item.symbol,
                "display_name": item.name,
                "currency": item.currency,
                "tick_size": str(item.tick_size),
                "active": True,
            }
            for item in INSTRUMENTS
        ]

    @app.get("/internal/v1/market/quotes", response_model=list[QuoteResponse])
    async def quotes(
        request: Request,
        symbols: list[str] | None = Query(default=None),
    ) -> list[QuoteResponse]:
        market: MemoryMarket = request.app.state.market
        selected = list(market.quotes) if not symbols else symbols
        try:
            normalized = [normalize_symbol(symbol) for symbol in selected]
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc
        return [_quote_response(market.quotes[symbol]) for symbol in normalized]

    @app.get("/internal/v1/market/quotes/{symbol}", response_model=QuoteResponse)
    async def quote(symbol: str, request: Request) -> QuoteResponse:
        try:
            normalized = normalize_symbol(symbol)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc
        market: MemoryMarket = request.app.state.market
        return _quote_response(market.quotes[normalized])

    @app.post(
        "/internal/v1/market/tick", response_model=list[QuoteResponse], include_in_schema=False
    )
    async def tick(request: Request) -> list[QuoteResponse]:
        market: MemoryMarket = request.app.state.market
        return [_quote_response(quote) for quote in market.tick()]

    return app


async def _quote_runtime(app: FastAPI, interval: float) -> None:
    market: MemoryMarket = app.state.market
    while not app.state.stop.is_set():
        for quote in market.tick():
            await app.state.snapshots.put(quote)
            envelope = EventEnvelope[QuoteData](
                event_type="market.quote.generated",
                producer="market-data",
                data=QuoteData(
                    symbol=quote.symbol,
                    bid=quote.bid,
                    ask=quote.ask,
                    last=quote.last,
                    bid_size=quote.bid_size,
                    ask_size=quote.ask_size,
                    sequence=quote.sequence,
                    quoted_at=quote.timestamp,
                ),
            )
            await app.state.publisher.publish(
                MARKET_QUOTES_TOPIC,
                quote.symbol,
                envelope.model_dump(mode="json"),
            )
        try:
            await asyncio.wait_for(app.state.stop.wait(), timeout=interval)
        except TimeoutError:
            continue


app = create_app()
