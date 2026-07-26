# Distributed Algorithmic Trading Platform Backend

A Dockerized paper-trading backend for 15 deterministic US equity instruments. Four independently runnable Python 3.12 services provide the REST/WebSocket gateway, market data, order execution, and portfolio/risk processing. PostgreSQL is authoritative state, Redis holds latest quotes, and Kafka carries versioned events.

## Prerequisites

- Docker with Compose v2 for the complete stack
- Python 3.12 and [uv](https://docs.astral.sh/uv/) for host development

## Run With Docker

No external credentials are needed for local development:

```bash
docker compose up --build
```

Only the gateway is published, at <http://localhost:8000>. Compose waits for PostgreSQL, Redis, and single-node Kafka KRaft health checks, applies Alembic migrations once, and then starts all four services. The database and Kafka log use named volumes.

Useful commands:

```bash
docker compose ps
docker compose logs -f gateway market-data orders portfolio
docker compose run --rm migrate
docker compose down
docker compose down --volumes  # also deletes all local trading state
```

Copy `.env.example` to `.env` only when overriding defaults. Local credentials are intentionally unsafe for deployment. Production must use managed secrets, a unique JWT secret of at least 32 characters, and production infrastructure endpoints.

## Develop With uv

Install the locked runtime and development environment:

```bash
uv sync --frozen
```

Start infrastructure for host-run services if its ports are explicitly published in a local Compose override, set the corresponding `TRADING_*` URLs from `.env.example`, then run migrations and services:

```bash
uv run alembic upgrade head
uv run uvicorn trading_platform.market_data.main:app --port 8001
uv run uvicorn trading_platform.orders.main:app --port 8002
uv run uvicorn trading_platform.portfolio.main:app --port 8003
uv run uvicorn trading_platform.api.main:app --port 8000
```

The checked-in Compose configuration deliberately does not publish infrastructure or internal service ports. For the least surprising local workflow, run the complete stack in Docker.

Quality checks:

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest tests/unit
uv run pytest tests/integration
uv run pytest tests/e2e
```

Integration and end-to-end suites require real infrastructure and must skip when it is unavailable; they never replace infrastructure checks with fake passing assertions.

## API Demo

Wait for `docker compose ps` to report all application services healthy. Register a user and capture the token with `jq`:

```bash
curl -sS -X POST http://localhost:8000/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"demo@example.com","password":"correct-horse-battery-staple"}'

TOKEN=$(curl -sS -X POST http://localhost:8000/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"demo@example.com","password":"correct-horse-battery-staple"}' \
  | jq -r .access_token)

curl -sS http://localhost:8000/v1/market/quotes/AAPL \
  -H "Authorization: Bearer $TOKEN"

curl -sS -X POST http://localhost:8000/v1/orders \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: demo-buy-aapl-1" \
  -H "Content-Type: application/json" \
  -d '{"symbol":"AAPL","side":"BUY","type":"MARKET","time_in_force":"DAY","quantity":2}'

curl -sS http://localhost:8000/v1/portfolio \
  -H "Authorization: Bearer $TOKEN"
```

The quote generator is seeded per symbol: identical seed and configuration produce the same sequence after a clean restart. Existing Docker volumes preserve durable trading state; `docker compose down --volumes` resets it.

## WebSocket Demo

Connect with [websocat](https://github.com/vi/websocat):

```bash
websocat ws://localhost:8000/v1/ws
```

Send these messages after connecting, replacing `<JWT>` with the token above:

```json
{"type":"authenticate","token":"<JWT>"}
{"type":"subscribe","channels":["quotes:AAPL","orders","executions","portfolio"]}
```

Use REST to fetch an authoritative snapshot before subscribing. Live messages contain event IDs that clients can use for deduplication; durable WebSocket replay is outside this MVP.

## Architecture

| Service | Responsibility | Public port |
| --- | --- | --- |
| `gateway` | Authenticated REST proxy and WebSocket fan-out | `8000` |
| `market-data` | Deterministic quotes, Redis snapshots, Kafka publication | None |
| `orders` | Order state machine, matching, fills, and audit history | None |
| `portfolio` | Risk reservations, cash, positions, and P&L projection | None |

All Kafka processing is at-least-once. Transactional outboxes, unique event identifiers, processed-event records, and database locking provide idempotent business effects.
