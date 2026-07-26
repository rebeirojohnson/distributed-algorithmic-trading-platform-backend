# Distributed Trading Platform Backend Plan

## Goal

Build a greenfield, Dockerized paper-trading backend for 15 seeded US equities. The system must provide local JWT users, deterministic real-time quotes, advanced paper orders, asynchronous execution, cash/position tracking, trade history, authenticated WebSocket updates, and reliable Kafka processing across four independently runnable Python services.

## Confirmed Scope

- Python 3.12, FastAPI, Pydantic v2, async SQLAlchemy 2, Alembic, PostgreSQL, Redis, Kafka, and Docker Compose.
- One monorepo and one PostgreSQL instance, with explicit service-owned tables and shared infrastructure packages.
- Four processes: API/realtime gateway, market-data service, order/execution service, and portfolio/risk service.
- One USD paper account per locally registered user, initialized with `$100,000.00`.
- Fifteen fixed, seeded US equity instruments (for example AAPL, MSFT, GOOGL, AMZN, META, NVDA, TSLA, JPM, V, JNJ, WMT, PG, XOM, KO, and DIS).
- Whole-share, long-only cash trading with zero fees, no leverage, and average-cost position accounting.
- BUY and SELL orders of type MARKET, LIMIT, STOP, and STOP_LIMIT; DAY and GTC duration; partial fills and cancellation.
- Deterministic local quote generation, requiring no external credentials. The simulator is always open; a simulated DAY is 24 hours from acceptance.
- Versioned JSON/Pydantic event contracts. A schema registry and Avro/Protobuf are deferred.
- Strategy execution, live brokers, external market-data providers, fractional shares, margin, shorting, options, multi-currency, amend/replace, bracket/OCO orders, fees, corporate actions, and Kubernetes are out of scope.

## Repository Structure

1. Create a `uv`-managed `pyproject.toml` with runtime and development dependency groups, Ruff formatting/linting, mypy strict-enough settings, pytest/asyncio configuration, and a lockfile.
2. Use a single installable package under `src/trading_platform/`:
   - `common/`: settings, IDs, decimal/time helpers, database sessions, Kafka clients/contracts, outbox, retry policy, JWT/password handling, errors, and structured logging.
   - `api/`: public REST gateway, auth endpoints, internal-service clients, WebSocket connection/subscription manager, and realtime Kafka consumers.
   - `market_data/`: seeded instruments, deterministic quote generator, Redis latest-quote cache, quote producer, and internal query routes.
   - `orders/`: order models/state machine, idempotency, validation-result and quote consumers, matching/execution engine, cancellation/expiry jobs, outbox publisher, and internal routes.
   - `portfolio/`: account/cash/position/reservation models, validation and execution consumers, projections, outbox publisher, and internal query routes.
3. Add `migrations/` with one Alembic environment and ordered revisions, `tests/{unit,integration,e2e}/`, a shared Dockerfile, `compose.yaml`, `.env.example`, and GitHub Actions CI.
4. Keep each service independently executable with its own FastAPI lifespan, `/health/live`, and dependency-aware `/health/ready` endpoints. Background loops must stop cleanly on shutdown.

## Core Contracts

1. Define UUID identifiers, timezone-aware UTC timestamps, and `Decimal` values serialized as strings. Persist price/cash as `NUMERIC(20, 4)` and quantity as positive integer; never use floating point for financial values.
2. Define a common event envelope containing `event_id`, `event_type`, `schema_version`, `occurred_at`, `producer`, `correlation_id`, `causation_id`, optional `account_id`/`order_id`, and typed `data`.
3. Define stable error responses containing `code`, `message`, `details`, and `request_id`.
4. Add Pydantic contracts and compatibility tests for these Kafka topics:
   - `market.quotes.v1`, partitioned by `symbol`.
   - `risk.validation.requested.v1` and `risk.validation.completed.v1`, partitioned by `order_id`.
   - `orders.events.v1` and `executions.v1`, partitioned by `order_id`.
   - `portfolio.events.v1`, partitioned by `account_id`.
   - A `<source-topic>.dlq` topic for exhausted/permanent consumer failures.
5. Treat Kafka as at-least-once. Business-level exactly-once effects come from event IDs, unique database constraints, row locks, and transactional updates rather than Kafka delivery assumptions.

## Data Model And Migrations

1. Auth/account tables:
   - `users`: unique normalized email, Argon2 password hash, active flag, timestamps.
   - `accounts`: user ownership, USD base currency, initial/current cash fields, timestamps, and one-account-per-user constraint.
2. Reference/market tables:
   - `instruments`: unique symbol, display name, currency, tick size, active flag, and deterministic initial price/seed.
   - Do not persist every generated tick in PostgreSQL in this MVP; Kafka is the event stream and Redis stores latest quotes.
3. Order tables:
   - `orders`: account, symbol, side, type, time-in-force, quantity, filled quantity, limit/stop prices, triggered flag, current status, average fill price, expiry, optimistic version, and timestamps.
   - `order_events`: immutable status/audit records with unique event ID and order version.
   - `executions`: immutable fills with unique execution ID, order, quantity, price, and timestamp.
   - `idempotency_records`: account, operation, key, canonical request hash, response status/body, and unique `(account_id, operation, key)` constraint.
4. Portfolio tables:
   - `cash_balances`, `positions` (quantity, average cost, realized P&L), and `reservations` (cash or shares, remaining amount, order, state).
   - `portfolio_processed_events` (and equivalent per consuming service) with unique `(consumer_name, event_id)`.
5. Reliability table:
   - `outbox_events`: event envelope/payload, topic/key, status, attempts, `next_attempt_at`, published timestamp, and indexes supporting `FOR UPDATE SKIP LOCKED` batches.
6. Add foreign keys, check constraints for positive quantities/nonnegative fills, uniqueness for immutable events/executions, and indexes for account/status/symbol/time queries. Migrations seed the 15 instruments but users/accounts are created only through registration.

## Authentication And Public API

1. Implement local registration and login using normalized email, Argon2 hashes, signed short-lived JWT access tokens, configurable issuer/audience/secret, and a `sub` user ID. Do not add refresh tokens in the MVP.
2. Make the API/realtime service the only publicly exposed application port. It validates JWTs, resolves the user's account, applies account isolation, and calls the other services over the Compose network. Internal services also validate forwarded JWTs rather than trusting caller-supplied account headers.
3. Add public endpoints:
   - `POST /v1/auth/register`, `POST /v1/auth/login`, and `GET /v1/auth/me`.
   - `GET /v1/instruments`, `GET /v1/market/quotes`, and `GET /v1/market/quotes/{symbol}`.
   - `POST /v1/orders`, `GET /v1/orders`, `GET /v1/orders/{id}`, `POST /v1/orders/{id}/cancel`, and `GET /v1/orders/{id}/events`.
   - `GET /v1/executions` and `GET /v1/executions/{id}` as persistent trade/fill history.
   - `GET /v1/portfolio`, `GET /v1/positions`, and `GET /v1/positions/{symbol}` with an `as_of` timestamp so eventual projection lag is explicit.
4. Require `Idempotency-Key` for order creation and cancellation. Bind it to account, operation, path, and canonical request hash; return the stored response for an identical retry and `409` when a key is reused with a different request.
5. Support cursor pagination and bounded filters for list endpoints. Return `202 Accepted` for order/cancel commands because lifecycle completion is asynchronous.
6. Add request-size limits, configurable CORS allowlist, safe pagination defaults, and account ownership checks on every identifier lookup.

## Order Lifecycle And Execution

1. Implement and exhaustively test this state machine: `PENDING_VALIDATION -> ACCEPTED -> ACTIVE -> PARTIALLY_FILLED -> FILLED`; rejection to `REJECTED`; active cancellation through `CANCEL_PENDING -> CANCELLED`; DAY timeout to `EXPIRED`; unrecoverable internal failure to `FAILED`.
2. Terminal states cannot return to active states. Cancellation is idempotent. A partially filled cancellation retains fills and cancels only remaining quantity. Lock the order row and check its optimistic version for every transition so quote, cancel, and expiry races serialize safely.
3. Submission transaction:
   - Validate request shape and instrument, create/replay the idempotency record, create `PENDING_VALIDATION` order and immutable order event, and enqueue `risk.validation.requested.v1` in the outbox.
   - Return the stable order ID and current status without waiting for Kafka processing.
4. Portfolio/risk validation transaction:
   - Lock cash/position rows, reject insufficient funds or shares, otherwise create a durable reservation.
   - BUY LIMIT/STOP_LIMIT reserves `limit_price * quantity`; BUY MARKET reserves current ask times quantity; BUY STOP reserves `max(current ask, stop price) * 1.05 * quantity`; SELL reserves quantity of shares.
   - Emit a validation result through its outbox. A zero/missing/stale current quote is a retryable validation failure, then a clear rejection if freshness cannot be restored within the bounded policy.
5. On accepted validation, move the order through `ACCEPTED` to `ACTIVE`. Consume quotes in the order service and apply deterministic rules:
   - MARKET executes at current ask for BUY and bid for SELL.
   - LIMIT executes when BUY ask is at or below limit or SELL bid is at or above limit.
   - STOP triggers when BUY ask is at or above stop or SELL bid is at or below stop, then behaves as MARKET.
   - STOP_LIMIT records a durable trigger and then behaves as LIMIT.
   - Fill no more than the quote's deterministic available side size; later ticks fill the remainder.
6. In one order-service transaction, persist each fill, update filled quantity/weighted average price/status, append order events, and enqueue both execution and order events. Ensure a deterministic unique execution ID prevents duplicate fills when a quote event is replayed.
7. If a triggered market-style fill would exceed its reservation, fill only the affordable whole-share quantity; if none is affordable, terminate the unfilled remainder as `REJECTED` before execution or `CANCELLED` after a prior partial fill, with a specific reason. Never permit negative cash or positions.
8. Run a periodic expiry job that expires active DAY orders at `accepted_at + 24 hours`. Terminal order events cause the portfolio service to release remaining reservations; fills settle reservation amounts and balances before release.

## Portfolio Projection

1. Consume executions in order per order ID, insert the processed-event marker in the same transaction as all financial changes, and ignore exact replays.
2. BUY fills reduce reserved/available cash, increase position quantity, and recompute weighted average cost. SELL fills reduce reserved shares/position quantity, increase cash, and add `(fill price - average cost) * quantity` to realized P&L; remove zero-quantity positions after preserving audit through executions.
3. Compute unrealized P&L at read time from Redis latest quotes and persisted average cost. Return a staleness marker if any quote is older than the configured threshold.
4. Emit account-scoped `portfolio.events.v1` updates from an outbox after each committed projection change so realtime clients see cash and position changes.

## Deterministic Market Data

1. Seed each symbol with a fixed initial midpoint and pseudo-random seed. Generate one quote per symbol per configured interval using a bounded random walk; derive tick-size-rounded bid/ask, last price, and deterministic bid/ask sizes.
2. Restarting from the same seed/configuration must reproduce the sequence. Allow a faster interval in tests and expose configuration for the normal demo rate.
3. Store each latest quote in Redis with a timestamp and TTL, publish it to Kafka, and expose snapshot routes from the market-data service. PostgreSQL remains the source for instrument metadata; Redis is never authoritative for orders or portfolios.
4. Log/metric quote staleness and reconnect to Redis/Kafka with bounded exponential backoff and jitter.

## WebSocket Realtime

1. Expose `/v1/ws`. Require an `authenticate` message containing the JWT within a short timeout, then accept `subscribe`/`unsubscribe` messages for account-owned `orders`, `executions`, `portfolio`, and selected `quotes:<symbol>` channels.
2. Consume the Kafka event topics in the API service. Use a unique realtime consumer group per API instance so every instance receives all events for its local connections; filter account-scoped events before delivery.
3. Send versioned messages containing event ID, type, timestamp, channel, and data. Enforce allowed symbols, subscription count, message size, heartbeat/ping, bounded per-connection queues, and disconnect slow consumers rather than allowing unbounded memory growth.
4. On connection/reconnection, clients obtain authoritative snapshots through REST and then subscribe to live updates. Durable WebSocket replay is explicitly deferred; event IDs allow clients to deduplicate live duplicates.

## Reliability And Observability

1. Implement a reusable transactional outbox worker. Claim due rows with `FOR UPDATE SKIP LOCKED`, publish, and mark them delivered only after Kafka acknowledgement. If a crash occurs after publish and before marking, consumers safely deduplicate the repeated event.
2. Retry transient database, Redis, Kafka, and internal HTTP failures with bounded exponential backoff and jitter. Never retry validation/authentication errors. Track outbox attempts/next-attempt time; after the configured maximum, leave the row inspectable and report unhealthy/metric failure.
3. For Kafka handlers, commit offsets only after the database transaction commits. Retry transient handler failures a bounded number of times; publish exhausted or permanent failures with original topic/partition/offset/error metadata to the topic DLQ, then commit the poison record.
4. Configure JSON structured logs with service, environment, request/trace/correlation IDs, account/order/event IDs, Kafka topic/partition/offset, status, and duration. Redact passwords, JWTs, authorization headers, and full idempotency keys.
5. Add Prometheus-compatible metrics for HTTP latency/errors, active WebSockets, generated/stale quotes, Kafka processing/errors/lag, order transitions, executions, outbox backlog/failures, and DLQ writes. Include OpenTelemetry-ready trace propagation in HTTP headers and event envelopes; external collector deployment is out of scope.
6. Add readiness checks for each service's required PostgreSQL/Redis/Kafka/internal dependencies. Compose uses health checks and dependency conditions rather than startup ordering alone.

## Containers And Developer Workflow

1. Build one non-root application image and select each service's module/command in Compose. Include PostgreSQL, Redis, single-node Kafka in KRaft mode, a one-shot migration container, and the four application containers.
2. Publish only the gateway port by default; keep databases, Kafka, Redis, and internal service ports on the Compose network. Use named volumes for durable local PostgreSQL/Kafka state.
3. Provide `.env.example` with safe local defaults and explicit production overrides. Fail startup for missing/unsafe JWT secrets outside development.
4. Document commands in the existing README for `uv sync`, migration, local service startup, full Compose startup, seed behavior, tests, lint/type checks, and a curl/WebSocket demo flow.
5. Add CI stages for Ruff, mypy, unit tests, migration upgrade from an empty PostgreSQL database, integration tests with real PostgreSQL/Redis/Kafka, and Docker image build.

## Validation Plan

1. Unit tests:
   - Every allowed/forbidden state transition, terminal-state immutability, cancel/expiry races, stop triggering, limit matching, partial fills, and weighted fill price.
   - Request validation, canonical idempotency hashing, event serialization, quote determinism, retry classification, reservation calculations, average-cost accounting, and realized/unrealized P&L.
2. Integration tests with real infrastructure (Testcontainers or the Compose test profile):
   - Upgrade migrations from empty database and enforce all financial/uniqueness constraints.
   - Concurrent identical order submissions converge on one order; changed payload with the same key returns `409`.
   - Order/outbox atomicity, outbox publish replay, consumer replay, and duplicate execution events do not duplicate orders, fills, cash changes, or positions.
   - JWT account isolation prevents reading/cancelling another user's resources.
   - Redis quote expiry is surfaced as stale/unavailable, not silently accepted.
3. End-to-end scenarios:
   - Register/login, observe quotes, submit BUY MARKET, receive order/execution/portfolio WebSocket events, and verify cash/position/trade history.
   - Submit a LIMIT order away from market, observe it remain active, cancel it twice with the same idempotency key, and verify reservation release.
   - Exercise STOP and STOP_LIMIT triggers by advancing deterministic prices.
   - Submit an order larger than quote liquidity, observe partial fills over several ticks, then fill or cancel the remainder.
   - Reject insufficient cash and insufficient shares without negative balances.
   - Replay Kafka events and restart order/portfolio services without duplicate business effects.
   - Stop Redis/Kafka temporarily, verify bounded retries/readiness changes, restore them, and verify recovery plus no lost outbox events.
4. Performance smoke test a sustained quote stream for all 15 symbols plus concurrent order submissions and WebSocket clients. Assert bounded queues, no duplicate fills, stable memory, and measured API/order-processing latency; do not claim a numeric low-latency target unless the test environment and threshold are explicitly recorded.

## Implementation Order

1. Scaffold tooling, package structure, settings, logging, Compose dependencies, health endpoints, and CI.
2. Add common contracts, database base/session handling, initial migrations, seeded instruments, auth, and account creation.
3. Implement deterministic market-data generation, Redis snapshots, Kafka quotes, and market REST proxying.
4. Implement order schemas, state machine, persistence, idempotent create/read/cancel routes, and immutable audit history.
5. Implement portfolio reservations and validation events, then order validation-result handling.
6. Implement quote matching, fills, executions, expiry, reservation settlement/release, and portfolio projections.
7. Add transactional outboxes, processed-event deduplication, retries, DLQs, and failure metrics to every event path before treating the workflow as complete.
8. Add authenticated WebSocket subscriptions and account/symbol filtering.
9. Complete integration/end-to-end/failure tests, performance smoke test, API examples, and operational documentation.

## Completion Criteria

- A clean checkout reaches a healthy stack with one documented Docker Compose command and no external credentials.
- All four services run independently and communicate through documented HTTP and Kafka contracts.
- Every confirmed order type follows the specified lifecycle, survives duplicate/replayed events, and preserves financial invariants.
- PostgreSQL contains durable users, accounts, orders, events, executions, reservations, positions, cash, idempotency records, processed-event markers, and outboxes; Redis loss cannot lose authoritative trading state.
- Authenticated REST and WebSocket clients are isolated by account and receive correct quote, order, execution, and portfolio data.
- Lint, typing, unit, integration, end-to-end, migration, and image-build checks pass in CI.
