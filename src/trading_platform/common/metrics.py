from prometheus_client import Counter, Gauge, Histogram

HTTP_REQUEST_DURATION = Histogram(
    "trading_http_request_duration_seconds",
    "HTTP request duration",
    ("service", "method", "route", "status"),
)
HTTP_ERRORS = Counter("trading_http_errors_total", "HTTP errors", ("service", "route", "status"))
ACTIVE_WEBSOCKETS = Gauge(
    "trading_active_websockets", "Currently active WebSocket connections", ("service",)
)
QUOTES_GENERATED = Counter(
    "trading_quotes_generated_total", "Generated quotes", ("service", "symbol")
)
STALE_QUOTES = Counter("trading_stale_quotes_total", "Stale quote observations", ("service",))
KAFKA_MESSAGES = Counter(
    "trading_kafka_messages_total", "Kafka messages processed", ("service", "topic", "result")
)
KAFKA_CONSUMER_LAG = Gauge(
    "trading_kafka_consumer_lag", "Kafka consumer lag", ("service", "topic", "partition")
)
ORDER_TRANSITIONS = Counter(
    "trading_order_transitions_total",
    "Order state transitions",
    ("service", "from_status", "to_status"),
)
EXECUTIONS = Counter(
    "trading_executions_total", "Executions persisted", ("service", "symbol", "side")
)
OUTBOX_BACKLOG = Gauge("trading_outbox_backlog", "Pending outbox messages", ("service",))
OUTBOX_FAILURES = Counter(
    "trading_outbox_failures_total", "Outbox publish failures", ("service", "topic")
)
DLQ_WRITES = Counter("trading_dlq_writes_total", "Dead-letter writes", ("service", "topic"))
