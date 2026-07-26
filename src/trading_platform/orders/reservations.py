from __future__ import annotations

from decimal import Decimal

from trading_platform.common.enums import OrderSide, OrderType

STOP_BUFFER = Decimal("1.05")


def reservation_required(
    *,
    side: OrderSide,
    order_type: OrderType,
    quantity: int,
    current_ask: Decimal | None = None,
    limit_price: Decimal | None = None,
    stop_price: Decimal | None = None,
) -> Decimal | int:
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    if side == OrderSide.SELL:
        return quantity
    if order_type in {OrderType.LIMIT, OrderType.STOP_LIMIT}:
        price = limit_price
    elif order_type == OrderType.MARKET:
        price = current_ask
    else:
        if current_ask is None or stop_price is None:
            raise ValueError("current_ask and stop_price are required for a stop buy")
        price = max(current_ask, stop_price) * STOP_BUFFER
    if price is None or price <= 0:
        raise ValueError("a positive reservation price is required")
    return price * quantity
