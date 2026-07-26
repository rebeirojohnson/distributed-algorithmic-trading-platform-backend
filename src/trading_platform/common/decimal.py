from decimal import ROUND_HALF_UP, Decimal

MONEY_SCALE = Decimal("0.0001")
ZERO = Decimal("0.0000")


def quantize_money(value: Decimal | str | int) -> Decimal:
    return Decimal(value).quantize(MONEY_SCALE, rounding=ROUND_HALF_UP)


def round_to_tick(value: Decimal, tick_size: Decimal) -> Decimal:
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    return (value / tick_size).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick_size
