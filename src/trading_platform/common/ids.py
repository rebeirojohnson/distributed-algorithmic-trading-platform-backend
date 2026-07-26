from uuid import UUID, uuid4


def new_id() -> UUID:
    return uuid4()


def parse_id(value: str | UUID) -> UUID:
    return value if isinstance(value, UUID) else UUID(value)
