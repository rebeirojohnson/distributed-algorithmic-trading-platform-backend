from uuid import uuid4

import pytest
from pydantic import SecretStr

from trading_platform.common.errors import AuthenticationError
from trading_platform.common.security import (
    create_access_token,
    decode_access_token,
    hash_password,
    normalize_email,
    verify_password,
)
from trading_platform.common.settings import Settings


def test_argon2_password_round_trip() -> None:
    password = "correct horse battery staple"
    password_hash = hash_password(password)

    assert password_hash.startswith("$argon2")
    assert verify_password(password, password_hash)
    assert not verify_password("incorrect password", password_hash)


def test_access_token_round_trip_and_wrong_secret_rejection() -> None:
    settings = Settings(
        environment="test",
        jwt_secret=SecretStr("unit-test-secret-with-at-least-32-characters"),
    )
    user_id = uuid4()
    token = create_access_token(user_id, settings)

    assert decode_access_token(token, settings).sub == user_id

    wrong_settings = settings.model_copy(
        update={"jwt_secret": SecretStr("different-unit-test-secret-at-least-32-chars")}
    )
    with pytest.raises(AuthenticationError):
        decode_access_token(token, wrong_settings)


def test_email_normalization() -> None:
    assert normalize_email("  Trader@Example.COM ") == "trader@example.com"
