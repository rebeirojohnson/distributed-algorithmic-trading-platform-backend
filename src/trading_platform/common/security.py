from datetime import timedelta
from typing import Any
from uuid import UUID

import jwt
from passlib.context import CryptContext
from pydantic import BaseModel, ConfigDict, ValidationError

from trading_platform.common.errors import AuthenticationError
from trading_platform.common.ids import new_id
from trading_platform.common.settings import Settings
from trading_platform.common.time import utc_now

_passwords = CryptContext(schemes=["argon2"], deprecated="auto")


class AccessTokenClaims(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    sub: UUID
    iss: str
    aud: str | list[str]
    iat: int
    exp: int
    jti: UUID
    token_type: str


def normalize_email(email: str) -> str:
    normalized = email.strip().casefold()
    if not normalized or "@" not in normalized or len(normalized) > 320:
        raise ValueError("invalid email address")
    return normalized


def hash_password(password: str) -> str:
    if len(password) < 12:
        raise ValueError("password must contain at least 12 characters")
    if len(password.encode("utf-8")) > 1024:
        raise ValueError("password is too long")
    return str(_passwords.hash(password))


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bool(_passwords.verify(password, password_hash))
    except (ValueError, TypeError):
        return False


def create_access_token(user_id: UUID, settings: Settings) -> str:
    now = utc_now()
    payload: dict[str, Any] = {
        "sub": str(user_id),
        "iss": settings.jwt_issuer,
        "aud": settings.jwt_audience,
        "iat": now,
        "exp": now + timedelta(minutes=settings.jwt_access_token_minutes),
        "jti": str(new_id()),
        "token_type": "access",
    }
    return jwt.encode(payload, settings.jwt_secret.get_secret_value(), algorithm="HS256")


def decode_access_token(token: str, settings: Settings) -> AccessTokenClaims:
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret.get_secret_value(),
            algorithms=["HS256"],
            audience=settings.jwt_audience,
            issuer=settings.jwt_issuer,
            options={"require": ["sub", "iss", "aud", "iat", "exp", "jti", "token_type"]},
        )
        claims = AccessTokenClaims.model_validate(payload)
    except (jwt.PyJWTError, ValidationError, ValueError) as exc:
        raise AuthenticationError("invalid or expired access token") from exc
    if claims.token_type != "access":
        raise AuthenticationError("invalid token type")
    return claims
