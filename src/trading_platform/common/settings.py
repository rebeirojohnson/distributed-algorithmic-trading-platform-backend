from functools import lru_cache
from typing import Annotated, Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="TRADING_",
        case_sensitive=False,
        extra="ignore",
    )

    service_name: str = "trading-platform"
    environment: Literal["development", "test", "staging", "production"] = "development"
    log_level: str = "INFO"
    database_url: str = "postgresql+asyncpg://trading:trading@localhost:5432/trading"
    database_pool_size: int = Field(default=10, ge=1, le=100)
    database_max_overflow: int = Field(default=20, ge=0, le=200)
    redis_url: str = "redis://localhost:6379/0"
    quote_interval_seconds: float = Field(default=1.0, gt=0, le=60)
    quote_ttl_seconds: int = Field(default=30, ge=2, le=3600)
    quote_stale_seconds: int = Field(default=10, ge=1, le=3600)
    kafka_bootstrap_servers: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["localhost:9092"]
    )
    kafka_client_id: str = "trading-platform"
    kafka_security_protocol: str = "PLAINTEXT"
    jwt_secret: SecretStr = SecretStr("development-only-change-me-32chars")
    jwt_issuer: str = "trading-platform"
    jwt_audience: str = "trading-platform-api"
    jwt_access_token_minutes: int = Field(default=15, ge=1, le=1440)
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_poll_interval_seconds: float = Field(default=0.5, gt=0, le=60)
    outbox_max_attempts: int = Field(default=10, ge=1, le=100)
    retry_base_delay_seconds: float = Field(default=0.1, gt=0, le=60)
    retry_max_delay_seconds: float = Field(default=10, gt=0, le=300)

    @field_validator("kafka_bootstrap_servers", mode="before")
    @classmethod
    def parse_servers(cls, value: object) -> object:
        if isinstance(value, str) and not value.lstrip().startswith("["):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @model_validator(mode="after")
    def reject_unsafe_production_secret(self) -> "Settings":
        if self.environment != "development" and self.environment != "test":
            secret = self.jwt_secret.get_secret_value()
            if secret == "development-only-change-me-32chars" or len(secret) < 32:
                raise ValueError("TRADING_JWT_SECRET must be a strong secret outside development")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
