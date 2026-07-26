from http import HTTPStatus
from typing import Any
from uuid import UUID

from trading_platform.common.contracts import ErrorResponse


class PlatformError(Exception):
    code = "INTERNAL_ERROR"
    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
    retryable = False

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def response(self, request_id: UUID) -> ErrorResponse:
        return ErrorResponse(
            code=self.code,
            message=self.message,
            details=self.details,
            request_id=request_id,
        )


class ValidationError(PlatformError):
    code = "VALIDATION_ERROR"
    status_code = HTTPStatus.UNPROCESSABLE_ENTITY


class AuthenticationError(PlatformError):
    code = "AUTHENTICATION_REQUIRED"
    status_code = HTTPStatus.UNAUTHORIZED


class AuthorizationError(PlatformError):
    code = "FORBIDDEN"
    status_code = HTTPStatus.FORBIDDEN


class NotFoundError(PlatformError):
    code = "NOT_FOUND"
    status_code = HTTPStatus.NOT_FOUND


class ConflictError(PlatformError):
    code = "CONFLICT"
    status_code = HTTPStatus.CONFLICT


class IdempotencyConflictError(ConflictError):
    code = "IDEMPOTENCY_KEY_REUSED"


class DependencyUnavailableError(PlatformError):
    code = "DEPENDENCY_UNAVAILABLE"
    status_code = HTTPStatus.SERVICE_UNAVAILABLE
    retryable = True
