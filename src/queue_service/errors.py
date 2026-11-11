"""Error classification for retries (DESIGN.md s6)."""

from typing import Literal

import asyncpg
from pydantic import ValidationError


class TransientError(Exception):
    """Worth retrying: the same job may succeed later."""


class PermanentError(Exception):
    """Retrying cannot help: dead-letter immediately."""


ErrorClass = Literal["transient", "permanent"]


def classify(exc: BaseException) -> ErrorClass:
    # Unknown exceptions are transient (decided 2026-11-11): a bug costs a few bounded retries
    # rather than silently dead-lettering good jobs, and the DLQ keeps the exception type.
    if isinstance(exc, PermanentError | ValidationError):
        return "permanent"
    return "transient"


# Infrastructure failures. These never count against a job's attempt budget: the worker
# pauses instead (DESIGN.md s6). Constraint violations are deliberately NOT here: they mean
# a bug or a broken invariant, and must surface loudly.
DB_INFRA_ERRORS: tuple[type[BaseException], ...] = (
    OSError,
    TimeoutError,
    asyncpg.InterfaceError,
    asyncpg.PostgresConnectionError,
    asyncpg.exceptions.OperatorInterventionError,  # e.g. admin shutdown, cannot connect now
)
