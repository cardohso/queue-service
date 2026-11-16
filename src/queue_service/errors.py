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
    # SQLSTATE class 53: "too many clients already", out of memory/disk. Found by a failure
    # test: unclassified, it crashed handlers and start-up instead of pausing and backing off.
    asyncpg.exceptions.InsufficientResourcesError,
    # "cannot switch to state N; another operation is in progress": the client library's state
    # error when a connection is killed mid-operation (failover, pg_terminate_backend, a proxy
    # reset). Not a PostgresError or InterfaceError, so it escaped as an HTTP 500 and as
    # handler_crashed; a flaky failure test showed it. The pool discards such a connection.
    asyncpg.exceptions.InternalClientError,
)

# Everything that can go wrong talking to Postgres and is worth waiting out or reporting as 503.
# Integrity violations are PostgresErrors too, but a bug there must not be retried forever;
# API handlers want both, workers only DB_INFRA_ERRORS.
DB_ERRORS: tuple[type[BaseException], ...] = (asyncpg.PostgresError, *DB_INFRA_ERRORS)
