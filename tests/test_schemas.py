from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from queue_service.schemas import IoSleepJob, JobRequest, request_hash

_adapter: TypeAdapter[Any] = TypeAdapter(JobRequest)


def _io(duration_ms: int = 10) -> dict[str, Any]:
    return {"type": "io_sleep", "schema_version": 1, "payload": {"duration_ms": duration_ms}}


def test_valid_request_parses_to_the_right_model() -> None:
    assert isinstance(_adapter.validate_python(_io()), IoSleepJob)


@pytest.mark.parametrize(
    "bad",
    [
        {"type": "nope", "schema_version": 1, "payload": {}},
        {**_io(), "schema_version": 2},  # unknown version is rejected at the edge
        {**_io(), "extra": 1},
        _io(-1),
        _io(60_001),
        {"type": "io_sleep", "schema_version": 1, "payload": {"duration_ms": 1, "x": 2}},
    ],
)
def test_invalid_requests_are_rejected(bad: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        _adapter.validate_python(bad)


def test_request_hash_ignores_key_order_and_detects_changes() -> None:
    a = _adapter.validate_python(_io(5))
    b = _adapter.validate_python(
        {"payload": {"duration_ms": 5}, "schema_version": 1, "type": "io_sleep"}
    )
    c = _adapter.validate_python(_io(6))
    assert request_hash(a) == request_hash(b)
    assert request_hash(a) != request_hash(c)
