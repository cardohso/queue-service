"""Redis Streams as a rebuildable dispatch channel (DESIGN.md s2)."""

import logging
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import ResponseError

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Message:
    entry_id: str
    job_id: UUID | None  # None if the entry is malformed; it is acked and dropped
    epoch: int


class Broker(Protocol):
    async def ensure_group(self) -> None: ...
    async def read(self, consumer: str, count: int, block_ms: int) -> list[Message]: ...
    async def autoclaim(self, consumer: str, min_idle_ms: int, count: int) -> list[Message]: ...
    async def touch(self, consumer: str, entry_ids: list[str]) -> None: ...
    async def ack(self, entry_id: str) -> None: ...
    async def publish(self, job_id: UUID, epoch: int) -> str: ...
    async def publish_dead(self, job_id: UUID, epoch: int, reason: str) -> None: ...
    async def entries_exist(self, entry_ids: list[str]) -> dict[str, bool]: ...


def _parse(entry_id: Any, fields: Any) -> Message:
    try:
        return Message(str(entry_id), UUID(fields["job_id"]), int(fields["epoch"]))
    except KeyError, ValueError, TypeError:
        log.error("malformed_entry", extra={"entry_id": str(entry_id)})
        return Message(str(entry_id), None, 0)


class RedisBroker:
    """Requires a client created with ``decode_responses=True``."""

    def __init__(self, redis: Redis, stream: str, group: str, dead_stream: str) -> None:
        self._r = redis
        self._stream = stream
        self._group = group
        self._dead_stream = dead_stream
        self._autoclaim_cursor = "0-0"

    async def ensure_group(self) -> None:
        # id="0": after a Redis wipe, the group must also see entries the dispatcher published
        # *before* the group was recreated. "$" would silently skip them.
        try:
            await self._r.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def read(self, consumer: str, count: int, block_ms: int) -> list[Message]:
        try:
            resp: Any = await self._r.xreadgroup(
                self._group, consumer, {self._stream: ">"}, count=count, block=block_ms
            )
        except ResponseError as exc:
            if "NOGROUP" in str(exc):  # Redis lost its data (restart without persistence)
                log.warning("group_missing_recreating")
                await self.ensure_group()
                return []
            raise
        if not resp:
            return []
        return [_parse(eid, f) for _, entries in resp for eid, f in entries]

    async def autoclaim(self, consumer: str, min_idle_ms: int, count: int) -> list[Message]:
        """Take over entries idle longer than the lease: their worker is presumed dead."""
        try:
            resp: Any = await self._r.xautoclaim(
                self._stream,
                self._group,
                consumer,
                min_idle_time=min_idle_ms,
                start_id=self._autoclaim_cursor,
                count=count,
            )
        except ResponseError as exc:
            if "NOGROUP" in str(exc):
                await self.ensure_group()
                return []
            raise
        next_cursor, entries = resp[0], resp[1]
        self._autoclaim_cursor = str(next_cursor)
        return [_parse(eid, f) for eid, f in entries if f is not None]

    async def touch(self, consumer: str, entry_ids: list[str]) -> None:
        """Reset PEL idle time for entries we are still working on (heartbeat)."""
        await self._r.xclaim(
            self._stream,
            self._group,
            consumer,
            min_idle_time=0,
            message_ids=list[Any](entry_ids),
            justid=True,
        )

    async def ack(self, entry_id: str) -> None:
        # XACK + XDEL atomically: an entry that still exists is always unprocessed, which is
        # what lets the dispatcher use "entry exists" as "will still be delivered".
        pipe = self._r.pipeline(transaction=True)
        pipe.xack(self._stream, self._group, entry_id)
        pipe.xdel(self._stream, entry_id)
        await pipe.execute()

    async def publish(self, job_id: UUID, epoch: int) -> str:
        return str(await self._r.xadd(self._stream, {"job_id": str(job_id), "epoch": str(epoch)}))

    async def publish_dead(self, job_id: UUID, epoch: int, reason: str) -> None:
        # Observability copy only: the truth is jobs.state = 'dead'. Duplicates are possible.
        await self._r.xadd(
            self._dead_stream, {"job_id": str(job_id), "epoch": str(epoch), "reason": reason}
        )

    async def entries_exist(self, entry_ids: list[str]) -> dict[str, bool]:
        if not entry_ids:
            return {}
        pipe = self._r.pipeline(transaction=False)
        for eid in entry_ids:
            pipe.xrange(self._stream, min=eid, max=eid, count=1)
        found = await pipe.execute()
        return {eid: bool(f) for eid, f in zip(entry_ids, found, strict=True)}
