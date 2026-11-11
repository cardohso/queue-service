"""Run "suspect" jobs alone, so a job that crashes the worker cannot take others with it.

A job is a suspect when its previous attempt ended in a crash (its lease expired and another
worker must take it over). The crash cannot tell us *which* of the jobs running at the time
caused it, so every survivor is re-run in isolation: the next crash, if any, has exactly one
possible culprit, and innocent jobs are charged at most the one crash they happened to share.

Implementation: a writer-preferring readers/writer lock.
  * normal jobs hold it shared (many at once, up to max_in_flight)
  * suspect jobs hold it exclusively (alone)
  * once a suspect is waiting, no new normal job may start, so it cannot be starved
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


class IsolationLock:
    def __init__(self) -> None:
        self._cond = asyncio.Condition()
        self._shared = 0
        self._exclusive = False
        self._waiting_exclusive = 0

    @property
    def isolating(self) -> bool:
        """True while a suspect runs or waits to run: the worker should not pull new work."""
        return self._exclusive or self._waiting_exclusive > 0

    @asynccontextmanager
    async def shared(self) -> AsyncIterator[None]:
        async with self._cond:
            await self._cond.wait_for(lambda: not self._exclusive and self._waiting_exclusive == 0)
            self._shared += 1
        try:
            yield
        finally:
            async with self._cond:
                self._shared -= 1
                self._cond.notify_all()

    @asynccontextmanager
    async def exclusive(self) -> AsyncIterator[None]:
        async with self._cond:
            self._waiting_exclusive += 1
            try:
                await self._cond.wait_for(lambda: not self._exclusive and self._shared == 0)
            finally:
                self._waiting_exclusive -= 1
                self._cond.notify_all()  # waiters re-check: e.g. shared waiters if we gave up
            self._exclusive = True
        try:
            yield
        finally:
            async with self._cond:
                self._exclusive = False
                self._cond.notify_all()
