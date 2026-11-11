from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path

import pytest
import pytest_asyncio

from tests.failure.harness import Cluster, Infra


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "tests/failure" in str(item.fspath):
            item.add_marker(pytest.mark.failure)


@pytest.fixture
def make_infra() -> Iterator[Callable[..., Infra]]:
    started: list[Infra] = []

    def make(**kw: bool) -> Infra:
        infra = Infra(**kw).start()
        started.append(infra)
        return infra

    yield make
    for infra in started:
        infra.stop()


@pytest_asyncio.fixture
async def make_cluster(tmp_path: Path) -> AsyncIterator[Callable[..., Cluster]]:
    clusters: list[Cluster] = []

    def make(infra: Infra, **kw: str) -> Cluster:
        c = Cluster(infra, tmp_path, **kw)
        clusters.append(c)
        return c

    yield make
    for c in clusters:
        c.stop_all()
