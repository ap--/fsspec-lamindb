"""Test fixtures: a fake LaminHub server and in-memory storage filesystems."""

from __future__ import annotations

import importlib
from typing import ClassVar

import fsspec
import pytest
from fsspec.implementations.memory import MemoryFileSystem

from fakehub import API_KEY, MEMORY_FILES, S3_OBJECTS, FakeHub
from fsspec_lamindb import LaminDBFileSystem, _hub, _storage

# the fake LaminHub also has a storage location in memory
PROTOCOLS = [*_storage.DEFAULT_STORAGE_PROTOCOLS, "memory"]


class FakeS3(MemoryFileSystem):
    """Stands in for s3fs and records the options it was created with.

    Parameters
    ----------
    *args
        Positional arguments of :class:`MemoryFileSystem`.
    **kwargs
        Options of the filesystem; they're recorded in ``FakeS3.created``.
    """

    protocol = ("s3",)
    # shared by all instances, like in MemoryFileSystem
    store: dict = {}  # noqa: RUF012
    pseudo_dirs = [""]  # noqa: RUF012
    created: ClassVar[list[dict]] = []
    # how many reads fail as if the credentials had expired
    permission_errors: ClassVar[int] = 0

    def __init__(self, *args, **kwargs):
        FakeS3.created.append(dict(kwargs))
        super().__init__(*args, **kwargs)

    def cat_file(self, path, start=None, end=None, **kwargs):
        if FakeS3.permission_errors:
            FakeS3.permission_errors -= 1
            raise PermissionError("ExpiredToken")
        return super().cat_file(path, start=start, end=end, **kwargs)

    @classmethod
    def _strip_protocol(cls, path):
        if path.startswith("s3://"):
            path = path[len("s3://") :]
        return "/" + path.strip("/")


@pytest.fixture(scope="session")
def hub_server():
    server = FakeHub()
    server.start()
    return server


@pytest.fixture
def hub(hub_server, monkeypatch):
    hub_server.reset_state()
    # no waiting between retries
    monkeypatch.setattr(_hub, "_backoff", lambda attempt, retry_after: 0)
    monkeypatch.setattr(_storage, "has_ambient_aws_credentials", lambda: False)
    return hub_server


@pytest.fixture(autouse=True)
def storages(request):
    if request.node.get_closest_marker("live") is not None:
        yield
        return
    registry = importlib.import_module("fsspec.registry")._registry
    original = registry.get("s3")
    fsspec.register_implementation("s3", FakeS3, clobber=True)
    FakeS3.store.clear()
    FakeS3.created.clear()
    FakeS3.permission_errors = 0
    s3 = FakeS3()
    for key, data in S3_OBJECTS.items():
        s3.pipe_file(f"s3://{key}", data)
    FakeS3.created.clear()
    memory = MemoryFileSystem()
    for path, data in MEMORY_FILES.items():
        memory.pipe_file(path, data)
    yield
    for path in MEMORY_FILES:
        memory.rm_file(path)
    FakeS3.store.clear()
    if original is not None:
        registry["s3"] = original
    else:
        registry.pop("s3", None)


def make_fs(hub: FakeHub, api_key: str | None = API_KEY, **kwargs) -> LaminDBFileSystem:
    kwargs.setdefault("storage_protocols", PROTOCOLS)
    return LaminDBFileSystem(
        api_key=api_key or "",
        api_url=hub.api_url,
        hub_url=hub.hub_url,
        hub_anon_key="anon-key",
        skip_instance_cache=True,
        **kwargs,
    )


@pytest.fixture
def lamin(hub):
    return make_fs(hub)
