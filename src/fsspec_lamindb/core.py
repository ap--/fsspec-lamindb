"""Filesystem for LaminDB instances: ``lamin://<owner>/<instance>/...``."""

from __future__ import annotations

import asyncio
import functools
import os
import time
import weakref
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Sequence
from contextvars import ContextVar
from datetime import datetime
from typing import Any

from fsspec.asyn import AsyncFileSystem, sync, sync_wrapper
from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.exceptions import FSTimeoutError
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from fsspec.utils import stringify_path, tokenize

from ._common import (
    ARTIFACT,
    PROTOCOL,
    Listing,
    _Folder,
    _join_uri,
    _parent,
    _parts,
    _sorted,
    _State,
    _Target,
)
from ._hub import DEFAULT_API_URL, DEFAULT_HUB_ANON_KEY, DEFAULT_HUB_URL, MAIN_BRANCH_ID, LaminHub
from ._records import (
    Storage,
    artifact_info,
    directory_info,
    is_artifact_entry,
    is_below,
    is_folder,
    join,
    merge_entry,
    relative,
)
from ._storage import DEFAULT_STORAGE_PROTOCOLS, StorageAccess, call, is_async

# listings expire after an hour by default, as artifacts are added and get new versions
DEFAULT_LISTINGS_EXPIRY_TIME = 3600
# a 16-character uid addresses a version family, a 20-character uid a single version
UID_LENGTHS = (16, 20)
# options of storage filesystems that hold secrets; to_dict(include_password=False) drops them
SECRET_OPTIONS = frozenset(
    {
        "access_token",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "client_secret",
        "credentials",
        "key",
        "password",
        "refresh_token",
        "secret",
        "session_token",
        "token",
    }
)

# when the current filesystem operation started; results fetched since then are always fresh
_operation_start: ContextVar[float | None] = ContextVar("lamin_operation_start", default=None)


def _operation(method: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """Mark an entry point of a filesystem operation.

    Results fetched during the operation are reused by it even if the listings cache is
    disabled; nested entry points belong to the outermost operation. If the operation
    raises FileNotFoundError for a path that isn't normalized, a hint is added.

    Parameters
    ----------
    method : callable
        The coroutine function of the operation.

    Returns
    -------
    callable
        The wrapped coroutine function.
    """

    @functools.wraps(method)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        if _operation_start.get() is not None:
            return await method(*args, **kwargs)
        token = _operation_start.set(time.monotonic())
        try:
            return await method(*args, **kwargs)
        except FileNotFoundError as error:
            # the first argument after self is the path
            if len(args) > 1 and _not_normalized(args[1]):
                error.args = (f"{error}; {_NORMALIZE_HINT}",)
            raise
        finally:
            _operation_start.reset(token)

    return wrapper


_NORMALIZE_HINT = "paths aren't normalized, so they can't contain //, /./ or /../"


def _not_normalized(path: Any) -> bool:
    """Check whether a path has empty, ``.`` or ``..`` segments.

    Parameters
    ----------
    path : Any
        The path, with or without protocol.

    Returns
    -------
    bool
        Whether the path has such segments.
    """
    if not isinstance(path, (str, os.PathLike)):
        return False
    path = stringify_path(path)
    if path.startswith(f"{PROTOCOL}://"):
        path = path[len(PROTOCOL) + 3 :]
    path = path.strip("/")
    return bool(path) and any(segment in ("", ".", "..") for segment in path.split("/"))


def _unlisted(path: str) -> bool:
    """Check whether a path can exist although its parent's listing doesn't show it.

    These are owners and instances, as only accessible ones are listed, and artifacts
    addressed by uid, as only the 20-character uids of the visible branches are listed.

    Parameters
    ----------
    path : str
        A path without protocol.

    Returns
    -------
    bool
        Whether the path can be missing from its parent's listing.
    """
    parts = _parts(path)
    return len(parts) <= 2 or (len(parts) == 4 and parts[2] == ARTIFACT)


def _split_artifact(path: str) -> tuple[str, str]:
    """Split a path below an instance into the ``artifact`` directory and the rest.

    Parameters
    ----------
    path : str
        A path below ``<owner>/<instance>``, without protocol.

    Returns
    -------
    base : str
        The path of the ``artifact`` directory, ``<owner>/<instance>/artifact``.
    rest : str
        The path relative to ``base``, ``<uid>[/<subpath>]``; empty for ``base`` itself.

    Raises
    ------
    FileNotFoundError
        If the path isn't below ``<owner>/<instance>/artifact``.
    """
    parts = _parts(path)
    if parts[2] != ARTIFACT:
        raise FileNotFoundError(path)
    return "/".join(parts[:3]), "/".join(parts[3:])


def _timestamp(value: str | None, path: str) -> datetime:
    if not value:
        raise ValueError(f"no timestamp available for {path}")
    return datetime.fromisoformat(value)


def _make_parent(path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)


def _without_secrets(options: Any) -> Any:
    if isinstance(options, dict):
        return {k: _without_secrets(v) for k, v in options.items() if k not in SECRET_OPTIONS}
    return options


def _close_session(loop: asyncio.AbstractEventLoop | None, hub: LaminHub) -> None:
    session = hub._session
    if session is None or session.closed:
        return
    if loop is not None and loop.is_running():
        try:
            sync(loop, session.close, timeout=0.1)
            return
        except (TimeoutError, FSTimeoutError, NotImplementedError, RuntimeError):
            pass
    connector = getattr(session, "_connector", None)
    if connector is not None:
        connector._close()


class LaminFile(AbstractBufferedFile):
    """Read-only file of a :class:`LaminDBFileSystem`, fetched in byte ranges.

    Parameters
    ----------
    fs : LaminDBFileSystem
        The filesystem the file belongs to.
    path : str
        Path of the file in ``fs``.
    target : _Target
        The entry of the file and the storage that holds its data.
    **kwargs
        Options of :class:`fsspec.spec.AbstractBufferedFile`, e.g. ``block_size``.
    """

    def __init__(self, fs: LaminDBFileSystem, path: str, target: _Target, **kwargs: Any):
        self.target = target
        super().__init__(fs, path, **kwargs)

    @property
    def uri(self) -> str | None:
        return self.target.uri

    def _fetch_range(self, start: int, end: int) -> bytes:
        return sync(self.fs.loop, self.fs._read_range, self.target, start, end)


class LaminDBFileSystem(AsyncFileSystem):
    r"""Read-only filesystem of the artifacts of LaminDB instances.

    URIs follow nf-lamin's ``lamin://`` scheme::

        lamin://<owner>/<instance>/artifact/<uid>[/<subpath>]

    A 20-character uid addresses that exact version of an artifact, a 16-character uid the
    latest version. ``artifact/`` lists all artifacts by their 20-character uid, i.e. every
    version, including artifacts without a key and those that lamindb creates to track runs.
    Folder artifacts are directories whose content is listed from their storage, and
    ``<subpath>`` addresses the files and directories inside them.

    Metadata comes from the LaminHub REST API; neither ``lamindb`` nor database access is
    needed. Data is read with the fsspec filesystem of its storage, e.g. ``s3fs``, with
    temporary credentials from LaminHub for the S3 storage that Lamin manages.

    Parameters
    ----------
    api_key : str, optional
        Lamin API key; defaults to the ``LAMIN_API_KEY`` environment variable. Without one,
        only public instances are accessible.
    api_url : str, optional
        LaminHub REST API used to look up instances; defaults to the ``LAMIN_API_URL``
        environment variable, else ``https://aws.us-east-1.lamin.ai/api``.
    hub_url : str, optional
        LaminHub URL that grants storage credentials; defaults to the ``LAMIN_HUB_URL``
        environment variable, else ``https://hub.lamin.ai``.
    hub_anon_key : str, optional
        LaminHub's public key; defaults to ``LAMIN_HUB_ANON_KEY``, else the key of
        ``hub.lamin.ai``.
    branch : str, optional
        Name or uid of a branch to show on top of ``main``, like ``lamin switch``.
    remote_options : dict of str to dict, optional
        Options of the filesystems of storage locations by protocol or by storage root, e.g.
        ``{"s3": {"profile": "default"}, "s3://bucket": {"profile": "other"}}``. Options for
        a root are merged over those for its protocol, and ``"gcs"`` is the same as ``"gs"``.
        Credentials given for S3 replace those from LaminHub.
    timeout : float, default 60.0
        Timeout of each request to LaminHub in seconds.
    max_concurrency : int, default 8
        Maximum number of result pages of a query fetched concurrently.
    max_requests : int, default 16
        Maximum number of concurrent requests to LaminHub and its REST API.
    max_cached_paths : int, optional
        Maximum number of paths whose lookups are cached, besides the listings; the least
        recently used are dropped first. Unlimited by default; reading a large Zarr store
        caches a lookup per chunk.
    storage_protocols : sequence of str, optional
        The protocols of the storage locations that may be read; by default ``("s3", "gs",
        "hf", "http", "https")``. Add ``"file"`` to read local storage locations. Storage
        locations are named by the records of an instance, so reading local or other
        protocols lets its owner choose what is read on your machine.
    trusted_hosts : sequence of str, optional
        Hosts that your access token may be sent to besides ``*.lamin.ai`` and the host of
        ``api_url``, e.g. of on-premises deployments; ``"*.example.com"`` matches its
        subdomains. The API URLs of instances and storages come from their settings.
    **kwargs
        Options of :class:`fsspec.asyn.AsyncFileSystem`, e.g. ``asynchronous``,
        ``use_listings_cache`` or ``listings_expiry_time``, which is an hour by default;
        ``None`` keeps listings until the cache is invalidated.

    Attributes
    ----------
    branch : str or None
        Name or uid of the branch shown on top of ``main``.
    remote_options : dict of str to dict
        Options of the filesystems of storage locations by protocol or storage root.
    max_cached_paths : int or None
        Maximum number of paths whose lookups are cached besides the listings.

    Examples
    --------
    >>> import fsspec
    >>> fs = fsspec.filesystem("lamin")
    >>> fs.ls("lamin://laminlabs/lamindata", detail=False)
    ['laminlabs/lamindata/artifact']
    >>> with fs.open("lamin://laminlabs/lamindata/artifact/K415DrXcuPiJmXlX/meta.csv") as f:
    ...     f.readline()
    b',species,file_name,scientist,instrument\n'
    """

    protocol = PROTOCOL
    root_marker = ""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        api_url: str | None = None,
        hub_url: str | None = None,
        hub_anon_key: str | None = None,
        branch: str | None = None,
        remote_options: dict[str, dict[str, Any]] | None = None,
        timeout: float = 60.0,
        max_concurrency: int = 8,
        max_requests: int = 16,
        max_cached_paths: int | None = None,
        storage_protocols: Sequence[str] | None = None,
        trusted_hosts: Sequence[str] | None = None,
        **kwargs: Any,
    ):
        kwargs.setdefault("listings_expiry_time", DEFAULT_LISTINGS_EXPIRY_TIME)
        super().__init__(**kwargs)
        if api_key is None:
            api_key = os.environ.get("LAMIN_API_KEY")
        self._hub = LaminHub(
            api_key=api_key,
            api_url=api_url or os.environ.get("LAMIN_API_URL") or DEFAULT_API_URL,
            hub_url=hub_url or os.environ.get("LAMIN_HUB_URL") or DEFAULT_HUB_URL,
            hub_anon_key=hub_anon_key
            or os.environ.get("LAMIN_HUB_ANON_KEY")
            or DEFAULT_HUB_ANON_KEY,
            timeout=timeout,
            max_concurrency=max_concurrency,
            trusted_hosts=trusted_hosts or (),
            max_requests=max_requests,
        )
        self.branch = branch or None
        self.remote_options = dict(remote_options or {})
        if storage_protocols is None:
            storage_protocols = DEFAULT_STORAGE_PROTOCOLS
        self._storage_access = StorageAccess(
            self._hub, self.remote_options, self.asynchronous, self._loop, storage_protocols
        )
        self._tasks: dict[tuple, tuple[float, asyncio.Future]] = {}
        self._folders: dict[str, _Folder] = {}
        # the located paths, least recently used first
        self._located: OrderedDict[str, tuple[float, _Target]] = OrderedDict()
        self.max_cached_paths = max_cached_paths
        # storages outside of the visible branches that artifacts are in, by instance and root
        self._storage_roots: dict[tuple[str, str], Storage] = {}
        if not self.asynchronous:
            weakref.finalize(self, _close_session, self._loop, self._hub)

    @classmethod
    def _strip_protocol(cls, path):
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        path = stringify_path(path)
        if path.startswith(f"{PROTOCOL}://"):
            path = path[len(PROTOCOL) + 3 :]
        return path.strip("/")

    # memoization

    def _fresh(self, stamp: float) -> bool:
        """Check whether a result can be reused under the listings cache options.

        Results fetched during the current operation are always fresh. Otherwise, they're
        fresh if the listings cache is enabled and they haven't expired.

        Parameters
        ----------
        stamp : float
            When the result was fetched, as a :func:`time.monotonic` timestamp.

        Returns
        -------
        bool
            Whether the result can be reused.
        """
        start = _operation_start.get()
        if start is not None and stamp >= start:
            return True
        if not self.dircache.use_listings_cache:
            return False
        expiry = self.dircache.listings_expiry_time
        return expiry is None or time.monotonic() - stamp <= expiry

    async def _once(
        self, key: tuple, factory: Callable[[], Awaitable[Any]], persistent: bool = False
    ) -> Any:
        """Run ``factory`` once per key, sharing the result with concurrent callers.

        Results are reused while they're fresh (see :meth:`_fresh`); failures aren't kept.

        Parameters
        ----------
        key : tuple
            The key of the result.
        factory : callable
            Creates the coroutine that computes the result.
        persistent : bool, default False
            Whether the result is kept until the cache is invalidated, regardless of the
            listings cache options.

        Returns
        -------
        Any
            The result of the coroutine.
        """
        entry = self._tasks.get(key)
        if entry is not None and entry[1].done() and not persistent and not self._fresh(entry[0]):
            entry = None
        if entry is None:
            entry = self._tasks[key] = (time.monotonic(), asyncio.ensure_future(factory()))
        future = entry[1]
        try:
            return await asyncio.shield(future)
        except BaseException:
            if future.done() and self._tasks.get(key) is entry:
                del self._tasks[key]
            raise

    # instances

    async def _state(self, owner: str, name: str) -> _State:
        # the instance settings and the branch are kept for the lifetime of the filesystem
        return await self._once(
            ("state", f"{owner}/{name}"), lambda: self._load_state(owner, name), persistent=True
        )

    async def _load_state(self, owner: str, name: str) -> _State:
        instance = await self._hub.instance(f"{owner}/{name}")
        branch_id = None
        if self.branch is not None:
            branches = await self._hub.query(
                instance,
                "branch",
                filter={"or": [{"uid": {"eq": self.branch}}, {"name": {"eq": self.branch}}]},
                branch_ids=None,
                limit=2,
            )
            if len(branches) != 1:
                problem = "does not exist" if not branches else "is ambiguous"
                raise FileNotFoundError(f"branch {self.branch!r} {problem} in {instance.slug}")
            branch_id = branches[0]["id"]
        if branch_id in (None, MAIN_BRANCH_ID):
            return _State(instance, [MAIN_BRANCH_ID])
        return _State(instance, [branch_id, MAIN_BRANCH_ID], branch_id)

    async def _query(self, state: _State, model: str, **kwargs: Any) -> list[dict[str, Any]]:
        kwargs.setdefault("branch_ids", state.branch_ids)
        return await self._hub.query(state.instance, model, **kwargs)

    # storages

    async def _storage_index(self, state: _State) -> dict[int, Storage]:
        async def load() -> dict[int, Storage]:
            records = await self._query(state, "storage")
            storages = [Storage.from_record(r) for r in records if r.get("root")]
            return {storage.id: storage for storage in storages}

        return await self._once(("storages", state.slug), load)

    async def _storage(self, state: _State, storage_id: int | None) -> Storage | None:
        if storage_id is None:
            return None
        index = await self._storage_index(state)
        if storage_id in index:
            return index[storage_id]

        async def load() -> Storage | None:
            # e.g. a storage record outside of the visible branches
            records = await self._query(
                state, "storage", filter={"id": {"eq": storage_id}}, branch_ids=None, limit=1
            )
            if not records:
                return None
            storage = Storage.from_record(records[0])
            # cached entries only carry the root, so it has to be found again by root
            self._storage_roots[(state.slug, storage.root)] = storage
            return storage

        return await self._once(("storage", state.slug, storage_id), load)

    async def _storage_by_root(self, state: _State, root: str | None) -> Storage | None:
        if root is None:
            return None
        storage = self._storage_roots.get((state.slug, root))
        if storage is not None:
            return storage
        index = await self._storage_index(state)
        return next((s for s in index.values() if s.root == root), None)

    # artifacts

    async def _artifact_entry(
        self, state: _State, name: str, record: dict[str, Any]
    ) -> tuple[dict[str, Any], Storage | None]:
        storage = await self._storage(state, record.get("storage_id"))
        return artifact_info(name, record, storage), storage

    async def _ls_artifacts(
        self, state: _State, path: str, base: str, rest: str
    ) -> list[dict[str, Any]]:
        """List ``artifact/``, an artifact, or a directory inside a folder artifact.

        Parameters
        ----------
        state : _State
            The instance.
        path : str
            The path to list, without protocol.
        base : str
            The path of the ``artifact`` directory, ``<owner>/<instance>/artifact``.
        rest : str
            The path relative to ``base``, ``<uid>[/<subpath>]``.

        Returns
        -------
        list of dict
            The entries of the directory ``path``, or the entry of the file ``path``.

        Raises
        ------
        FileNotFoundError
            If nothing exists at ``path``.
        """
        if rest:
            target = await self._locate(path)
            folder = self._find_folder(path)
            if not target.is_dir or folder is None:
                return [target.info]
            return await self._ls_folder(path, folder)
        records = await self._query(state, "artifact")
        listing: Listing = {}
        for record in records:
            info, storage = await self._artifact_entry(state, join(base, record["uid"]), record)
            listing[record["uid"]] = info
            if is_folder(record) and storage is not None:
                self._register_folder(info, storage)
        return self._store(path, listing)

    async def _locate_artifact(self, state: _State, path: str, base: str, rest: str) -> _Target:
        """Locate an artifact by uid, or a path inside a folder artifact.

        Parameters
        ----------
        state : _State
            The instance.
        path : str
            The path to locate, without protocol.
        base : str
            The path of the ``artifact`` directory, ``<owner>/<instance>/artifact``.
        rest : str
            The path relative to ``base``, ``<uid>[/<subpath>]``.

        Returns
        -------
        _Target
            The target of the path.

        Raises
        ------
        FileNotFoundError
            If nothing exists at ``path``.
        """
        uid, _, sub = rest.partition("/")
        if len(uid) not in UID_LENGTHS:
            raise FileNotFoundError(f"{path}: uids have 16 or 20 characters")
        if len(uid) == 20:
            records = await self._query(
                state, "artifact", filter={"uid": {"eq": uid}}, branch_ids=None, limit=1
            )
        else:
            records = await self._query(
                state,
                "artifact",
                filter={"and": [{"uid": {"startswith": uid}}, {"is_latest": {"eq": True}}]},
            )
            records = state.prefer(records)[-1:]
        if not records:
            raise FileNotFoundError(path)
        info, storage = await self._artifact_entry(state, join(base, uid), records[0])
        if is_folder(records[0]) and storage is not None:
            return await self._locate_in_folder(path, self._register_folder(info, storage))
        if sub:
            raise FileNotFoundError(path)
        return _Target(info, storage)

    # folders

    def _find_folder(self, path: str) -> _Folder | None:
        probe = path
        while probe:
            folder = self._folders.get(probe)
            if folder is not None:
                if self._fresh(folder.registered):
                    return folder
                del self._folders[probe]
            probe = _parent(probe)
        return None

    def _register_folder(self, info: dict[str, Any], storage: Storage) -> _Folder:
        path = info["name"]
        folder = self._folders.get(path)
        if folder is None or not self._fresh(folder.registered):
            folder = self._folders[path] = _Folder(path, info, storage)
        return folder

    # listing

    @_operation
    async def _ls(self, path, detail=True, refresh=False, **kwargs):
        path = self._strip_protocol(path)
        if refresh:
            self.invalidate_cache(path)
        entries = self._cached_ls(path)
        if entries is None:
            entries = await self._ls_uncached(path)
        return list(entries) if detail else [entry["name"] for entry in entries]

    def _cached_ls(self, path: str) -> list[dict[str, Any]] | None:
        try:
            return self.dircache[path]
        except KeyError:
            pass
        if not path:
            return None
        try:
            siblings = self.dircache[_parent(path)]
        except KeyError:
            return None
        for entry in siblings:
            if entry["name"] == path:
                return None if entry["type"] == "directory" else [entry]
        if _unlisted(path):
            return None
        raise FileNotFoundError(path)

    def _store(self, path: str, listing: Listing) -> list[dict[str, Any]]:
        entries = _sorted(listing)
        self.dircache[path] = entries
        return entries

    async def _ls_uncached(self, path: str) -> list[dict[str, Any]]:
        parts = _parts(path)
        if len(parts) < 2:
            return self._store(path, await self._ls_accessible(parts[0] if parts else None))
        state = await self._state(parts[0], parts[1])
        if len(parts) == 2:
            return self._store(path, {ARTIFACT: directory_info(join(path, ARTIFACT))})
        base, rest = _split_artifact(path)
        folder = self._find_folder(path)
        if folder is not None:
            return await self._ls_folder(path, folder)
        return await self._ls_artifacts(state, path, base, rest)

    async def _ls_accessible(self, owner: str | None) -> Listing:
        """List the owners, or the instances of an owner, that the account can access.

        Parameters
        ----------
        owner : str or None
            The owner whose instances are listed; ``None`` to list the owners.

        Returns
        -------
        dict of str to dict
            The entries by name.

        Raises
        ------
        FileNotFoundError
            If ``owner`` has no instance that the account can list.
        """
        listing: Listing = {}
        instances = await self._once(("instances", ""), self._hub.list_instances)
        for item in instances:
            item_owner, name = item.get("owner"), item.get("name")
            if not item_owner or not name:
                continue
            if owner is None:
                listing[item_owner] = directory_info(item_owner)
            elif item_owner == owner:
                info = directory_info(join(owner, name))
                info["instance_id"] = item.get("id")
                listing[name] = info
        if owner is not None and not listing:
            raise self._owner_not_found(owner)
        return listing

    def _owner_not_found(self, owner: str) -> FileNotFoundError:
        if self._hub.authenticated:
            reason = "no instance of this owner is accessible to your account"
        else:
            reason = "owners can only be listed with an api_key"
        return FileNotFoundError(
            f"{owner}: {reason}; public instances can still be opened as lamin://{owner}/<instance>"
        )

    async def _ls_folder(self, path: str, folder: _Folder) -> list[dict[str, Any]]:
        rel = relative(path, folder.path)
        uri = _join_uri(folder.uri, rel)
        listing = await self._ls_storage(path, folder.storage, uri)
        if rel and not listing:
            # not a directory in storage: a file (some filesystems list a file as itself),
            # an empty directory, or nothing
            target = await self._locate_in_storage(path, folder.storage, uri)
            if not target.is_dir:
                return [target.info]
        return self._store(path, listing or {})

    async def _ls_storage(self, path: str, storage: Storage, uri: str) -> Listing | None:
        """List the directory ``uri`` in ``storage``.

        Parameters
        ----------
        path : str
            The path that the entries are named below.
        storage : Storage
            The storage location.
        uri : str
            The URI of the directory in the storage.

        Returns
        -------
        dict of str to dict or None
            The entries by name; ``None`` if the directory doesn't exist.
        """
        try:
            entries = await self._storage_call(storage, "ls", uri, detail=True)
        except FileNotFoundError:
            return None
        fs = await self._storage_access.filesystem(storage)
        prefix = fs._strip_protocol(uri).rstrip("/") + "/"
        listing: Listing = {}
        for native in entries:
            native_name = native["name"].rstrip("/")
            if not native_name.startswith(prefix):
                continue
            name = native_name[len(prefix) :]
            if not name or "/" in name:
                continue
            info = {
                "name": join(path, name),
                "size": native.get("size"),
                "type": "directory" if native.get("type") == "directory" else "file",
                "uri": _join_uri(uri, name),
                "storage": storage.root,
            }
            if etag := native.get("ETag") or native.get("etag"):
                info["etag"] = etag
            if modified := native.get("LastModified") or native.get("last_modified"):
                info["last_modified"] = str(modified)
            merge_entry(listing, name, info)
        return listing

    async def _storage_call(self, storage: Storage, method: str, *args: Any, **kwargs: Any) -> Any:
        """Call a method of the filesystem of a storage location, see :func:`call`.

        Parameters
        ----------
        storage : Storage
            The storage location.
        method : str
            Name of the method, e.g. ``"cat_file"``.
        *args
            Positional arguments of the method.
        **kwargs
            Keyword arguments of the method.

        Returns
        -------
        Any
            The result of the method.
        """
        return await self._storage_access.run(storage, lambda fs: call(fs, method, *args, **kwargs))

    # searching

    @_operation
    async def _find(self, path, maxdepth=None, withdirs=False, **kwargs):
        """Find the files below a path, like fsspec's ``find``.

        The content of folder artifacts is found with a single recursive search of their
        storage instead of listing each of their directories.

        Parameters
        ----------
        path : str
            The path to search.
        maxdepth : int, optional
            Maximum depth to descend into; fsspec's default search is used if given.
        withdirs : bool, default False
            Whether directories are included.
        **kwargs
            Options of fsspec's ``find``, e.g. ``detail``.

        Returns
        -------
        list of str or dict of str to dict
            The names of the entries, or the entries by name if ``detail=True``.
        """
        if maxdepth is not None:
            return await super()._find(path, maxdepth=maxdepth, withdirs=withdirs, **kwargs)
        path = self._strip_protocol(path)
        detail = kwargs.pop("detail", False)
        try:
            root = await self._info(path)
        except FileNotFoundError:
            return {} if detail else []
        out: dict[str, dict[str, Any]] = {}
        if root["type"] != "directory":
            out[path] = root
        else:
            if withdirs and path:
                out[path] = root
            semaphore = asyncio.Semaphore(16)
            frontier = [path]
            while frontier:
                steps = [self._find_step(p, withdirs, semaphore) for p in frontier]
                frontier = []
                for found, subdirectories in await asyncio.gather(*steps):
                    out.update(found)
                    frontier.extend(subdirectories)
        names = sorted(out)
        return {name: out[name] for name in names} if detail else names

    async def _find_step(
        self, path: str, withdirs: bool, semaphore: asyncio.Semaphore
    ) -> tuple[dict[str, dict[str, Any]], list[str]]:
        """Find the entries below a directory and the subdirectories still to search.

        Parameters
        ----------
        path : str
            The directory.
        withdirs : bool
            Whether directories are included in the entries.
        semaphore : asyncio.Semaphore
            Limits the number of concurrent searches.

        Returns
        -------
        found : dict of str to dict
            The entries below the directory by name.
        subdirectories : list of str
            The subdirectories that still need to be searched.
        """
        async with semaphore:
            folder = self._find_folder(path)
            if folder is not None:
                return await self._find_in_folder(path, folder, withdirs), []
            try:
                entries = await self._ls(path, detail=True)
            except FileNotFoundError:
                return {}, []
        found: dict[str, dict[str, Any]] = {}
        subdirectories = []
        for entry in entries:
            name = entry["name"]
            if entry["type"] != "directory":
                found[name] = entry
            elif name != path:
                if withdirs:
                    found[name] = entry
                subdirectories.append(name)
        return found, subdirectories

    async def _find_in_folder(
        self, path: str, folder: _Folder, withdirs: bool
    ) -> dict[str, dict[str, Any]]:
        uri = _join_uri(folder.uri, relative(path, folder.path))
        try:
            native = await self._storage_call(
                folder.storage, "find", uri, withdirs=withdirs, detail=True
            )
        except FileNotFoundError:
            native = {}
        fs = await self._storage_access.filesystem(folder.storage)
        prefix = fs._strip_protocol(uri).rstrip("/") + "/"
        found: dict[str, dict[str, Any]] = {}
        for native_name, native_info in native.items():
            native_name = native_name.rstrip("/")
            if not native_name.startswith(prefix):
                continue
            sub = native_name[len(prefix) :]
            name = join(path, sub)
            found[name] = {
                "name": name,
                "size": native_info.get("size"),
                "type": "directory" if native_info.get("type") == "directory" else "file",
                "uri": _join_uri(uri, sub),
                "storage": folder.storage.root,
            }
        return found

    # locating single paths

    @_operation
    async def _info(self, path, **kwargs):
        return dict((await self._locate(self._strip_protocol(path))).info)

    async def _locate(self, path: str) -> _Target:
        parts = _parts(path)
        if not parts:
            return _Target(directory_info(path))
        if len(parts) == 1:
            await self._ls_accessible(parts[0])
            return _Target(directory_info(path))
        state = await self._state(parts[0], parts[1])
        if len(parts) == 2:
            info = directory_info(path)
            info.update(instance_id=state.instance.id, api_url=state.instance.api_url)
            return _Target(info)
        base, rest = _split_artifact(path)
        if not rest:
            return _Target(directory_info(path))
        cached = await self._from_dircache(state, path)
        if cached is not None:
            return cached
        target = self._recall(path)
        if target is None:
            folder = self._find_folder(path)
            if folder is not None:
                target = await self._locate_in_folder(path, folder)
            else:
                target = await self._locate_artifact(state, path, base, rest)
            self._remember(path, target)
        return target

    async def _from_dircache(self, state: _State, path: str) -> _Target | None:
        try:
            siblings = self.dircache[_parent(path)]
        except KeyError:
            return None
        for entry in siblings:
            if entry["name"] == path:
                return _Target(entry, await self._storage_by_root(state, entry.get("storage")))
        if _unlisted(path):
            return None
        raise FileNotFoundError(path)

    def _recall(self, path: str) -> _Target | None:
        """Return a path located before, while the listings cache is valid.

        Parameters
        ----------
        path : str
            The path.

        Returns
        -------
        _Target or None
            The target of the path; ``None`` if it wasn't located before or has expired.
        """
        located = self._located.get(path)
        if located is None:
            return None
        if not self._fresh(located[0]):
            self._located.pop(path, None)
            return None
        self._located.move_to_end(path)
        return located[1]

    def _remember(self, path: str, target: _Target) -> None:
        if not self.dircache.use_listings_cache:
            return
        self._located[path] = (time.monotonic(), target)
        self._located.move_to_end(path)
        if self.max_cached_paths is not None:
            while len(self._located) > self.max_cached_paths:
                self._located.popitem(last=False)

    async def _locate_in_folder(self, path: str, folder: _Folder) -> _Target:
        if path == folder.path:
            return _Target(folder.info, folder.storage)
        uri = _join_uri(folder.uri, relative(path, folder.path))
        return await self._locate_in_storage(path, folder.storage, uri)

    async def _locate_in_storage(self, path: str, storage: Storage, uri: str) -> _Target:
        try:
            native = await self._storage_call(storage, "info", uri)
        except FileNotFoundError:
            raise FileNotFoundError(path) from None
        info = {
            "name": path,
            "size": native.get("size"),
            "type": "directory" if native.get("type") == "directory" else "file",
            "uri": uri,
            "storage": storage.root,
        }
        return _Target(info, storage)

    # reading

    @_operation
    async def _file_target(self, path: str) -> _Target:
        target = await self._locate(self._strip_protocol(path))
        if target.is_dir:
            raise IsADirectoryError(path)
        target.location()
        return target

    async def _target_size(self, target: _Target) -> int:
        size = target.info.get("size")
        if size is not None:
            return size
        storage, uri = target.location()
        return (await self._storage_call(storage, "info", uri))["size"]

    async def _read_range(self, target: _Target, start: int | None, end: int | None) -> bytes:
        storage, uri = target.location()
        return await self._storage_call(storage, "cat_file", uri, start=start, end=end)

    @_operation
    async def _cat_file(self, path, start=None, end=None, **kwargs):
        target = await self._file_target(path)
        if (start is not None and start < 0) or (end is not None and end < 0):
            size = await self._target_size(target)
            if start is not None and start < 0:
                start = max(0, size + start)
            if end is not None and end < 0:
                end = size + end
        if start is not None and end is not None and end <= start:
            return b""
        return await self._read_range(target, start, end)

    @_operation
    async def _get_file(self, rpath, lpath, callback=DEFAULT_CALLBACK, **kwargs):
        target = await self._locate(self._strip_protocol(rpath))
        if target.is_dir:
            await asyncio.to_thread(os.makedirs, lpath, exist_ok=True)
            return None
        storage, uri = target.location()
        await asyncio.to_thread(_make_parent, lpath)
        await self._storage_call(storage, "get_file", uri, lpath, callback=callback)
        return None

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        if mode != "rb":
            raise NotImplementedError(f"{type(self).__name__} is read-only")
        target = sync(self.loop, self._file_target, path)
        size = sync(self.loop, self._target_size, target)
        return LaminFile(
            self,
            self._strip_protocol(path),
            target,
            mode=mode,
            block_size=block_size,
            cache_options=cache_options,
            size=size,
            **kwargs,
        )

    @_operation
    async def open_async(self, path, mode="rb", **kwargs):
        if mode != "rb":
            raise NotImplementedError(f"{type(self).__name__} is read-only")
        target = await self._file_target(path)
        storage, uri = target.location()

        async def open_native(fs: AbstractFileSystem) -> Any:
            if not (is_async(fs) and fs.asynchronous):
                raise NotImplementedError(f"the {storage.protocol!r} filesystem is not async")
            return await fs.open_async(uri, mode, **kwargs)

        return await self._storage_access.run(storage, open_native)

    async def close(self) -> None:
        """Close the sessions of the filesystem in async mode.

        This closes the session with LaminHub and those of the storage filesystems, including
        the ones replaced when credentials were renewed. The filesystem opens new sessions if
        it's used again. In sync mode, the sessions are closed when the filesystem is garbage
        collected. In async mode, the filesystem can also be used with ``async with``.
        """
        await self._hub.close()
        await self._storage_access.close()

    async def __aenter__(self) -> LaminDBFileSystem:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    def to_dict(self, *, include_password: bool = True) -> dict[str, Any]:
        """Return a JSON-serializable dictionary of the filesystem's options.

        Parameters
        ----------
        include_password : bool, default True
            Whether secrets are included: the API key, and credentials in
            ``remote_options`` such as ``key``, ``secret`` and ``token``.

        Returns
        -------
        dict
            The options, see :meth:`fsspec.spec.AbstractFileSystem.to_dict`.
        """
        data = super().to_dict(include_password=include_password)
        if not include_password:
            data.pop("api_key", None)
            # the only positional argument is the API key
            data["args"] = []
            if "remote_options" in data:
                data["remote_options"] = _without_secrets(data["remote_options"])
        return data

    def ukey(self, path):
        """Return a key that changes when the content of a file changes.

        Artifacts use their content hash, so that changes of their metadata don't change
        the key. Artifacts without a hash use fsspec's default, a hash of their info. Files
        inside folder artifacts use the key of their storage filesystem.

        Parameters
        ----------
        path : str
            The path of the file.

        Returns
        -------
        str
            The key.
        """
        info = self.info(path)
        native = self._native_file(path, info)
        if native is not None:
            fs, uri = native
            return fs.ukey(uri)
        if info.get("hash"):
            return tokenize(PROTOCOL, info["hash"])
        return super().ukey(path)

    def checksum(self, path):
        """Return a checksum that's the same as long as the content of a file is the same.

        It's derived like :meth:`ukey`.

        Parameters
        ----------
        path : str
            The path of the file.

        Returns
        -------
        int
            The checksum.
        """
        info = self.info(path)
        native = self._native_file(path, info)
        if native is not None:
            fs, uri = native
            return fs.checksum(uri)
        if info.get("hash"):
            return int(tokenize(PROTOCOL, info["hash"]), 16)
        return super().checksum(path)

    def _native_file(
        self, path: str, info: dict[str, Any]
    ) -> tuple[AbstractFileSystem, str] | None:
        """Return the storage filesystem and URI of a file inside a folder artifact.

        Parameters
        ----------
        path : str
            The path of the file.
        info : dict
            The info of the file.

        Returns
        -------
        tuple of (AbstractFileSystem, str) or None
            The storage filesystem and the URI of the file; ``None`` if the path isn't a
            file inside a folder artifact.
        """
        if "uid" in info or not info.get("uri") or info["type"] != "file":
            return None
        target = sync(self.loop, self._locate, self._strip_protocol(path))
        storage, uri = target.location()
        return sync(self.loop, self._storage_access.filesystem, storage), uri

    @_operation
    async def _artifact_uri(self, path: str, version_family: bool = False) -> str:
        """Return the URI of the artifact behind a path, in nf-lamin's ``artifact/<uid>`` form.

        16-character uids resolve to the version that they address now, e.g.
        ``lamin://laminlabs/lamindata/artifact/K415DrXcuPiJmXlX`` to
        ``lamin://laminlabs/lamindata/artifact/K415DrXcuPiJmXlX5iXI``. Paths inside a folder
        artifact resolve to the folder artifact's URI and their path inside of it.

        Parameters
        ----------
        path : str
            A path of an artifact or inside a folder artifact, with or without protocol.
        version_family : bool, default False
            Whether to return the 16-character uid of the artifact's version family, which
            nf-lamin and this filesystem resolve to the latest version, instead of the
            20-character uid of the exact version.

        Returns
        -------
        str
            The URI, ``lamin://<owner>/<instance>/artifact/<uid>[/<path in folder>]``.

        Raises
        ------
        FileNotFoundError
            If nothing exists at ``path``.
        ValueError
            If ``path`` is neither an artifact nor inside a folder artifact, e.g. the
            ``artifact`` directory itself.

        Examples
        --------
        >>> import fsspec
        >>> fs = fsspec.filesystem("lamin")
        >>> fs.artifact_uri("lamin://laminlabs/lamindata/artifact/K415DrXcuPiJmXlX/meta.csv")
        'lamin://laminlabs/lamindata/artifact/K415DrXcuPiJmXlX5iXI/meta.csv'
        """
        path = self._strip_protocol(path)
        artifact = path
        info = (await self._locate(artifact)).info
        # files and directories inside a folder artifact are entries of its storage
        while not is_artifact_entry(info):
            if not info.get("uri") or len(_parts(artifact)) <= 2:
                raise ValueError(f"{path} is neither an artifact nor inside a folder artifact")
            artifact = _parent(artifact)
            info = (await self._locate(artifact)).info
        uid = info["uid"][:16] if version_family else info["uid"]
        slug = "/".join(_parts(path)[:2])
        return f"{PROTOCOL}://{join(slug, ARTIFACT, uid, relative(path, artifact))}"

    artifact_uri = sync_wrapper(_artifact_uri)

    def modified(self, path):
        info = self.info(path)
        value = info.get("updated_at") or info.get("last_modified") or info.get("created_at")
        return _timestamp(value, path)

    def created(self, path):
        info = self.info(path)
        return _timestamp(info.get("created_at") or info.get("last_modified"), path)

    def invalidate_cache(self, path=None):
        if path is None:
            self.dircache.clear()
            self._tasks.clear()
            self._folders.clear()
            self._located.clear()
            self._storage_roots.clear()
            self._storage_access.clear()
        else:
            path = self._strip_protocol(path)
            for cached in list(self.dircache):
                if is_below(cached, path) or is_below(path, cached):
                    self.dircache.pop(cached, None)
            for mapping in (self._folders, self._located):
                for cached in list(mapping):
                    if is_below(cached, path) or is_below(path, cached):
                        mapping.pop(cached, None)
            parts = _parts(path)
            if len(parts) < 2:
                self._tasks.clear()
            elif len(parts) == 2:
                slug = "/".join(parts)
                for key in list(self._tasks):
                    if key[0] in ("state", "storages", "storage") and key[1] == slug:
                        self._tasks.pop(key, None)
        super().invalidate_cache(path)

    # the filesystem is read-only

    async def _read_only(self, *args, **kwargs):
        raise NotImplementedError(f"{type(self).__name__} is read-only")

    # rm and copy would otherwise expand their paths with requests before failing
    _rm = _rm_file = _copy = _cp_file = _pipe_file = _put_file = _read_only
    _mkdir = _makedirs = _read_only

    def rmdir(self, path):
        raise NotImplementedError(f"{type(self).__name__} is read-only")
