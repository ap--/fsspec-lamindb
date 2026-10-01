"""Native fsspec filesystems of storage locations, with credentials from LaminHub."""

from __future__ import annotations

import asyncio
import copy
import inspect
import logging
import os
import time
from collections.abc import Awaitable, Callable, Iterable
from datetime import UTC, datetime
from typing import Any, TypeGuard, TypeVar

import aiohttp
import fsspec
from fsspec.asyn import AsyncFileSystem
from fsspec.spec import AbstractFileSystem

from ._hub import LaminHub
from ._records import Storage

logger = logging.getLogger(__name__)

# credentials are refreshed this many seconds before they expire
REFRESH_MARGIN = 300
# credentials without a valid expiration are renewed after this many seconds
FALLBACK_LIFETIME = 900

T = TypeVar("T")
# options that configure S3 credentials themselves; LaminHub isn't asked if any is given
_S3_CREDENTIAL_OPTIONS = frozenset({"anon", "key", "secret", "token", "profile", "session"})
_EXTRAS = {"gs": "gcs", "hf": "hf"}
# the storage protocols that are read by default; local storages need "file"
DEFAULT_STORAGE_PROTOCOLS = ("s3", "gs", "hf", "http", "https")
_PROTOCOL_ALIASES = {"gcs": "gs", "local": "file"}


def canonical_protocol(protocol: str) -> str:
    """Return the canonical name of a storage protocol, e.g. ``"gs"`` for ``"gcs"``.

    Parameters
    ----------
    protocol : str
        The protocol.

    Returns
    -------
    str
        The canonical name of the protocol.
    """
    protocol = protocol.lower()
    return _PROTOCOL_ALIASES.get(protocol, protocol)


def has_ambient_aws_credentials() -> bool:
    """Check whether boto finds AWS credentials, e.g. in the environment or ``~/.aws``.

    Returns
    -------
    bool
        Whether boto finds AWS credentials.
    """
    try:
        import botocore.session

        return botocore.session.get_session().get_credentials() is not None
    except Exception:
        return False


# set on Google Cloud, where the metadata server provides credentials: Cloud Run services and
# jobs, Cloud Functions, App Engine, and custom metadata servers
_GOOGLE_CLOUD_VARIABLES = (
    "K_SERVICE",
    "CLOUD_RUN_JOB",
    "FUNCTION_TARGET",
    "GAE_APPLICATION",
    "GCE_METADATA_HOST",
    "GCE_METADATA_IP",
)


def has_ambient_google_credentials() -> bool:
    """Check whether gcsfs finds Google credentials, without network requests.

    gcsfs asks the metadata server of Google Compute Engine for credentials if it finds no
    others. Outside of Google Cloud, that takes 15 to 30 seconds until it times out. This
    checks for ``GOOGLE_APPLICATION_CREDENTIALS``, the application default credentials of
    gcloud, gcsfs's token cache, and whether this runs on Google Cloud, where the metadata
    server provides credentials. ``NO_GCE_CHECK=true`` turns the latter off.

    Returns
    -------
    bool
        Whether gcsfs finds Google credentials.
    """
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return True
    on_google_cloud = os.environ.get("NO_GCE_CHECK", "").lower() != "true"
    if on_google_cloud and any(os.environ.get(name) for name in _GOOGLE_CLOUD_VARIABLES):
        return True
    try:
        import gcsfs.credentials
        from google.auth import _cloud_sdk
        from google.auth.compute_engine import _metadata

        if os.path.isfile(_cloud_sdk.get_application_default_credentials_path()):
            return True
        token_cache = getattr(gcsfs.credentials, "tfile", None)
        if token_cache and os.path.isfile(token_cache):
            return True
        return on_google_cloud and bool(_metadata.detect_gce_residency_linux())
    except Exception:
        return False


def _merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.copy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _expiry(value: str | None, root: str) -> float:
    """Return when credentials expire.

    Parameters
    ----------
    value : str or None
        The expiration of the credentials as an ISO 8601 timestamp; without a time zone,
        it's taken to be UTC.
    root : str
        The storage location of the credentials, for the warning if ``value`` isn't valid.

    Returns
    -------
    float
        The expiration as a Unix timestamp; if ``value`` isn't valid, the credentials are
        renewed after ``FALLBACK_LIFETIME`` seconds.
    """
    try:
        expires = datetime.fromisoformat(str(value))
    except ValueError:
        logger.warning(
            "the credentials for %s have no valid expiration (%r); renewing them in %d s",
            root,
            value,
            FALLBACK_LIFETIME,
        )
        return time.time() + FALLBACK_LIFETIME + REFRESH_MARGIN
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=UTC)
    return expires.timestamp()


def is_async(fs: AbstractFileSystem) -> TypeGuard[AsyncFileSystem]:
    return isinstance(fs, AsyncFileSystem) and bool(fs.async_impl)


async def call(fs: AbstractFileSystem, method: str, *args: Any, **kwargs: Any) -> Any:
    """Call a method of a filesystem from the event loop.

    Async filesystems run the coroutine of the method on the same event loop as the caller;
    the methods of sync filesystems run in a thread.

    Parameters
    ----------
    fs : AbstractFileSystem
        The filesystem.
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
    if is_async(fs):
        return await getattr(fs, "_" + method)(*args, **kwargs)
    return await asyncio.to_thread(getattr(fs, method), *args, **kwargs)


async def close_filesystem(fs: AbstractFileSystem) -> None:
    """Close the HTTP session of a filesystem in async mode, if it has one.

    In sync mode, filesystems close their sessions themselves when they're garbage
    collected.

    Parameters
    ----------
    fs : AbstractFileSystem
        The filesystem.
    """
    if not (is_async(fs) and fs.asynchronous):
        return
    # the aiobotocore client of s3fs, and the aiohttp session of the HTTP filesystem and gcsfs
    for attribute in ("_s3", "_session"):
        session = getattr(fs, attribute, None)
        close = getattr(session, "close", None)
        if close is None:
            continue
        try:
            result = close()
            if inspect.isawaitable(result):
                await result
        except Exception as error:
            logger.debug("closing the session of %r failed: %s", fs, error)


class StorageAccess:
    """Creates and caches a native filesystem per storage location.

    S3 locations that LaminHub manages get temporary credentials from LaminHub, which are
    renewed before they expire. Other locations use the default credentials of their
    filesystem, e.g. from the environment; S3 buckets are read anonymously if there are none.

    Parameters
    ----------
    hub : LaminHub
        The LaminHub client.
    remote_options : dict of str to dict
        Options of the native filesystems by protocol, e.g. ``"gs"`` or its alias ``"gcs"``,
        or by storage root, e.g. ``"s3://bucket"``; they take precedence. Options for a
        storage root are merged over those for its protocol.
    asynchronous : bool
        Whether async filesystems are created in async mode; in sync mode, they close their
        sessions themselves.
    loop : asyncio.AbstractEventLoop or None
        The event loop that async filesystems are bound to; ``None`` for the running loop in
        async mode.
    protocols : iterable of str, default DEFAULT_STORAGE_PROTOCOLS
        The protocols of the storage locations that may be read.
    """

    def __init__(
        self,
        hub: LaminHub,
        remote_options: dict[str, dict[str, Any]],
        asynchronous: bool,
        loop: asyncio.AbstractEventLoop | None,
        protocols: Iterable[str] = DEFAULT_STORAGE_PROTOCOLS,
    ):
        self.hub = hub
        self.remote_options = remote_options
        self.protocols = frozenset(canonical_protocol(p) for p in protocols)
        self._asynchronous = asynchronous
        self._loop = loop
        self._filesystems: dict[str, tuple[AbstractFileSystem, float | None]] = {}
        self._pending: dict[str, asyncio.Future] = {}
        self._ambient_aws: bool | None = None
        self._ambient_google: bool | None = None
        # replaced filesystems in async mode, whose sessions are closed by close()
        self._retired: list[AbstractFileSystem] = []

    def clear(self) -> None:
        for fs, _ in self._filesystems.values():
            self._retire(fs)
        self._filesystems.clear()

    async def close(self) -> None:
        """Close the sessions of all filesystems, including replaced ones, in async mode."""
        filesystems = [fs for fs, _ in self._filesystems.values()] + self._retired
        self._filesystems.clear()
        self._retired = []
        for fs in filesystems:
            await close_filesystem(fs)

    def _retire(self, fs: AbstractFileSystem) -> None:
        # in sync mode, holding on to the filesystem would keep it from closing its session
        if self._asynchronous:
            self._retired.append(fs)

    async def filesystem(self, storage: Storage) -> AbstractFileSystem:
        """Get the filesystem of a storage location.

        Parameters
        ----------
        storage : Storage
            The storage location.

        Returns
        -------
        AbstractFileSystem
            The filesystem, with valid credentials.
        """
        return (await self._entry(storage))[0]

    async def run(
        self, storage: Storage, operation: Callable[[AbstractFileSystem], Awaitable[T]]
    ) -> T:
        """Run an operation with the filesystem of a storage location.

        If the credentials from LaminHub are rejected with PermissionError, e.g. because they
        were revoked or expired early, new credentials are requested and the operation is
        retried once.

        Parameters
        ----------
        storage : Storage
            The storage location.
        operation : callable
            Creates the coroutine of the operation from the filesystem.

        Returns
        -------
        Any
            The result of the operation.
        """
        fs, expires = await self._entry(storage)
        try:
            return await operation(fs)
        except PermissionError as error:
            if expires is None:
                raise
            logger.info("%s rejected the credentials from LaminHub: %s", storage.root, error)
            self._discard(storage.root, fs)
            fs, _ = await self._entry(storage)
            return await operation(fs)

    def _discard(self, root: str, fs: AbstractFileSystem) -> None:
        cached = self._filesystems.get(root)
        if cached is not None and cached[0] is fs:
            del self._filesystems[root]
            self._retire(fs)

    async def _entry(self, storage: Storage) -> tuple[AbstractFileSystem, float | None]:
        cached = self._filesystems.get(storage.root)
        if cached is not None:
            expires = cached[1]
            if expires is None or expires - time.time() > REFRESH_MARGIN:
                return cached
        future = self._pending.get(storage.root)
        if future is None:
            future = self._pending[storage.root] = asyncio.ensure_future(self._create(storage))
        try:
            entry = await asyncio.shield(future)
        finally:
            if self._pending.get(storage.root) is future and future.done():
                del self._pending[storage.root]
        replaced = self._filesystems.get(storage.root)
        if replaced is not None and replaced[0] is not entry[0]:
            self._retire(replaced[0])
        self._filesystems[storage.root] = entry
        return entry

    async def _create(self, storage: Storage) -> tuple[AbstractFileSystem, float | None]:
        protocol = storage.protocol
        if canonical_protocol(protocol) not in self.protocols:
            allowed = ", ".join(sorted(self.protocols))
            raise PermissionError(
                f"cannot access {storage.root}: the {protocol!r} protocol isn't in "
                f"storage_protocols ({allowed})"
            )
        try:
            cls = fsspec.get_filesystem_class(protocol)
        except (ImportError, ValueError) as error:
            extra = _EXTRAS.get(canonical_protocol(protocol))
            hint = f"; install it with: pip install 'fsspec-lamindb[{extra}]'" if extra else ""
            raise ImportError(f"cannot access {storage.root}: {error}{hint}") from error
        options, expires = await self.options(storage)
        options.setdefault("skip_instance_cache", True)
        # the listings are cached by LaminDBFileSystem, which also refreshes them
        options.setdefault("use_listings_cache", False)
        if getattr(cls, "async_impl", False):
            fs = cls(asynchronous=self._asynchronous, loop=self._loop, **options)
        else:
            fs = cls(**options)
        return fs, expires

    async def options(self, storage: Storage) -> tuple[dict[str, Any], float | None]:
        """Get the storage options of a location and when they expire.

        Parameters
        ----------
        storage : Storage
            The storage location.

        Returns
        -------
        options : dict
            Options of the native filesystem of the location.
        expires : float or None
            When the options expire, as a Unix timestamp; ``None`` if they don't.
        """
        protocol = storage.protocol
        user = self.user_options(storage)
        options: dict[str, Any] = {}
        expires = None
        if protocol == "s3":
            options, expires = await self._s3_options(storage, user)
        elif canonical_protocol(protocol) == "gs" and "token" not in user:
            if self._ambient_google is None:
                self._ambient_google = await asyncio.to_thread(has_ambient_google_credentials)
            if not self._ambient_google:
                # otherwise, gcsfs waits for the metadata server of Google Compute Engine
                options = {"token": "anon"}
        elif protocol in ("http", "https"):
            # aiohttp's default total timeout of 5 minutes breaks downloads of large files
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=60)
            options = {"client_kwargs": {"timeout": timeout}}
        return _merge(options, user), expires

    def user_options(self, storage: Storage) -> dict[str, Any]:
        """Get the options of ``remote_options`` that apply to a storage location.

        Parameters
        ----------
        storage : Storage
            The storage location.

        Returns
        -------
        dict
            The options for the protocol of the location, with the options for its root
            merged over them.
        """
        protocol = canonical_protocol(storage.protocol)
        by_protocol: dict[str, Any] = {}
        by_root: dict[str, Any] = {}
        for key, value in self.remote_options.items():
            if "/" in key:
                if key.rstrip("/") == storage.root:
                    by_root = _merge(by_root, value)
            elif canonical_protocol(key) == protocol:
                by_protocol = _merge(by_protocol, value)
        return _merge(by_protocol, by_root)

    async def _s3_options(
        self, storage: Storage, user: dict[str, Any]
    ) -> tuple[dict[str, Any], float | None]:
        options: dict[str, Any] = {}
        if storage.region:
            options["client_kwargs"] = {"region_name": storage.region}
        if _S3_CREDENTIAL_OPTIONS & set(user):
            return options, None
        access = None
        try:
            access = await self.hub.storage_credentials(storage.root)
        except (OSError, aiohttp.ClientError) as error:
            logger.warning("no LaminHub credentials for %s: %s", storage.root, error)
        if access is not None:
            credentials = access["Credentials"]
            options.update(
                key=credentials["AccessKeyId"],
                secret=credentials["SecretAccessKey"],
                token=credentials.get("SessionToken"),
            )
            return options, _expiry(credentials.get("Expiration"), storage.root)
        if self._ambient_aws is None:
            self._ambient_aws = await asyncio.to_thread(has_ambient_aws_credentials)
        if not self._ambient_aws:
            options["anon"] = True
        return options, None
