"""Async client for the parts of the LaminHub REST API that the filesystem uses."""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import random
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlsplit

import aiohttp

from ._version import __version__

logger = logging.getLogger(__name__)

# Note:
#   Ideally these settings would directly come from lamindb-setup,
#   but the default dependencies on lamindb-setup have grown significantly.
#   Since they are all public we just have to ensure that they are kept
#   up-to-date with the values used in lamindb-setup.
#   `just test-lamindb-setup` checks DEFAULT_HUB_URL and DEFAULT_HUB_ANON_KEY;
#   lamindb-setup has no default for DEFAULT_API_URL.
DEFAULT_API_URL = "https://aws.us-east-1.lamin.ai/api"
DEFAULT_HUB_URL = "https://hub.lamin.ai"
# LaminHub's public Supabase key; lamindb-setup ships the same key, it isn't a secret
DEFAULT_HUB_ANON_KEY = "sb_publishable_YVa4h8hQ-yBhXpfa2cP39w_PhoLW6Nu"
# the API returns at most 200 records per request
PAGE_SIZE = 200
MAIN_BRANCH_ID = 1
# the access token may always be sent to Lamin's own deployments
LAMIN_HOSTS = ("lamin.ai", "*.lamin.ai")
MAX_RETRIES = 4
_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
# instance settings fields that are kept; the response also contains database credentials
_SETTINGS_FIELDS = ("id", "lnid", "owner", "name", "api_url", "public", "fine_grained_access")


class LaminAPIError(OSError):
    """An unexpected response of the LaminHub REST API.

    Parameters
    ----------
    message : str
        The error message.
    status : int, optional
        The HTTP status code of the response.

    Attributes
    ----------
    status : int or None
        The HTTP status code of the response, if any.
    """

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class Instance:
    """A LaminDB instance as returned by the instance settings endpoint.

    Attributes
    ----------
    owner : str
        Handle of the account or organization that owns the instance.
    name : str
        Name of the instance.
    id : str
        Id of the instance.
    api_url : str
        URL of the REST API that serves the instance.
    lnid : str or None
        Short uid of the instance.
    public : bool or None
        Whether the instance is public.
    fine_grained_access : bool or None
        Whether access to the records of the instance is managed per space.
    """

    owner: str
    name: str
    id: str
    api_url: str
    lnid: str | None = None
    public: bool | None = None
    fine_grained_access: bool | None = None

    @property
    def slug(self) -> str:
        return f"{self.owner}/{self.name}"


@dataclass
class Response:
    status: int
    headers: Any
    body: bytes

    @property
    def ok(self) -> bool:
        return self.status < 300

    def json(self) -> Any:
        return json.loads(self.body or b"null")

    @property
    def detail(self) -> str:
        try:
            data = self.json()
        except ValueError:
            return self.body[:500].decode(errors="replace")
        if isinstance(data, dict) and "detail" in data:
            return str(data["detail"])[:500]
        return str(data)[:500]


def raise_for_status(response: Response, what: str, forbidden_hint: str = "") -> None:
    if response.ok:
        return
    message = f"{what} failed ({response.status}): {response.detail}"
    if response.status == 404:
        raise FileNotFoundError(message)
    if response.status == 403:
        raise PermissionError(message + forbidden_hint)
    if response.status == 401:
        raise PermissionError(message)
    raise LaminAPIError(message, response.status)


def _forbidden_hint(instance: Instance) -> str:
    if not instance.fine_grained_access:
        return ""
    return (
        f"; {instance.slug} has fine-grained access, so your account may only be able to "
        "read the records of some spaces"
    )


def split_slug(slug: str) -> tuple[str, str]:
    owner, sep, name = str(slug).strip().strip("/").partition("/")
    if not sep or not owner or not name or "/" in name:
        raise ValueError(f"instance must be a slug 'owner/name', got {slug!r}")
    return owner, name


def _unique(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[Any] = set()
    unique = []
    for record in records:
        id = record.get("id")
        if id is not None:
            if id in seen:
                continue
            seen.add(id)
        unique.append(record)
    return unique


def _backoff(attempt: int, retry_after: str | None) -> float:
    if retry_after:
        try:
            return min(float(retry_after), 30.0)
        except ValueError:
            pass
    return 0.5 * 2**attempt * (1 + random.random() / 4)  # noqa: S311 - jitter, not crypto


def _delegated_credentials_url(api_url: str) -> str:
    """Return the URL of the credential endpoint of a storage's API.

    Parameters
    ----------
    api_url : str
        The API URL of the storage.

    Returns
    -------
    str
        The URL of the credential endpoint.

    Raises
    ------
    LaminAPIError
        If ``api_url`` isn't an HTTP(S) URL, or has credentials, a query or a fragment.
    """
    parts = urlsplit(api_url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise LaminAPIError(f"invalid storage credential API {api_url}")
    if parts.username or parts.password or parts.query or parts.fragment:
        raise LaminAPIError(f"invalid storage credential API {api_url}")
    return api_url.rstrip("/") + "/storages/credentials"


def _is_loopback(host: str) -> bool:
    try:
        return host == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _host_matches(host: str, patterns: Iterable[str]) -> bool:
    """Check whether a host matches a host name, or a pattern like ``"*.example.com"``.

    Parameters
    ----------
    host : str
        The host, in lower case.
    patterns : iterable of str
        Host names and patterns.

    Returns
    -------
    bool
        Whether ``host`` matches any of ``patterns``.
    """
    for pattern in patterns:
        pattern = pattern.strip().lower()
        if host == pattern or (pattern.startswith("*.") and host.endswith(pattern[1:])):
            return True
    return False


class LaminHub:
    """Async client for LaminHub, shared by all instances a filesystem accesses.

    Parameters
    ----------
    api_key : str, optional
        Lamin API key. Without one, only public instances are accessible.
    api_url : str, default "https://aws.us-east-1.lamin.ai/api"
        LaminHub REST API used to look up instances. Requests for an instance go to the API
        URL in its settings.
    hub_url : str, default "https://hub.lamin.ai"
        LaminHub URL used to obtain storage credentials.
    hub_anon_key : str, optional
        LaminHub's public key; defaults to the key of ``hub.lamin.ai``.
    timeout : float, default 60
        Timeout of each HTTP request in seconds.
    max_concurrency : int, default 8
        Maximum number of pages of a query fetched concurrently.
    max_requests : int, default 16
        Maximum number of concurrent requests to LaminHub and its REST API.
    trusted_hosts : sequence of str, default ()
        Hosts besides the one of ``api_url`` and ``*.lamin.ai`` that the access token may be
        sent to, e.g. of on-premises deployments; ``"*.example.com"`` matches subdomains.
    """

    def __init__(
        self,
        api_key: str | None = None,
        api_url: str = DEFAULT_API_URL,
        hub_url: str = DEFAULT_HUB_URL,
        hub_anon_key: str = DEFAULT_HUB_ANON_KEY,
        timeout: float = 60,
        max_concurrency: int = 8,
        trusted_hosts: Sequence[str] = (),
        max_requests: int = 16,
    ):
        self.api_url = api_url.rstrip("/")
        self.trusted_hosts = tuple(trusted_hosts)
        self.hub_url = hub_url.rstrip("/")
        self.hub_anon_key = hub_anon_key
        self.timeout = timeout
        self.max_concurrency = max(1, int(max_concurrency))
        self.max_requests = max(1, int(max_requests))
        self._requests: asyncio.Semaphore | None = None
        self._api_key = api_key or None
        self._access_token: str | None = None
        self._token_lock: asyncio.Lock | None = None
        self._session: aiohttp.ClientSession | None = None
        self._instances: dict[str, asyncio.Future] = {}

    def __repr__(self) -> str:
        auth = "api_key" if self._api_key else "anonymous"
        return f"LaminHub(api_url={self.api_url!r}, {auth})"

    @property
    def authenticated(self) -> bool:
        return self._api_key is not None

    # HTTP

    async def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                headers={"User-Agent": f"fsspec-lamindb/{__version__}"},
            )
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def send(self, method: str, url: str, **kwargs: Any) -> Response:
        """Send a request, retrying transient failures.

        All requests of this client are reads or idempotent lookups, so retrying them is
        safe.

        Parameters
        ----------
        method : str
            The HTTP method.
        url : str
            The URL.
        **kwargs
            Options of :meth:`aiohttp.ClientSession.request`.

        Returns
        -------
        Response
            The response.

        Raises
        ------
        LaminAPIError
            If every attempt fails to connect or times out.
        """
        session = await self.session()
        if self._requests is None:
            self._requests = asyncio.Semaphore(self.max_requests)
        for attempt in range(MAX_RETRIES + 1):
            try:
                # the semaphore isn't held while waiting to retry, and redirects aren't
                # followed, so that the access token never leaves the API
                async with (
                    self._requests,
                    session.request(method, url, allow_redirects=False, **kwargs) as r,
                ):
                    response = Response(r.status, r.headers.copy(), await r.read())
            except (
                TimeoutError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientPayloadError,
            ) as error:
                if attempt == MAX_RETRIES:
                    raise LaminAPIError(f"{method} {url} failed: {error!r}") from error
                await asyncio.sleep(_backoff(attempt, None))
                continue
            if response.status in _RETRY_STATUSES and attempt < MAX_RETRIES:
                await asyncio.sleep(_backoff(attempt, response.headers.get("Retry-After")))
                continue
            return response
        raise AssertionError("unreachable")  # pragma: no cover

    async def request(
        self, method: str, url: str, *, headers: dict[str, str] | None = None, **kwargs: Any
    ) -> Response:
        """Send a request with the access token.

        An expired access token is refreshed once.

        Parameters
        ----------
        method : str
            The HTTP method.
        url : str
            The URL.
        headers : dict of str to str, optional
            Headers of the request.
        **kwargs
            Options of :meth:`aiohttp.ClientSession.request`.

        Returns
        -------
        Response
            The response.
        """
        headers = dict(headers or {})
        if self._api_key is not None:
            self._check_token_destination(url)
        for attempt in range(2):
            token = await self.token()
            if token is not None:
                headers["Authorization"] = f"Bearer {token}"
            response = await self.send(method, url, headers=headers, **kwargs)
            if response.status == 401 and token is not None and attempt == 0:
                await self._refresh_token(token)
                continue
            return response
        raise AssertionError("unreachable")  # pragma: no cover

    # authentication

    def _check_token_destination(self, url: str) -> None:
        """Check that the access token may be sent to a URL.

        The token gives access to the whole account. Besides the configured API, it's only
        sent with HTTPS (or HTTP on the loopback interface) to hosts of Lamin, the host of the
        configured API, and ``trusted_hosts``. The API URLs of instances and storages come from
        their settings.

        Parameters
        ----------
        url : str
            The URL of the request.

        Raises
        ------
        PermissionError
            If the access token may not be sent to ``url``.
        """
        if url == self.api_url or url.startswith(self.api_url + "/"):
            return
        parts = urlsplit(url)
        host = (parts.hostname or "").lower()
        if parts.scheme != "https" and not (parts.scheme == "http" and _is_loopback(host)):
            raise PermissionError(f"refusing to send the access token without HTTPS to {url}")
        if parts.username or parts.password:
            raise PermissionError("refusing to send the access token to a URL with credentials")
        api_host = (urlsplit(self.api_url).hostname or "").lower()
        if host != api_host and not _host_matches(host, (*LAMIN_HOSTS, *self.trusted_hosts)):
            raise PermissionError(
                f"refusing to send the access token to {host}, which is neither a Lamin host "
                f"nor the host of the configured API; pass trusted_hosts=[{host!r}] to trust it"
            )

    def _lock(self) -> asyncio.Lock:
        if self._token_lock is None:
            self._token_lock = asyncio.Lock()
        return self._token_lock

    async def token(self) -> str | None:
        if self._api_key is None:
            return None
        async with self._lock():
            if self._access_token is None:
                self._access_token = await self._exchange_api_key()
            return self._access_token

    async def _refresh_token(self, stale: str) -> None:
        async with self._lock():
            if self._access_token == stale:
                self._access_token = await self._exchange_api_key()

    async def _exchange_api_key(self) -> str:
        api_key = self._api_key
        if api_key is None:
            raise PermissionError("no API key to exchange for an access token")
        response = await self.send("POST", f"{self.api_url}/account/jwt", json={"api_key": api_key})
        if not response.ok:
            detail = response.detail.replace(api_key, "<redacted>")
            raise PermissionError(f"LaminHub rejected the API key ({response.status}): {detail}")
        data = response.json()
        for field in ("accessToken", "access_token", "token"):
            token = data.get(field) if isinstance(data, dict) else None
            if isinstance(token, str) and token:
                return token
        raise LaminAPIError("the API key exchange response did not include an access token")

    # instances

    async def instance(self, slug: str) -> Instance:
        """Look up the settings of an instance.

        Concurrent lookups of the same instance are shared, and the result is cached.

        Parameters
        ----------
        slug : str
            The instance as ``"<owner>/<name>"``.

        Returns
        -------
        Instance
            The instance.

        Raises
        ------
        ValueError
            If ``slug`` isn't of the form ``"<owner>/<name>"``.
        FileNotFoundError
            If the instance doesn't exist or isn't accessible.
        PermissionError
            If LaminHub rejects the API key.
        """
        owner, name = split_slug(slug)
        key = f"{owner}/{name}"
        future = self._instances.get(key)
        if future is None:
            future = self._instances[key] = asyncio.ensure_future(self._lookup(owner, name))
        try:
            return await asyncio.shield(future)
        except BaseException:
            if self._instances.get(key) is future and future.done():
                del self._instances[key]
            raise

    async def _lookup(self, owner: str, name: str) -> Instance:
        url = f"{self.api_url}/instances/{quote(owner, safe='')}/{quote(name, safe='')}/settings"
        response = await self.request("GET", url)
        if response.status == 404:
            hint = "" if self._api_key else "; pass an api_key for private instances"
            raise FileNotFoundError(
                f"LaminDB instance {owner}/{name} does not exist or is not accessible{hint}"
            )
        raise_for_status(response, f"Looking up instance {owner}/{name}")
        settings = response.json()
        if not isinstance(settings, dict) or settings.get("id") is None:
            raise LaminAPIError(f"unexpected settings of instance {owner}/{name}: {settings!r}")
        data = {key: settings.get(key) for key in _SETTINGS_FIELDS}
        return Instance(
            owner=data.get("owner") or owner,
            name=data.get("name") or name,
            id=str(data["id"]),
            api_url=str(data.get("api_url") or self.api_url).rstrip("/"),
            lnid=data.get("lnid"),
            public=data.get("public"),
            fine_grained_access=data.get("fine_grained_access"),
        )

    async def list_instances(self) -> list[dict[str, Any]]:
        """List the instances that the account can access.

        Returns
        -------
        list of dict
            The instances; empty for anonymous callers, who can't list instances.
        """
        if self._api_key is None:
            return []
        response = await self.request("GET", f"{self.api_url}/instances")
        raise_for_status(response, "Listing instances")
        data = response.json()
        if isinstance(data, dict):
            data = data.get("data") or []
        return [item for item in data if isinstance(item, dict)]

    # records

    @staticmethod
    def _model_url(instance: Instance, model: str, module: str) -> str:
        return (
            f"{instance.api_url}/instances/{instance.id}/modules/"
            f"{quote(module, safe='')}/{quote(model, safe='')}"
        )

    @staticmethod
    def _scoped(
        body: dict[str, Any], filter: dict | None, branch_ids: Sequence[int] | None
    ) -> dict[str, Any]:
        if filter:
            body["filter"] = filter
        if branch_ids:
            body["scope"] = {"branch_ids": list(branch_ids)}
        return body

    async def query_page(
        self,
        instance: Instance,
        model: str,
        *,
        filter: dict | None = None,
        limit: int | None = None,
        offset: int = 0,
        order_by: Sequence[str] = ("id",),
        branch_ids: Sequence[int] | None = (MAIN_BRANCH_ID,),
        module: str = "core",
    ) -> list[dict[str, Any]]:
        """Query one page of records.

        Parameters
        ----------
        instance : Instance
            The instance to query.
        model : str
            Name of the model, e.g. ``"artifact"``.
        filter : dict, optional
            Filter of the records in the syntax of the REST API.
        limit : int, optional
            Maximum number of records; defaults to the page size of 200.
        offset : int, default 0
            Number of records to skip.
        order_by : sequence of str, default ("id",)
            Fields to sort by; fields prefixed with ``-`` sort descending.
        branch_ids : sequence of int or None, default (1,)
            Ids of the branches to query, by default only ``main``; ``None`` for all.
        module : str, default "core"
            Module of the model.

        Returns
        -------
        list of dict
            The records.

        Raises
        ------
        LaminAPIError
            If the response is unexpected.
        """
        body = self._scoped({}, filter, branch_ids)
        if order_by:
            body["order_by"] = [
                {"field": field.lstrip("-"), "descending": field.startswith("-")}
                for field in order_by
            ]
        response = await self.request(
            "POST",
            self._model_url(instance, model, module),
            params={
                "limit": str(limit or PAGE_SIZE),
                "offset": str(offset),
                "include_foreign_keys": "true",
            },
            json=body,
        )
        raise_for_status(
            response,
            f"Querying {module}.{model} records of {instance.slug}",
            _forbidden_hint(instance),
        )
        data = response.json()
        if not isinstance(data, list):
            raise LaminAPIError(f"unexpected response when querying {model} records: {data!r}")
        return data

    async def count(
        self,
        instance: Instance,
        model: str,
        *,
        filter: dict | None = None,
        branch_ids: Sequence[int] | None = (MAIN_BRANCH_ID,),
        module: str = "core",
    ) -> int:
        body = self._scoped(
            {
                "dimensions": [],
                "measures": [{"field_name": "id", "agg_func": "count", "alias": "n"}],
            },
            filter,
            branch_ids,
        )
        url = self._model_url(instance, model, module) + "/group-by"
        response = await self.request("POST", url, json=body)
        raise_for_status(
            response,
            f"Counting {module}.{model} records of {instance.slug}",
            _forbidden_hint(instance),
        )
        rows = response.json()
        return int(rows[0]["n"]) if rows else 0

    async def query(
        self,
        instance: Instance,
        model: str,
        *,
        filter: dict | None = None,
        limit: int | None = None,
        order_by: Sequence[str] = ("id",),
        branch_ids: Sequence[int] | None = (MAIN_BRANCH_ID,),
        module: str = "core",
    ) -> list[dict[str, Any]]:
        """Query all records matching ``filter``, fetching pages concurrently.

        Parameters
        ----------
        instance : Instance
            The instance to query.
        model : str
            Name of the model, e.g. ``"artifact"``.
        filter : dict, optional
            Filter of the records in the syntax of the REST API.
        limit : int, optional
            Maximum number of records; ``None`` for all.
        order_by : sequence of str, default ("id",)
            Fields to sort by; fields prefixed with ``-`` sort descending.
        branch_ids : sequence of int or None, default (1,)
            Ids of the branches to query, by default only ``main``; ``None`` for all.
        module : str, default "core"
            Module of the model.

        Returns
        -------
        list of dict
            The records.
        """
        kwargs: dict[str, Any] = dict(
            filter=filter, order_by=order_by, branch_ids=branch_ids, module=module
        )
        if limit is not None and limit <= PAGE_SIZE:
            return await self.query_page(instance, model, limit=limit, **kwargs)
        records = await self.query_page(instance, model, **kwargs)
        if len(records) < PAGE_SIZE:
            return records
        total: int | None = None
        if self.max_concurrency > 1:
            try:
                total = await self.count(
                    instance, model, filter=filter, branch_ids=branch_ids, module=module
                )
            except OSError as error:
                logger.debug("counting %s records failed, paging sequentially: %s", model, error)
        if total is not None and limit is not None:
            total = min(total, limit)
        offset = PAGE_SIZE
        last_page_full = True
        if total is not None and total > offset:
            semaphore = asyncio.Semaphore(self.max_concurrency)

            async def page(offset: int) -> list[dict[str, Any]]:
                async with semaphore:
                    return await self.query_page(instance, model, offset=offset, **kwargs)

            offsets = list(range(offset, total, PAGE_SIZE))
            pages = await asyncio.gather(*(page(o) for o in offsets))
            for page_records in pages:
                records.extend(page_records)
            offset = offsets[-1] + PAGE_SIZE
            last_page_full = len(pages[-1]) == PAGE_SIZE
        # without a count, page on until a page isn't full
        while (
            last_page_full
            and (total is None or offset < total)
            and (limit is None or len(records) < limit)
        ):
            page_records = await self.query_page(instance, model, offset=offset, **kwargs)
            records.extend(page_records)
            offset += PAGE_SIZE
            last_page_full = len(page_records) == PAGE_SIZE
        # records added or deleted while paging shift the pages: a record can then appear
        # twice, or be skipped
        records = _unique(records)
        return records if limit is None else records[:limit]

    # storage credentials

    async def _hub_request(self, method: str, path: str, **kwargs: Any) -> Response:
        """Send a request to LaminHub, authenticated with the access token if there is one.

        Parameters
        ----------
        method : str
            The HTTP method.
        path : str
            Path of the URL below ``hub_url``.
        **kwargs
            Options of :meth:`aiohttp.ClientSession.request`.

        Returns
        -------
        Response
            The response.
        """
        for attempt in range(2):
            token = await self.token()
            headers = {
                "apikey": self.hub_anon_key,
                "Authorization": f"Bearer {token or self.hub_anon_key}",
            }
            response = await self.send(method, f"{self.hub_url}{path}", headers=headers, **kwargs)
            if response.status == 401 and token is not None and attempt == 0:
                await self._refresh_token(token)
                continue
            return response
        raise AssertionError("unreachable")  # pragma: no cover

    async def storage_credentials(self, uri: str) -> dict[str, Any] | None:
        """Request the temporary cloud credentials that LaminHub grants for a storage location.

        Parameters
        ----------
        uri : str
            Root of the storage location, e.g. ``"s3://bucket"``.

        Returns
        -------
        dict or None
            The response with ``Credentials`` and ``StorageAccessibility``; ``None`` if
            LaminHub doesn't manage access to the location.
        """
        response = await self._hub_request(
            "GET", "/rest/v1/rpc/get_storage_api_info_from_path", params={"_path": uri}
        )
        raise_for_status(response, f"Looking up the credential API of {uri}")
        infos = response.json()
        info = infos[0] if isinstance(infos, list) and infos else {}
        if info.get("api_url") and info.get("assume_role_arn"):
            url = _delegated_credentials_url(info["api_url"])
            response = await self.request("POST", url, json={"path": uri, "duration_seconds": 3600})
        else:
            response = await self._hub_request(
                "POST", "/functions/v1/get-cloud-access-v1", json={"path": uri}
            )
        raise_for_status(response, f"Requesting credentials for {uri}")
        data = response.json()
        if not isinstance(data, dict) or not data.get("Credentials"):
            return None
        return data
