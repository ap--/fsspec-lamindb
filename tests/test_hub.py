from __future__ import annotations

import asyncio

import aiohttp
import pytest

from fakehub import INSTANCE_ID
from fsspec_lamindb import LaminAPIError, _hub
from fsspec_lamindb._hub import LaminHub


def client(hub, **kwargs) -> LaminHub:
    kwargs.setdefault("api_key", "good-key")
    return LaminHub(api_url=hub.api_url, hub_url=hub.hub_url, hub_anon_key="anon-key", **kwargs)


async def run(hub_client: LaminHub, coro):
    try:
        return await coro
    finally:
        await hub_client.close()


@pytest.mark.parametrize("slug", ["", "owner", "owner/", "/inst", "a/b/c"])
async def test_invalid_slug(hub, slug):
    c = client(hub)
    with pytest.raises(ValueError, match="slug"):
        await run(c, c.instance(slug))


async def test_instance_settings_are_filtered(hub):
    c = client(hub)
    instance = await run(c, c.instance("owner/inst"))
    assert instance.id == INSTANCE_ID
    assert instance.api_url == hub.api_url
    assert instance.slug == "owner/inst"
    assert not hasattr(instance, "db_user_password")


async def test_missing_and_private_instances(hub):
    anonymous = client(hub, api_key=None)
    with pytest.raises(FileNotFoundError, match="pass an api_key"):
        await run(anonymous, anonymous.instance("owner/private"))
    c = client(hub)
    assert (await run(c, c.instance("owner/private"))).name == "private"


async def test_api_key_is_exchanged_and_refreshed(hub):
    c = client(hub)
    try:
        instance = await c.instance("owner/inst")
        hub.revoked.add("token-1")
        assert await c.query(instance, "branch", branch_ids=None) != []
    finally:
        await c.close()
    assert hub.tokens == ["token-1", "token-2"]


async def test_rejected_api_key_is_not_echoed(hub):
    c = client(hub, api_key="bad-key")
    with pytest.raises(PermissionError) as error:
        await run(c, c.instance("owner/inst"))
    assert "bad-key" not in str(error.value)
    assert "<redacted>" in str(error.value)


async def test_query_pages_concurrently(hub, monkeypatch):
    monkeypatch.setattr(_hub, "PAGE_SIZE", 3)
    c = client(hub)
    try:
        instance = await c.instance("owner/inst")
        records = await c.query(instance, "artifact", filter={"key": {"isnull": False}})
    finally:
        await c.close()
    expected = [r["id"] for r in hub.records["artifact"] if r["key"] and r["branch_id"] == 1]
    assert [r["id"] for r in records] == expected
    counts = [call for call in hub.calls if call[0] == "count"]
    assert counts[0][2]["filter"] == {"key": {"isnull": False}}
    assert counts[0][2]["scope"] == {"branch_ids": [1]}


async def test_query_pages_sequentially_without_count(hub, monkeypatch):
    monkeypatch.setattr(_hub, "PAGE_SIZE", 4)
    hub.fail_count = True
    c = client(hub)
    try:
        instance = await c.instance("owner/inst")
        records = await c.query(instance, "artifact", branch_ids=None)
    finally:
        await c.close()
    assert len(records) == len(hub.records["artifact"])


async def test_records_added_while_paging_are_not_duplicated(hub, monkeypatch):
    monkeypatch.setattr(_hub, "PAGE_SIZE", 3)
    c = client(hub)
    query_page = c.query_page
    added = dict(hub.records["artifact"][0], id=100, uid="addedaddedaddedadded")

    async def adding_query_page(instance, model, *, offset=0, **kwargs):
        if offset == 3 and added not in hub.records["artifact"]:
            # with a descending order, the new record shifts all pages by one
            hub.records["artifact"].append(added)
        return await query_page(instance, model, offset=offset, **kwargs)

    monkeypatch.setattr(c, "query_page", adding_query_page)
    try:
        instance = await c.instance("owner/inst")
        records = await c.query(instance, "artifact", order_by=("-id",), branch_ids=None)
    finally:
        await c.close()
    ids = [r["id"] for r in records]
    assert len(ids) == len(set(ids))


async def test_query_limit(hub, monkeypatch):
    monkeypatch.setattr(_hub, "PAGE_SIZE", 2)
    c = client(hub)
    try:
        instance = await c.instance("owner/inst")
        assert len(await c.query(instance, "artifact", limit=1)) == 1
        assert len(await c.query(instance, "artifact", limit=5)) == 5
    finally:
        await c.close()


@pytest.mark.parametrize("max_requests", [1, 3])
async def test_concurrent_requests_are_limited(hub, max_requests):
    hub.query_delay = 0.02
    c = client(hub, max_requests=max_requests)
    try:
        instance = await c.instance("owner/inst")
        await asyncio.gather(*(c.query_page(instance, "branch") for _ in range(10)))
    finally:
        await c.close()
    assert hub.max_in_flight == max_requests


async def test_transient_errors_are_retried(hub):
    c = client(hub)
    try:
        instance = await c.instance("owner/inst")
        hub.fail_queries = 2
        assert await c.query(instance, "branch", branch_ids=None)
        hub.fail_queries = _hub.MAX_RETRIES + 1
        with pytest.raises(LaminAPIError, match="502"):
            await c.query(instance, "branch", branch_ids=None)
    finally:
        await c.close()


@pytest.mark.parametrize(
    "error",
    [
        aiohttp.ClientPayloadError("truncated"),
        aiohttp.ServerDisconnectedError("disconnected"),
        TimeoutError("timed out"),
    ],
)
async def test_failed_connections_are_retried(hub, monkeypatch, error):
    read = aiohttp.ClientResponse.read
    failures = [error] * 2

    async def failing_read(self):
        if failures:
            raise failures.pop()
        return await read(self)

    monkeypatch.setattr(aiohttp.ClientResponse, "read", failing_read)
    c = client(hub)
    assert (await run(c, c.instance("owner/inst"))).id == INSTANCE_ID
    failures.extend([error] * (_hub.MAX_RETRIES + 1))
    c = client(hub, api_key=None)
    with pytest.raises(LaminAPIError, match="failed"):
        await run(c, c.instance("owner/inst"))


async def test_retry_after_is_respected(hub, monkeypatch):
    waits = []

    def backoff(attempt, retry_after):
        waits.append(retry_after)
        return 0

    monkeypatch.setattr(_hub, "_backoff", backoff)
    hub.fail_queries = 2
    hub.retry_after = "3"
    c = client(hub)
    try:
        instance = await c.instance("owner/inst")
        assert await c.query(instance, "branch", branch_ids=None)
    finally:
        await c.close()
    assert waits == ["3", "3"]


def test_backoff(monkeypatch):
    monkeypatch.setattr(_hub.random, "random", lambda: 1.0)
    assert _hub._backoff(0, "2") == 2.0
    assert _hub._backoff(0, "120") == 30.0
    assert _hub._backoff(0, None) == 0.625
    assert _hub._backoff(2, "Wed, 21 Oct 2026 07:28:00 GMT") == 2.5


def test_error_details():
    assert _hub.Response(500, {}, b"<html>oops").detail == "<html>oops"
    assert _hub.Response(500, {}, b'["a"]').detail == "['a']"
    with pytest.raises(LaminAPIError) as error:
        _hub.raise_for_status(_hub.Response(500, {}, b'{"detail": "boom"}'), "Doing")
    assert str(error.value) == "Doing failed (500): boom"
    assert error.value.status == 500
    with pytest.raises(PermissionError, match="401"):
        _hub.raise_for_status(_hub.Response(401, {}, b""), "Doing")


@pytest.mark.parametrize("settings", [[], {"name": "inst"}, {"id": None}])
async def test_unexpected_instance_settings(hub, settings):
    hub.settings_response = settings
    c = client(hub)
    with pytest.raises(LaminAPIError, match="unexpected settings"):
        await run(c, c.instance("owner/inst"))


@pytest.mark.parametrize("fine_grained", [False, True])
async def test_forbidden_queries_of_fine_grained_instances_have_a_hint(hub, fine_grained):
    hub.settings_extra = {"fine_grained_access": fine_grained}
    hub.forbid_queries = True
    c = client(hub)
    try:
        instance = await c.instance("owner/inst")
        assert instance.fine_grained_access is fine_grained
        with pytest.raises(PermissionError) as error:
            await c.query(instance, "artifact")
    finally:
        await c.close()
    assert ("fine-grained access" in str(error.value)) is fine_grained


async def test_list_instances(hub):
    c = client(hub)
    assert [i["name"] for i in await run(c, c.list_instances())] == ["inst", "private"]
    anonymous = client(hub, api_key=None)
    assert await run(anonymous, anonymous.list_instances()) == []


async def test_storage_credentials(hub):
    c = client(hub)
    try:
        managed = await c.storage_credentials("s3://bucket-a")
        delegated = await c.storage_credentials("s3://delegated-bucket")
        unmanaged = await c.storage_credentials("s3://public-bucket")
    finally:
        await c.close()
    assert managed is not None
    assert delegated is not None
    assert managed["Credentials"]["AccessKeyId"] == "key-for-s3://bucket-a"
    assert delegated["Credentials"]["AccessKeyId"] == "key-for-s3://delegated-bucket"
    assert unmanaged is None
    (call,) = [call for call in hub.calls if call[0] == "delegated"]
    assert call[2] == "Bearer token-1"


@pytest.mark.parametrize(
    "url",
    ["ftp://example.com", "https://user:pw@example.com/api", "https://example.com/api?x=1"],
)
def test_invalid_delegated_credentials_api(url):
    with pytest.raises(LaminAPIError):
        _hub._delegated_credentials_url(url)


@pytest.mark.parametrize(
    ("url", "trusted_hosts"),
    [
        ("http://127.0.0.1:1/api/instances", ()),
        ("https://aws.eu-central-1.lamin.ai/api/instances/x", ()),
        ("https://lamin.ai/api", ()),
        ("https://onprem.example.com/api", ("onprem.example.com",)),
        ("https://a.corp.example/api", ("*.corp.example",)),
        ("http://localhost:8000/api", ("localhost",)),
    ],
)
def test_trusted_token_destinations(url, trusted_hosts):
    c = LaminHub(api_key="k", api_url="http://127.0.0.1:1/api", trusted_hosts=trusted_hosts)
    c._check_token_destination(url)


@pytest.mark.parametrize(
    ("url", "match"),
    [
        ("http://aws.us-west-2.lamin.ai/api", "HTTPS"),
        ("https://onprem.example.com/api", "trusted_hosts=\\['onprem.example.com'\\]"),
        ("https://lamin.ai.example.com/api", "trusted_hosts"),
        ("https://corp.example/api", "trusted_hosts"),
        ("https://user:pw@x.lamin.ai/api", "credentials"),
    ],
)
def test_untrusted_token_destinations(url, match):
    c = LaminHub(api_key="k", api_url="http://127.0.0.1:1/api", trusted_hosts=["*.corp.example"])
    with pytest.raises(PermissionError, match=match):
        c._check_token_destination(url)


async def test_token_is_not_sent_to_an_untrusted_instance_api(hub):
    hub.instance_api_url = f"http://localhost:{hub.port}/api"
    c = client(hub)
    try:
        instance = await c.instance("owner/inst")
        with pytest.raises(PermissionError, match="trusted_hosts"):
            await c.query(instance, "branch", branch_ids=None)
    finally:
        await c.close()
    assert not [call for call in hub.calls if call[0] == "query"]
    trusting = client(hub, trusted_hosts=["localhost"])
    instance = await trusting.instance("owner/inst")
    assert await run(trusting, trusting.query(instance, "branch", branch_ids=None))
    # anonymous callers send no token
    anonymous = client(hub, api_key=None)
    instance = await anonymous.instance("owner/inst")
    assert await run(anonymous, anonymous.query(instance, "branch", branch_ids=None))


async def test_token_is_not_sent_to_an_untrusted_credentials_api(hub):
    hub.delegated_api_url = f"http://localhost:{hub.port}/api"
    c = client(hub)
    with pytest.raises(PermissionError, match="trusted_hosts"):
        await run(c, c.storage_credentials("s3://delegated-bucket"))
    assert not [call for call in hub.calls if call[0] == "delegated"]


def test_delegated_credentials_url():
    assert (
        _hub._delegated_credentials_url("https://example.com/api/")
        == "https://example.com/api/storages/credentials"
    )
    assert _hub._delegated_credentials_url("http://127.0.0.1:8000/api").startswith("http://")
