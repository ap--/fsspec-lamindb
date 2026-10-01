"""An in-process stand-in for the LaminHub REST API, LaminHub and an HTTP storage server.

Note:
  This is intended for testing purposes and needs to be verified against
  the real LaminHub REST API for accuracy when the real API is updated.
"""

from __future__ import annotations

import asyncio
import copy
import threading
from typing import Any

from aiohttp import web

INSTANCE_ID = "11111111-2222-3333-4444-555555555555"
PRIVATE_ID = "99999999-2222-3333-4444-555555555555"
API_KEY = "good-key"


def _value(record: dict[str, Any], field: str) -> Any:
    if field == "storage.id":
        return record["storage_id"]
    if field == "collections.uid":
        return record.get("_collections", [])
    return record.get(field)


def matches(record: dict[str, Any], node: dict | None) -> bool:
    if not node:
        return True
    if "and" in node:
        return all(matches(record, child) for child in node["and"])
    if "or" in node:
        return any(matches(record, child) for child in node["or"])
    ((field, condition),) = node.items()
    ((op, arg),) = condition.items()
    value = _value(record, field)
    if isinstance(value, list):
        assert op == "eq", op
        return arg in value
    if op == "eq":
        return value == arg
    if op == "in":
        return value in arg
    if op == "notin":
        # like SQL, null is not in any list
        return value is not None and value not in arg
    if op == "isnull":
        return (value is None) == arg
    if op == "startswith":
        return isinstance(value, str) and value.startswith(arg)
    raise NotImplementedError(op)


def _artifact(id: int, uid: str, storage_id: int, **fields: Any) -> dict[str, Any]:
    record = {
        "id": id,
        "uid": uid,
        "storage_id": storage_id,
        "branch_id": 1,
        "key": None,
        "suffix": "",
        "size": None,
        "hash": f"hash{id}",
        "kind": None,
        "otype": None,
        "description": None,
        "version_tag": None,
        "is_latest": True,
        "n_files": None,
        "n_observations": None,
        "_key_is_virtual": True,
        "_real_key": None,
        "_overwrite_versions": False,
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-02T00:00:00Z",
    }
    record.update(fields)
    return record


def make_records(http_root: str) -> dict[str, list[dict[str, Any]]]:
    storages = [
        {"id": 1, "uid": "stor1", "root": "s3://bucket-a", "type": "s3", "region": "us-east-1"},
        {"id": 2, "uid": "stor2", "root": "s3://bucket-a/nested", "type": "s3"},
        {"id": 3, "uid": "stor3", "root": "memory://mem-store", "type": "local"},
        {"id": 4, "uid": "stor4", "root": http_root, "type": "http"},
        {"id": 5, "uid": "stor5", "root": "s3://public-bucket", "type": "s3"},
        {"id": 6, "uid": "stor6", "root": "s3://delegated-bucket", "type": "s3"},
        {"id": 7, "uid": "stor7", "root": "s3://lamin-us-east-1/abc123", "type": "s3"},
    ]
    a = _artifact
    artifacts = [
        a(
            1,
            "aaaaaaaaaaaaaaaa0000",
            1,
            key="data/table.csv",
            suffix=".csv",
            size=12,
            is_latest=False,
            _collections=["colmycollecti0000"],
        ),
        a(2, "aaaaaaaaaaaaaaaa0001", 1, key="data/table.csv", suffix=".csv", size=8),
        a(
            3,
            "fastqfastqfastq00000",
            1,
            key="data/raw/reads.fastq",
            suffix=".fastq",
            size=12,
            _key_is_virtual=False,
            _collections=["colgroup00000000000"],
        ),
        a(
            4,
            "imagesimagesimag0000",
            1,
            key="images",
            n_files=2,
            size=12,
            _key_is_virtual=False,
            _collections=["colmycollecti0000"],
        ),
        a(5, "kkkkkkkkkkkkkkkk0000", 1, suffix=".txt", size=6, _collections=["colmycollecti0000"]),
        a(6, "rrrrrrrrrrrrrrrr0000", 1, suffix=".html", size=19, kind="__lamindb_run__"),
        a(
            7,
            "branchbranchbran0000",
            1,
            key="data/branch-only.csv",
            suffix=".csv",
            size=6,
            branch_id=2,
        ),
        a(
            8,
            "zzzzzzzzzzzzzzzz0000",
            1,
            key="zarr/store.zarr",
            suffix=".zarr",
            n_files=2,
            size=7,
            _overwrite_versions=True,
        ),
        a(9, "eeeeeeeeeeeeeeee0000", 1, key="images/extra.txt", suffix=".txt", size=13),
        a(
            10,
            "bbbbbbbbbbbbbbbb0000",
            2,
            key="inner.csv",
            suffix=".csv",
            size=6,
            _collections=["colmycollecti0000"],
        ),
        a(
            11,
            "memmemmemmemmemm0000",
            3,
            key="mem/file.txt",
            suffix=".txt",
            size=9,
            _key_is_virtual=False,
            _collections=["colsubcollecti000"],
        ),
        a(
            12,
            "httphttphttphttp0000",
            4,
            key="web/page.txt",
            suffix=".txt",
            size=10,
            _key_is_virtual=False,
        ),
        a(
            13,
            "publicpublicpubl0000",
            5,
            key="data.bin",
            suffix=".bin",
            size=14,
            _key_is_virtual=False,
        ),
        a(
            14,
            "sharedsharedshar0000",
            1,
            key="shared.csv",
            suffix=".csv",
            size=7,
            _collections=["colmycollecti0000"],
        ),
        a(
            15,
            "sharedsharedshar0001",
            2,
            key="shared.csv",
            suffix=".csv",
            size=9,
            _collections=["colmycollecti0000"],
        ),
        a(16, "branchtablebranc0000", 1, key="data/table.csv", suffix=".csv", size=10, branch_id=2),
        a(
            17,
            "delegateddelegat0000",
            6,
            key="deleg.txt",
            suffix=".txt",
            size=9,
            _key_is_virtual=False,
        ),
        a(18, "trashtrashtrasht0000", 1, key="trashed.csv", suffix=".csv", size=1, branch_id=-1),
    ]
    collections = [
        {
            "id": 1,
            "uid": "colold0000000000000",
            "key": "my collection",
            "is_latest": False,
            "branch_id": 1,
        },
        {
            "id": 2,
            "uid": "colmycollecti0000",
            "key": "my collection",
            "is_latest": True,
            "branch_id": 1,
        },
        {
            "id": 3,
            "uid": "colsubcollecti000",
            "key": "group/sub collection",
            "is_latest": True,
            "branch_id": 1,
        },
        {"id": 4, "uid": "colgroup00000000000", "key": "group", "is_latest": True, "branch_id": 1},
    ]
    branches = [
        {"id": 1, "uid": "mmmmmmmmmmmm", "name": "main", "branch_id": 1},
        {"id": 2, "uid": "contribcontr", "name": "contrib", "branch_id": 1},
    ]
    for storage in storages:
        storage["branch_id"] = 1
    return {
        "storage": storages,
        "artifact": artifacts,
        "collection": collections,
        "branch": branches,
    }


S3_OBJECTS = {
    "bucket-a/.lamindb/aaaaaaaaaaaaaaaa0000.csv": b"old,version\n",
    "bucket-a/.lamindb/aaaaaaaaaaaaaaaa0001.csv": b"a,b\n1,2\n",
    "bucket-a/data/raw/reads.fastq": b"@read1\nACGT\n",
    "bucket-a/images/1.jpg": b"jpeg-1",
    "bucket-a/images/sub/2.jpg": b"jpeg-2",
    "bucket-a/.lamindb/eeeeeeeeeeeeeeee0000.txt": b"inside images",
    "bucket-a/.lamindb/kkkkkkkkkkkkkkkk0000.txt": b"no key",
    "bucket-a/.lamindb/rrrrrrrrrrrrrrrr0000.html": b"<html>report</html>",
    "bucket-a/.lamindb/zzzzzzzzzzzzzzzz.zarr/.zattrs": b"{}",
    "bucket-a/.lamindb/zzzzzzzzzzzzzzzz.zarr/0/0": b"chunk",
    "bucket-a/.lamindb/branchbranchbran0000.csv": b"branch",
    "bucket-a/.lamindb/branchtablebranc0000.csv": b"branch,table",
    "bucket-a/.lamindb/sharedsharedshar0000.csv": b"shared1",
    "bucket-a/nested/.lamindb/bbbbbbbbbbbbbbbb0000.csv": b"inner\n",
    "bucket-a/nested/.lamindb/sharedsharedshar0001.csv": b"shared2\n\n",
    "public-bucket/data.bin": b"from public s3",
    "delegated-bucket/deleg.txt": b"delegated",
}
MEMORY_FILES = {"/mem-store/mem/file.txt": b"in memory"}
HTTP_FILES = {"web/page.txt": b"from http\n"}


class FakeHub:
    """In-process stand-in for LaminHub.

    Serves the LaminHub REST API under ``/api``, LaminHub under ``/hub`` and the files of an
    HTTP storage under ``/files``.
    """

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.runner: web.AppRunner | None = None
        self.port: int | None = None
        self.calls: list[tuple[str, str, Any]] = []
        self.reset_state()

    # lifecycle

    def start(self) -> None:
        started = threading.Event()

        def run():
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self._start())
            started.set()
            self.loop.run_forever()

        threading.Thread(target=run, daemon=True).start()
        started.wait()

    async def _start(self) -> None:
        app = web.Application()
        app.router.add_post("/api/account/jwt", self.jwt)
        app.router.add_get("/api/instances", self.list_instances)
        app.router.add_get("/api/instances/{owner}/{name}/settings", self.settings)
        app.router.add_post("/api/instances/{id}/modules/{module}/{model}/group-by", self.count)
        app.router.add_post("/api/instances/{id}/modules/{module}/{model}", self.query)
        app.router.add_post("/api/storages/credentials", self.delegated_credentials)
        app.router.add_get("/hub/rest/v1/rpc/get_storage_api_info_from_path", self.api_info)
        app.router.add_post("/hub/functions/v1/get-cloud-access-v1", self.cloud_access)
        app.router.add_route("*", "/files/{path:.*}", self.files)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.port = self.runner.addresses[0][1]

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def api_url(self) -> str:
        return f"{self.base}/api"

    @property
    def hub_url(self) -> str:
        return f"{self.base}/hub"

    @property
    def http_root(self) -> str:
        return f"{self.base}/files"

    def reset_state(self) -> None:
        self.records = make_records(self.http_root if self.port else "http://unset/files")
        self.calls = []
        self.tokens: list[str] = []
        self.revoked: set[str] = set()
        self.fail_queries = 0
        # the Retry-After header of failed queries, which are then 429 instead of 502
        self.retry_after: str | None = None
        self.fail_count = False
        # API URLs in the settings of the instance and of the delegated storage, if not this API
        self.instance_api_url: str | None = None
        self.delegated_api_url: str | None = None
        # delay of record queries in seconds, and the most queries handled at once
        self.query_delay = 0.0
        # replaces the settings of the instances, if set
        self.settings_response: Any = None
        # added to the settings of the instances
        self.settings_extra: dict[str, Any] = {}
        # whether record queries are forbidden
        self.forbid_queries = False
        self.in_flight = 0
        self.max_in_flight = 0

    # helpers

    def _token(self, request: web.Request) -> str | None:
        header = request.headers.get("Authorization", "")
        token = header.removeprefix("Bearer ") or None
        return token if token in self.tokens else None

    def _unauthorized(self, request: web.Request) -> bool:
        header = request.headers.get("Authorization", "")
        return header.removeprefix("Bearer ") in self.revoked

    def _instance(self, id: str, request: web.Request) -> web.Response | None:
        if id == PRIVATE_ID and self._token(request) is None:
            return web.json_response({"detail": "not found"}, status=404)
        if id not in (INSTANCE_ID, PRIVATE_ID):
            return web.json_response({"detail": "not found"}, status=404)
        return None

    # API

    async def jwt(self, request: web.Request) -> web.Response:
        body = await request.json()
        self.calls.append(("jwt", "", None))
        if body.get("api_key") != API_KEY:
            return web.json_response({"detail": f"unknown key {body.get('api_key')}"}, status=400)
        token = f"token-{len(self.tokens) + 1}"
        self.tokens.append(token)
        return web.json_response({"accessToken": token})

    async def list_instances(self, request: web.Request) -> web.Response:
        self.calls.append(("list_instances", "", None))
        if self._token(request) is None:
            return web.json_response({"detail": "Unauthorized"}, status=401)
        data = [
            {"id": INSTANCE_ID, "owner": "owner", "name": "inst"},
            {"id": PRIVATE_ID, "owner": "owner", "name": "private"},
        ]
        return web.json_response({"status": "success", "data": data})

    async def settings(self, request: web.Request) -> web.Response:
        owner, name = request.match_info["owner"], request.match_info["name"]
        self.calls.append(("settings", f"{owner}/{name}", None))
        if self._unauthorized(request):
            return web.json_response({"detail": "expired"}, status=401)
        ids = {"owner/inst": INSTANCE_ID, "owner/private": PRIVATE_ID}
        id = ids.get(f"{owner}/{name}")
        if id is None or (id == PRIVATE_ID and self._token(request) is None):
            return web.json_response({"detail": "not found"}, status=404)
        if self.settings_response is not None:
            return web.json_response(self.settings_response)
        return web.json_response(
            {
                "id": id,
                "owner": owner,
                "name": name,
                "api_url": self.instance_api_url or self.api_url,
                "db_user_password": "secret-db-password",
                **self.settings_extra,
            }
        )

    def _select(self, request: web.Request, body: dict) -> list[dict[str, Any]]:
        model = request.match_info["model"]
        scope = (body.get("scope") or {}).get("branch_ids")
        rows = [
            record
            for record in self.records.get(model, [])
            if (not scope or record.get("branch_id", 1) in scope)
            and matches(record, body.get("filter"))
        ]
        rows.sort(key=lambda r: r["id"])
        for order in reversed(body.get("order_by") or []):
            rows.sort(key=lambda r: r[order["field"]], reverse=order["descending"])
        return rows

    async def query(self, request: web.Request) -> web.Response:
        body = await request.json()
        self.calls.append(("query", request.match_info["model"], body))
        if self._unauthorized(request):
            return web.json_response({"detail": "expired"}, status=401)
        if self.fail_queries:
            self.fail_queries -= 1
            if self.retry_after is not None:
                headers = {"Retry-After": self.retry_after}
                return web.Response(status=429, text="slow down", headers=headers)
            return web.Response(status=502, text="bad gateway")
        if self.forbid_queries:
            return web.json_response({"detail": "Forbidden"}, status=403)
        denied = self._instance(request.match_info["id"], request)
        if denied is not None:
            return denied
        assert request.query["include_foreign_keys"] == "true"
        offset, limit = int(request.query["offset"]), int(request.query["limit"])
        rows = self._select(request, body)[offset : offset + limit]
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await asyncio.sleep(self.query_delay)
        finally:
            self.in_flight -= 1
        return web.json_response([copy.deepcopy(r) for r in rows])

    async def count(self, request: web.Request) -> web.Response:
        body = await request.json()
        self.calls.append(("count", request.match_info["model"], body))
        if self.fail_count:
            return web.json_response({"detail": "boom"}, status=400)
        return web.json_response([{"n": len(self._select(request, body))}])

    # storage credentials

    def _credentials(self, root: str) -> dict[str, Any]:
        return {
            "Credentials": {
                "AccessKeyId": f"key-for-{root}",
                "SecretAccessKey": "secret",
                "SessionToken": "session",
                "Expiration": "2099-01-01T00:00:00.000Z",
            },
            "StorageAccessibility": {"storageRoot": root, "isManaged": True},
        }

    async def api_info(self, request: web.Request) -> web.Response:
        path = request.query["_path"]
        assert request.headers["apikey"] == "anon-key"
        self.calls.append(("api_info", path, request.headers.get("Authorization")))
        if path.startswith("s3://delegated-bucket"):
            api_url = self.delegated_api_url or self.api_url
            return web.json_response([{"api_url": api_url, "assume_role_arn": "arn:role"}])
        if path.startswith(("s3://bucket-a", "s3://lamin-us-east-1/")):
            return web.json_response([{"api_url": self.api_url, "assume_role_arn": None}])
        return web.json_response([])

    async def cloud_access(self, request: web.Request) -> web.Response:
        path = (await request.json())["path"]
        self.calls.append(("cloud_access", path, request.headers.get("Authorization")))
        if path.startswith("s3://lamin-us-east-1/"):
            # a hosted bucket: anonymous callers get no credentials
            if request.headers.get("Authorization") == "Bearer anon-key":
                return web.json_response({})
            return web.json_response(self._credentials("s3://lamin-us-east-1/abc123"))
        if path.startswith("s3://bucket-a"):
            return web.json_response(self._credentials(path))
        return web.json_response({})

    async def delegated_credentials(self, request: web.Request) -> web.Response:
        body = await request.json()
        self.calls.append(("delegated", body["path"], request.headers.get("Authorization")))
        if self._token(request) is None:
            return web.json_response({"detail": "Unauthorized"}, status=401)
        return web.json_response(self._credentials(body["path"]))

    # HTTP storage

    async def files(self, request: web.Request) -> web.StreamResponse:
        path = request.match_info["path"]
        data = HTTP_FILES.get(path)
        if data is None:
            return web.Response(status=404)
        headers = {"Content-Length": str(len(data)), "Accept-Ranges": "bytes"}
        if request.method == "HEAD":
            return web.Response(headers=headers)
        range_header = request.headers.get("Range")
        if range_header:
            start_text, _, end_text = range_header.removeprefix("bytes=").partition("-")
            start = int(start_text)
            end = int(end_text) + 1 if end_text else len(data)
            chunk = data[start:end]
            return web.Response(
                status=206,
                body=chunk,
                headers={"Content-Range": f"bytes {start}-{start + len(chunk) - 1}/{len(data)}"},
            )
        return web.Response(body=data)
