from __future__ import annotations

import datetime
import importlib.metadata
import pickle
import time
from typing import Any

import fsspec
import pytest
from fsspec.implementations.http import HTTPFileSystem

import fsspec_lamindb
from conftest import PROTOCOLS, FakeS3, make_fs
from fsspec_lamindb import LaminDBFileSystem
from fsspec_lamindb.core import _close_session

INST = "owner/inst"
U = f"{INST}/artifact"
# artifacts of the fake LaminHub by uid
TABLE = f"{U}/aaaaaaaaaaaaaaaa0001"
READS = f"{U}/fastqfastqfastq00000"
IMAGES = f"{U}/imagesimagesimag0000"
ZARR = f"{U}/zzzzzzzzzzzzzzzz0000"
INNER = f"{U}/bbbbbbbbbbbbbbbb0000"
MEM = f"{U}/memmemmemmemmemm0000"
WEB = f"{U}/httphttphttphttp0000"
PUBLIC = f"{U}/publicpublicpubl0000"
DELEGATED = f"{U}/delegateddelegat0000"
NEW = f"{U}/newnewnewnewnewn0000"


def names(entries):
    return [entry["name"] if isinstance(entry, dict) else entry for entry in entries]


# URIs


def test_version():
    assert fsspec_lamindb.__version__ == importlib.metadata.version("fsspec-lamindb")


def test_registered_protocol():
    assert fsspec.get_filesystem_class("lamin") is LaminDBFileSystem


def test_strip_protocol():
    assert LaminDBFileSystem._strip_protocol("lamin://owner/inst/artifact/") == U
    assert LaminDBFileSystem._strip_protocol(["lamin://", "/owner"]) == ["", "owner"]


def test_url_to_fs(hub):
    fs, path = fsspec.core.url_to_fs(
        f"lamin://{TABLE}",
        api_key="good-key",
        api_url=hub.api_url,
        hub_url=hub.hub_url,
        hub_anon_key="anon-key",
        skip_instance_cache=True,
    )
    assert path == TABLE
    assert fs.cat_file(path) == b"a,b\n1,2\n"


def test_fsspec_open(hub):
    options = dict(api_key="good-key", api_url=hub.api_url, hub_url=hub.hub_url)
    with fsspec.open(f"lamin://{TABLE}", "rt", hub_anon_key="anon-key", **options) as f:
        assert f.read() == "a,b\n1,2\n"


# owners and instances


def test_accessible_instances(lamin):
    assert lamin.ls("", detail=False) == ["owner"]
    assert lamin.ls("lamin://owner", detail=False) == ["owner/inst", "owner/private"]


def test_anonymous_cannot_list_instances_but_open_public_ones(hub):
    fs = make_fs(hub, api_key=None)
    assert fs.ls("", detail=False) == []
    with pytest.raises(FileNotFoundError, match="api_key"):
        fs.ls("owner")
    assert not fs.exists("owner")
    assert fs.ls(INST, detail=False) == [U]
    with pytest.raises(FileNotFoundError, match="api_key"):
        fs.ls("owner/private")


def test_owners_exist_only_if_they_have_accessible_instances(lamin, hub):
    assert lamin.isdir("")
    assert lamin.isdir("owner")
    assert lamin.info("lamin://owner")["type"] == "directory"
    assert not lamin.exists("no-such-owner")
    assert not lamin.isdir("no-such-owner")
    with pytest.raises(FileNotFoundError, match="lamin://no-such-owner/<instance>"):
        lamin.ls("no-such-owner")
    # the accessible instances are only listed once
    lamin.ls("")
    assert len([c for c in hub.calls if c[0] == "list_instances"]) == 1


def test_instance(lamin):
    assert lamin.ls(INST, detail=False) == [U]
    assert lamin.info(f"lamin://{U}")["type"] == "directory"
    for missing in (f"{INST}/artifacts", f"{INST}/views", f"{INST}/views/artifacts"):
        assert not lamin.exists(missing), missing
    info = lamin.info(f"lamin://{INST}")
    assert info["type"] == "directory"
    assert info["instance_id"] == "11111111-2222-3333-4444-555555555555"
    with pytest.raises(FileNotFoundError):
        lamin.info("owner/missing")
    with pytest.raises(FileNotFoundError):
        lamin.ls(f"{INST}/views")


# artifact/<uid>, compatible with nf-lamin


def test_uid_listing_has_every_version(lamin):
    listed = lamin.ls(U, detail=False)
    uids = [name.rsplit("/", 1)[-1] for name in listed]
    # all versions, artifacts without key and run reports, but only main
    assert "aaaaaaaaaaaaaaaa0000" in uids
    assert "aaaaaaaaaaaaaaaa0001" in uids
    assert "kkkkkkkkkkkkkkkk0000" in uids
    assert "rrrrrrrrrrrrrrrr0000" in uids
    assert "branchbranchbran0000" not in uids
    assert "trashtrashtrasht0000" not in uids
    assert all(len(uid) == 20 for uid in uids)


def test_full_uid_addresses_a_version(lamin):
    info = lamin.info(f"lamin://{U}/aaaaaaaaaaaaaaaa0000")
    assert info["is_latest"] is False
    assert info["name"] == f"{U}/aaaaaaaaaaaaaaaa0000"
    assert lamin.cat_file(f"{U}/aaaaaaaaaaaaaaaa0000") == b"old,version\n"
    # even on other branches
    assert lamin.cat_file(f"{U}/branchbranchbran0000") == b"branch"


def test_base_uid_addresses_the_latest_version(lamin):
    info = lamin.info(f"{U}/aaaaaaaaaaaaaaaa")
    assert info["uid"] == "aaaaaaaaaaaaaaaa0001"
    assert info["name"] == f"{U}/aaaaaaaaaaaaaaaa"
    assert lamin.cat_file(f"{U}/aaaaaaaaaaaaaaaa") == b"a,b\n1,2\n"


def test_uid_addressing_after_listing(lamin):
    lamin.ls(U)
    # not listed, but addressable
    assert lamin.cat_file(f"{U}/aaaaaaaaaaaaaaaa") == b"a,b\n1,2\n"
    with pytest.raises(FileNotFoundError):
        lamin.info(f"{U}/xxxxxxxxxxxxxxxx0000")
    with pytest.raises(FileNotFoundError):
        lamin.info(f"{U}/short")


def test_uid_subpaths_of_folders(lamin):
    assert lamin.ls(f"{U}/imagesimagesimag", detail=False) == [
        f"{U}/imagesimagesimag/1.jpg",
        f"{U}/imagesimagesimag/sub",
    ]
    assert lamin.cat_file(f"{U}/imagesimagesimag0000/sub/2.jpg") == b"jpeg-2"
    with pytest.raises(FileNotFoundError):
        lamin.info(f"{U}/aaaaaaaaaaaaaaaa0001/sub")


# artifact_uri


@pytest.mark.parametrize(
    ("path", "uri"),
    [
        (TABLE, "artifact/aaaaaaaaaaaaaaaa0001"),
        (f"lamin://{TABLE}", "artifact/aaaaaaaaaaaaaaaa0001"),
        (f"{U}/aaaaaaaaaaaaaaaa0000", "artifact/aaaaaaaaaaaaaaaa0000"),
        # folder artifacts, and files and directories inside them
        (IMAGES, "artifact/imagesimagesimag0000"),
        (f"{IMAGES}/sub", "artifact/imagesimagesimag0000/sub"),
        (f"{IMAGES}/sub/2.jpg", "artifact/imagesimagesimag0000/sub/2.jpg"),
        (f"{ZARR}/0/0", "artifact/zzzzzzzzzzzzzzzz0000/0/0"),
        # uids of version families resolve to the latest version
        (f"{U}/aaaaaaaaaaaaaaaa", "artifact/aaaaaaaaaaaaaaaa0001"),
        (f"{U}/imagesimagesimag/sub/2.jpg", "artifact/imagesimagesimag0000/sub/2.jpg"),
    ],
)
def test_artifact_uri(lamin, path, uri):
    assert lamin.artifact_uri(path) == f"lamin://{INST}/{uri}"
    family = lamin.artifact_uri(path, version_family=True)
    assert family == f"lamin://{INST}/{uri[:25]}{uri[29:]}"


def test_artifact_uri_reads_the_same_data(lamin):
    for path in (
        f"{U}/aaaaaaaaaaaaaaaa",
        f"{U}/imagesimagesimag/sub/2.jpg",
        f"{U}/memmemmemmemmemm",
    ):
        assert lamin.cat_file(lamin.artifact_uri(path)) == lamin.cat_file(path)


@pytest.mark.parametrize("path", ["", "owner", INST, U])
def test_artifact_uri_of_paths_without_artifact(lamin, path):
    with pytest.raises(ValueError, match="neither an artifact nor inside a folder artifact"):
        lamin.artifact_uri(path)


def test_artifact_uri_of_missing_paths(lamin):
    for path in (f"{U}/xxxxxxxxxxxxxxxx", f"{TABLE}/nope", f"{IMAGES}/nope.jpg"):
        with pytest.raises(FileNotFoundError):
            lamin.artifact_uri(path)


async def test_artifact_uri_in_async_mode(hub):
    async with make_fs(hub, asynchronous=True) as fs:
        uri = await fs._artifact_uri(f"{U}/aaaaaaaaaaaaaaaa")
    assert uri == f"lamin://{TABLE}"


# listing and reading


def test_listing_serves_lookups(lamin, hub):
    lamin.ls(U)
    before = len(hub.calls)
    assert lamin.info(TABLE)["size"] == 8
    assert lamin.info(f"{U}/aaaaaaaaaaaaaaaa0000")["is_latest"] is False
    assert len(hub.calls) == before


def test_ls_of_a_file_returns_the_file(lamin):
    assert names(lamin.ls(TABLE)) == [TABLE]


def test_read_file(lamin):
    path = f"lamin://{TABLE}"
    assert lamin.cat_file(path) == b"a,b\n1,2\n"
    assert lamin.cat_file(path, start=2, end=5) == b"b\n1"
    assert lamin.cat_file(path, start=-4) == b"1,2\n"
    assert lamin.cat_file(path, start=5, end=5) == b""
    with lamin.open(path) as f:
        assert f.size == 8
        assert f.uri == "s3://bucket-a/.lamindb/aaaaaaaaaaaaaaaa0001.csv"
        f.seek(4)
        assert f.read(3) == b"1,2"
    with lamin.open(path, "r") as f:
        assert f.readline() == "a,b\n"


def test_cat_many(lamin):
    out = lamin.cat([TABLE, READS])
    assert out == {
        TABLE: b"a,b\n1,2\n",
        READS: b"@read1\nACGT\n",
    }


def test_folder_artifact(lamin):
    folder = lamin.info(IMAGES)
    assert folder["type"] == "directory"
    assert folder["n_files"] == 2
    entries = {e["name"]: e for e in lamin.ls(IMAGES)}
    assert list(entries) == [f"{IMAGES}/1.jpg", f"{IMAGES}/sub"]
    assert entries[f"{IMAGES}/1.jpg"]["size"] == 6
    assert lamin.ls(f"{IMAGES}/sub", detail=False) == [f"{IMAGES}/sub/2.jpg"]
    assert lamin.cat_file(f"{IMAGES}/sub/2.jpg") == b"jpeg-2"
    # artifacts whose keys lie inside the folder aren't part of it
    assert not lamin.exists(f"{IMAGES}/extra.txt")


def test_file_in_folder_without_listing(lamin):
    info = lamin.info(f"{IMAGES}/sub/2.jpg")
    assert info["uri"] == "s3://bucket-a/images/sub/2.jpg"
    assert info["size"] == 6
    assert info["type"] == "file"
    assert lamin.isdir(f"{IMAGES}/sub")
    assert names(lamin.ls(f"{IMAGES}/sub/2.jpg")) == [f"{IMAGES}/sub/2.jpg"]
    with pytest.raises(FileNotFoundError):
        lamin.info(f"{IMAGES}/missing.jpg")


def test_folder_with_virtual_key(lamin):
    assert lamin.ls(ZARR, detail=False) == [f"{ZARR}/.zattrs", f"{ZARR}/0"]
    assert lamin.cat_file(f"{ZARR}/0/0") == b"chunk"


def test_find_searches_folders_recursively(lamin):
    assert lamin.find(IMAGES) == [f"{IMAGES}/1.jpg", f"{IMAGES}/sub/2.jpg"]
    assert lamin.find(IMAGES, withdirs=True) == [
        IMAGES,
        f"{IMAGES}/1.jpg",
        f"{IMAGES}/sub",
        f"{IMAGES}/sub/2.jpg",
    ]
    assert lamin.find(TABLE) == [TABLE]
    assert lamin.find(f"{U}/xxxxxxxxxxxxxxxx") == []
    assert lamin.glob(f"{IMAGES}/**/*.jpg") == [f"{IMAGES}/1.jpg", f"{IMAGES}/sub/2.jpg"]
    assert lamin.find(IMAGES, maxdepth=1) == [f"{IMAGES}/1.jpg"]
    assert lamin.du(IMAGES) == 12


def test_find_searches_every_artifact_of_an_instance(lamin):
    found = lamin.find(INST)
    assert found == lamin.find(U)
    assert f"{U}/aaaaaaaaaaaaaaaa0000" in found
    assert f"{U}/rrrrrrrrrrrrrrrr0000" in found
    assert f"{IMAGES}/sub/2.jpg" in found
    assert f"{ZARR}/0/0" in found
    assert lamin.glob(f"{INST}/**/2.jpg") == [f"{IMAGES}/sub/2.jpg"]
    assert lamin.du(INST) == lamin.du(U)


def test_walk(lamin):
    walked = {root: (dirs, files) for root, dirs, files in lamin.walk(IMAGES)}
    assert walked == {IMAGES: (["sub"], ["1.jpg"]), f"{IMAGES}/sub": ([], ["2.jpg"])}


# branches


def test_branch_is_shown_on_top_of_main(hub):
    # a version on the branch that is older than main's latest version
    record = dict(hub.records["artifact"][1], id=0, uid="aaaaaaaaaaaaaaaa0002", branch_id=2)
    hub.records["artifact"].append(record)
    fs = make_fs(hub, branch="contrib")
    uids = [name.rsplit("/", 1)[-1] for name in fs.ls(U, detail=False)]
    assert {"aaaaaaaaaaaaaaaa0002", "branchbranchbran0000", "branchtablebranc0000"} <= set(uids)
    # the branch's latest version wins over main's
    assert fs.info(f"{U}/aaaaaaaaaaaaaaaa")["uid"] == "aaaaaaaaaaaaaaaa0002"
    assert make_fs(hub).info(f"{U}/aaaaaaaaaaaaaaaa")["uid"] == "aaaaaaaaaaaaaaaa0001"
    assert fs.cat_file(f"{U}/branchtablebranc") == b"branch,table"
    scopes = [c[2].get("scope") for c in hub.calls if c[0] == "query" and c[1] == "artifact"]
    assert {"branch_ids": [2, 1]} in scopes


def test_trusted_hosts(hub):
    hub.instance_api_url = f"http://localhost:{hub.port}/api"
    with pytest.raises(PermissionError, match="trusted_hosts"):
        make_fs(hub).ls(U)
    assert make_fs(hub, trusted_hosts=["localhost"]).ls(U)


def test_branch_by_uid_and_unknown_branch(hub):
    assert make_fs(hub, branch="contribcontr").exists(f"{U}/branchtablebranc")
    assert not make_fs(hub).exists(f"{U}/branchtablebranc")
    with pytest.raises(FileNotFoundError, match="nope"):
        make_fs(hub, branch="nope").ls(U)


def test_storage_on_another_branch_is_found_again_after_listing(lamin, hub):
    hub.records["storage"].append(
        {"id": 8, "uid": "stor8", "root": "s3://other-bucket", "type": "s3", "branch_id": 2}
    )
    record = dict(hub.records["artifact"][2], id=50, uid="reportreportrepo0000", storage_id=8)
    record.update(key="reports/report.csv", suffix=".csv", size=4)
    hub.records["artifact"].append(record)
    FakeS3().pipe_file("s3://other-bucket/reports/report.csv", b"x,y\n")
    path = f"{U}/reportreportrepo0000"
    assert lamin.cat_file(path) == b"x,y\n"
    assert path in lamin.ls(U, detail=False)
    # now served from the listing, which only names the storage root
    assert lamin.cat_file(path) == b"x,y\n"
    with lamin.open(path) as f:
        assert f.read() == b"x,y\n"


# storage access


def test_lamin_managed_s3_uses_credentials_from_laminhub(lamin, hub):
    assert lamin.cat_file(TABLE) == b"a,b\n1,2\n"
    (options,) = FakeS3.created
    assert options["key"] == "key-for-s3://bucket-a"
    assert options["secret"] == "secret"
    assert options["token"] == "session"
    assert options["client_kwargs"] == {"region_name": "us-east-1"}
    # LaminHub is asked with the access token
    api_info = [c for c in hub.calls if c[0] == "api_info"]
    assert api_info[0][2] == "Bearer token-1"
    # the filesystem and its credentials are reused
    lamin.cat_file(READS)
    assert len(FakeS3.created) == 1


def test_rejected_laminhub_credentials_are_renewed_once(lamin, hub):
    FakeS3.permission_errors = 1
    assert lamin.cat_file(TABLE) == b"a,b\n1,2\n"
    assert len(FakeS3.created) == 2
    assert len([c for c in hub.calls if c[0] == "cloud_access"]) == 2
    FakeS3.permission_errors = 2
    with pytest.raises(PermissionError):
        lamin.cat_file(READS)


def test_other_credentials_are_not_renewed(lamin):
    FakeS3.permission_errors = 1
    with pytest.raises(PermissionError):
        lamin.cat_file(PUBLIC)
    assert len(FakeS3.created) == 1


def test_delegated_storage_credentials(lamin, hub):
    assert lamin.cat_file(DELEGATED) == b"delegated"
    (call,) = [c for c in hub.calls if c[0] == "delegated"]
    assert call[1] == "s3://delegated-bucket"
    assert FakeS3.created[0]["key"] == "key-for-s3://delegated-bucket"


def test_unmanaged_s3_is_read_anonymously_without_ambient_credentials(lamin):
    assert lamin.cat_file(PUBLIC) == b"from public s3"
    assert FakeS3.created[0]["anon"] is True
    assert "key" not in FakeS3.created[0]


def test_anonymous_callers_ask_laminhub_with_its_public_key(hub):
    fs = make_fs(hub, api_key=None)
    assert fs.cat_file(TABLE) == b"a,b\n1,2\n"
    assert FakeS3.created[0]["key"] == "key-for-s3://bucket-a"
    api_info = [c for c in hub.calls if c[0] == "api_info"]
    assert api_info[0][2] == "Bearer anon-key"


def test_remote_options_replace_laminhub_credentials(hub):
    fs = make_fs(hub, remote_options={"s3": {"profile": "mine", "client_kwargs": {"x": 1}}})
    fs.cat_file(TABLE)
    options = FakeS3.created[0]
    assert options["profile"] == "mine"
    assert "key" not in options
    assert options["client_kwargs"] == {"region_name": "us-east-1", "x": 1}
    assert not [c for c in hub.calls if c[0] in ("api_info", "cloud_access")]


def test_remote_options_by_storage_root(hub):
    remote_options = {
        "s3": {"profile": "default", "client_kwargs": {"x": 1}},
        "s3://public-bucket/": {"profile": "public", "client_kwargs": {"y": 2}},
    }
    fs = make_fs(hub, remote_options=remote_options)
    fs.cat_file(PUBLIC)
    fs.cat_file(TABLE)
    public, managed = FakeS3.created
    assert public["profile"] == "public"
    assert public["client_kwargs"] == {"x": 1, "y": 2}
    assert managed["profile"] == "default"
    assert managed["client_kwargs"] == {"region_name": "us-east-1", "x": 1}


def test_storage_filesystems_do_not_cache_listings(lamin, hub):
    lamin.cat_file(TABLE)
    assert FakeS3.created[0]["use_listings_cache"] is False
    fs = make_fs(hub, remote_options={"s3": {"use_listings_cache": True}})
    fs.cat_file(TABLE)
    assert FakeS3.created[1]["use_listings_cache"] is True


def test_http_storage_is_read_with_the_async_http_filesystem(lamin, hub):
    path = WEB
    assert lamin.info(path)["uri"] == f"{hub.http_root}/web/page.txt"
    assert lamin.cat_file(path) == b"from http\n"
    assert lamin.cat_file(path, start=5, end=9) == b"http"
    fs = lamin._storage_access._filesystems[hub.http_root][0]
    assert fs.async_impl
    assert fs.loop is lamin.loop


def test_sync_storage_filesystems(lamin):
    assert lamin.cat_file(MEM) == b"in memory"
    with lamin.open(MEM) as f:
        assert f.read(2) == b"in"


def test_missing_filesystem_implementation(hub):
    lamin = make_fs(hub, storage_protocols=[*PROTOCOLS, "nosuchproto"])
    hub.records["storage"].append(
        {"id": 8, "uid": "stor8", "root": "nosuchproto://x", "type": "x", "branch_id": 1}
    )
    hub.records["artifact"].append(
        {
            "id": 99,
            "uid": "nosuchnosuchnosu0000",
            "storage_id": 8,
            "key": "x.bin",
            "branch_id": 1,
            "is_latest": True,
            "kind": None,
            "_key_is_virtual": False,
            "suffix": ".bin",
            "size": 1,
        }
    )
    with pytest.raises(ImportError, match="nosuchproto"):
        lamin.cat_file(f"{U}/nosuchnosuchnosu0000")


def test_storage_protocols(hub, tmp_path):
    (tmp_path / "local.txt").write_bytes(b"local")
    hub.records["storage"].append(
        {"id": 8, "uid": "stor8", "root": tmp_path.as_posix(), "type": "local", "branch_id": 1}
    )
    record: dict[str, Any] = dict(hub.records["artifact"][2], storage_id=8)
    record.update(id=60, uid="locallocallocal00000")
    record.update(key="local.txt", suffix=".txt", size=5, _collections=[])
    hub.records["artifact"].append(record)
    # neither local nor in-memory storage locations are read by default
    fs = make_fs(hub, storage_protocols=None)
    with pytest.raises(PermissionError, match="storage_protocols"):
        fs.cat_file(f"{U}/locallocallocal00000")
    with pytest.raises(PermissionError, match="'memory'"):
        fs.cat_file(MEM)
    assert fs.cat_file(TABLE) == b"a,b\n1,2\n"
    # "local" is an alias of "file"
    fs = make_fs(hub, storage_protocols=["local"])
    assert fs.cat_file(f"{U}/locallocallocal00000") == b"local"
    with pytest.raises(PermissionError, match="storage_protocols"):
        fs.cat_file(TABLE)


def test_get_file_and_recursive_get(lamin, tmp_path):
    target = tmp_path / "sub" / "table.csv"
    lamin.get(TABLE, str(target))
    assert target.read_bytes() == b"a,b\n1,2\n"
    lamin.get(IMAGES, str(tmp_path / "images"), recursive=True)
    assert (tmp_path / "images" / "1.jpg").read_bytes() == b"jpeg-1"
    assert (tmp_path / "images" / "sub" / "2.jpg").read_bytes() == b"jpeg-2"


# errors, caching and misc


def test_missing_paths(lamin):
    for path in [
        f"{INST}/nope",
        f"{INST}/views",
        f"{INST}/views/artifacts",
        f"{U}/short",
        f"{U}/xxxxxxxxxxxxxxxx",
        f"{U}/xxxxxxxxxxxxxxxx0000",
        f"{TABLE}/nope",
        f"{IMAGES}/nope",
    ]:
        assert not lamin.exists(path), path
        with pytest.raises(FileNotFoundError):
            lamin.ls(path)


@pytest.mark.parametrize(
    "path",
    [f"{U}//aaaaaaaaaaaaaaaa", f"lamin://{IMAGES}/./1.jpg", f"{IMAGES}/sub/../1.jpg"],
)
def test_paths_are_not_normalized(lamin, path):
    for method in (lamin.info, lamin.cat_file, lamin.ls):
        with pytest.raises(FileNotFoundError, match="aren't normalized"):
            method(path)
    assert not lamin.exists(path)


def test_missing_normalized_paths_have_no_hint(lamin):
    with pytest.raises(FileNotFoundError) as error:
        lamin.info(f"{IMAGES}/nope.jpg")
    assert "normalized" not in str(error.value)


def test_directories_cannot_be_read(lamin):
    for path in (U, IMAGES, f"{IMAGES}/sub"):
        with pytest.raises(IsADirectoryError):
            lamin.cat_file(path)
    with pytest.raises(IsADirectoryError):
        lamin.open(IMAGES)


def test_read_only(lamin, hub):
    with pytest.raises(NotImplementedError, match="read-only"):
        lamin.open(f"{U}/new.csv", "wb")
    calls = [
        (lamin.rm, (TABLE,), {}),
        (lamin.rm, (U,), {"recursive": True}),
        (lamin.rm_file, (TABLE,), {}),
        (lamin.rmdir, (IMAGES,), {}),
        (lamin.mkdir, (f"{U}/new",), {}),
        (lamin.makedirs, (f"{U}/new",), {}),
        (lamin.pipe_file, (f"{U}/new.csv", b"x"), {}),
        (lamin.copy, (IMAGES, f"{U}/copy"), {"recursive": True}),
        (lamin.mv, (IMAGES, f"{U}/moved"), {"recursive": True}),
    ]
    for method, args, kwargs in calls:
        with pytest.raises(NotImplementedError, match="read-only"):
            method(*args, **kwargs)
    # they fail before looking anything up
    assert hub.calls == []


def test_ukey_and_checksum_follow_the_content_hash(lamin, hub):
    path = TABLE
    ukey, checksum = lamin.ukey(path), lamin.checksum(path)
    assert isinstance(checksum, int)
    # metadata changes don't change them
    record = hub.records["artifact"][1]
    record.update(description="new", updated_at="2026-02-01T00:00:00+00:00")
    lamin.invalidate_cache()
    assert (lamin.ukey(path), lamin.checksum(path)) == (ukey, checksum)
    record["hash"] = "changed"
    lamin.invalidate_cache()
    assert lamin.ukey(path) != ukey
    assert lamin.checksum(path) != checksum


def test_ukey_of_artifacts_without_hash(lamin, hub):
    path = TABLE
    record = hub.records["artifact"][1]
    record["hash"] = None
    ukey = lamin.ukey(path)
    record["updated_at"] = "2026-02-01T00:00:00+00:00"
    lamin.invalidate_cache()
    # fsspec's default, which covers all of the info
    assert lamin.ukey(path) != ukey


def test_ukey_of_files_in_folders_comes_from_their_storage(lamin):
    path, uri = f"{IMAGES}/sub/2.jpg", "s3://bucket-a/images/sub/2.jpg"
    assert lamin.ukey(path) == FakeS3().ukey(uri)
    assert lamin.checksum(path) == FakeS3().checksum(uri)


def test_timestamps(lamin):
    path = TABLE
    assert lamin.modified(path) == datetime.datetime(2026, 1, 2, tzinfo=datetime.UTC)
    assert lamin.created(path) == datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)


def test_timestamps_with_short_fractions():
    from fsspec_lamindb.core import _timestamp

    assert _timestamp("2026-06-03T05:10:43.53169+00:00", "p").microsecond == 531690


def test_cache_and_invalidation(lamin, hub):
    listed = lamin.ls(U, detail=False)
    added = dict(hub.records["artifact"][2], id=100, uid="newnewnewnewnewn0000")
    hub.records["artifact"].append(added)
    assert lamin.ls(U, detail=False) == listed
    assert lamin.ls(U, detail=False, refresh=True) == sorted([*listed, NEW])
    hub.records["artifact"].remove(added)
    lamin.invalidate_cache()
    assert lamin.ls(U, detail=False) == listed


def test_invalidating_an_instance_refreshes_its_storages(lamin, hub):
    assert lamin.info(TABLE)["storage"] == "s3://bucket-a"
    hub.records["storage"][0]["root"] = "s3://moved-bucket"
    # invalidating an artifact only looks it up again
    lamin.invalidate_cache(TABLE)
    assert lamin.info(TABLE)["storage"] == "s3://bucket-a"
    lamin.invalidate_cache(INST)
    assert lamin.info(TABLE)["storage"] == "s3://moved-bucket"


def test_sessions_are_closed_when_the_filesystem_is_collected(hub):
    fs = make_fs(hub)
    fs.ls(INST)
    session = fs._hub._session
    assert session is not None
    _close_session(fs.loop, fs._hub)
    assert session.closed
    # closing again does nothing
    _close_session(fs.loop, fs._hub)
    # without a running loop, the connections are closed directly
    other = make_fs(hub)
    other.ls(INST)
    session = other._hub._session
    assert session is not None
    _close_session(None, other._hub)
    assert session.closed


def test_listings_cache_can_be_disabled(hub):
    fs = make_fs(hub, use_listings_cache=False)
    fs.info(TABLE)
    before = len(hub.calls)
    fs.info(TABLE)
    assert len(hub.calls) > before


def test_disabled_cache_refreshes_storages(hub):
    fs = make_fs(hub, use_listings_cache=False)
    assert fs.info(TABLE)["storage"] == "s3://bucket-a"
    hub.records["storage"][0]["root"] = "s3://moved-bucket"
    assert fs.info(TABLE)["storage"] == "s3://moved-bucket"


def test_disabled_cache_still_shares_results_within_an_operation(hub):
    fs = make_fs(hub, use_listings_cache=False)
    fs.ls(U)
    fs.ls(U)
    # one storage query per listing, although every artifact entry needs its storage
    assert len([c for c in hub.calls if c[0] == "query" and c[1] == "storage"]) == 2
    # the instance settings are kept for the lifetime of the filesystem
    assert len([c for c in hub.calls if c[0] == "settings"]) == 1


def test_listings_expire_after_an_hour_by_default(hub):
    assert make_fs(hub).dircache.listings_expiry_time == 3600
    assert make_fs(hub, listings_expiry_time=None).dircache.listings_expiry_time is None


def test_expired_storages_are_refreshed(hub):
    fs = make_fs(hub, listings_expiry_time=0.05)
    assert fs.info(TABLE)["storage"] == "s3://bucket-a"
    hub.records["storage"][0]["root"] = "s3://moved-bucket"
    time.sleep(0.1)
    assert fs.info(TABLE)["storage"] == "s3://moved-bucket"


def test_disabled_cache_refreshes_folder_artifacts(hub):
    fs = make_fs(hub, use_listings_cache=False)
    assert fs.ls(IMAGES, detail=False) == [f"{IMAGES}/1.jpg", f"{IMAGES}/sub"]
    # the folder artifact now points to another location in its storage
    hub.records["artifact"][3]["_real_key"] = "images/sub"
    assert fs.ls(IMAGES, detail=False) == [f"{IMAGES}/2.jpg"]


def test_located_paths_are_bounded(hub):
    fs = make_fs(hub, max_cached_paths=2)
    first, second, third = (f"{IMAGES}/1.jpg", f"{IMAGES}/sub/2.jpg", INNER)
    for path in (first, second, first, third):
        fs.info(path)
    # the least recently used path was dropped
    assert list(fs._located) == [first, third]
    assert len(make_fs(hub)._located) == 0


def test_concurrent_lookups_are_shared(lamin, hub):
    lamin.cat([TABLE, READS, INNER])
    assert len([c for c in hub.calls if c[0] == "settings"]) == 1
    assert len([c for c in hub.calls if c[0] == "query" and c[1] == "storage"]) == 1
    # one credential lookup per storage
    roots = [c[1] for c in hub.calls if c[0] == "api_info"]
    assert sorted(roots) == ["s3://bucket-a", "s3://bucket-a/nested"]


def test_pickle_roundtrip(lamin):
    clone = pickle.loads(pickle.dumps(lamin))
    assert isinstance(clone, LaminDBFileSystem)
    assert clone.cat_file(TABLE) == b"a,b\n1,2\n"


def test_serialization_without_secrets(hub, monkeypatch):
    remote_options = {
        "s3": {"key": "k", "secret": "s", "client_kwargs": {"aws_session_token": "t", "x": 1}},
        "gs": {"project": "p", "token": "/path/to/credentials.json"},
    }
    fs = make_fs(hub, remote_options=remote_options)
    assert fs.to_dict()["api_key"] == "good-key"
    data = fs.to_dict(include_password=False)
    assert "api_key" not in data
    assert data["remote_options"] == {"s3": {"client_kwargs": {"x": 1}}, "gs": {"project": "p"}}
    serialized = fs.to_json(include_password=False)
    assert "good-key" not in serialized
    assert '"s"' not in serialized
    # the API key is then taken from the environment
    monkeypatch.setenv("LAMIN_API_KEY", "good-key")
    clone = fsspec.AbstractFileSystem.from_json(serialized)
    assert clone.cat_file(TABLE) == b"a,b\n1,2\n"
    # an API key passed as positional argument is dropped as well
    positional = LaminDBFileSystem("good-key", api_url=hub.api_url, skip_instance_cache=True)
    assert positional.to_dict()["args"] == ["good-key"]
    assert positional.to_dict(include_password=False)["args"] == []


# async API


async def test_async_api(hub):
    async with make_fs(hub, asynchronous=True) as fs:
        assert await fs._cat_file(TABLE) == b"a,b\n1,2\n"
        entries = await fs._ls(IMAGES, detail=False)
        assert entries == [f"{IMAGES}/1.jpg", f"{IMAGES}/sub"]
        assert await fs._exists(f"{U}/aaaaaaaaaaaaaaaa")
        f = await fs.open_async(WEB)
        assert await f.read() == b"from http\n"
        await f.close()
        with pytest.raises(NotImplementedError, match="not async"):
            await fs.open_async(MEM)
        http = fs._storage_access._filesystems[hub.http_root][0]
        assert isinstance(http, HTTPFileSystem)
        assert http.asynchronous
        session = await http.set_session()
        hub_session = await fs._hub.session()
    assert session.closed
    assert hub_session.closed


async def test_close_closes_replaced_storage_filesystems(hub):
    fs = make_fs(hub, asynchronous=True)
    await fs._cat_file(WEB)
    first = fs._storage_access._filesystems[hub.http_root][0]
    fs.invalidate_cache()
    await fs._cat_file(WEB)
    second = fs._storage_access._filesystems[hub.http_root][0]
    assert second is not first
    assert isinstance(first, HTTPFileSystem)
    assert isinstance(second, HTTPFileSystem)
    sessions = [await first.set_session(), await second.set_session()]
    await fs.close()
    assert all(session.closed for session in sessions)
    # the filesystem opens new sessions when it's used again
    assert await fs._cat_file(WEB) == b"from http\n"
    await fs.close()


def test_sync_mode_does_not_keep_replaced_filesystems(lamin):
    lamin.cat_file(WEB)
    lamin.invalidate_cache()
    assert lamin._storage_access._retired == []
