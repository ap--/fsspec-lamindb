"""Tests against the live LaminHub; run them with ``FSSPEC_LAMINDB_LIVE=1``.

By default they read the public instance ``laminlabs/lamindata`` anonymously. Set
``FSSPEC_LAMINDB_TEST_INSTANCE`` (and ``LAMIN_API_KEY``, ``LAMIN_API_URL`` as needed) to test
another instance.
"""

from __future__ import annotations

import os
import urllib.error
import urllib.request

import fsspec
import pytest

from fsspec_lamindb import _storage

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("FSSPEC_LAMINDB_LIVE") != "1", reason="set FSSPEC_LAMINDB_LIVE=1"
    ),
]

INSTANCE = os.environ.get("FSSPEC_LAMINDB_TEST_INSTANCE", "laminlabs/lamindata")
LAMINDATA = INSTANCE == "laminlabs/lamindata"


@pytest.fixture(scope="module")
def fs():
    options = {"skip_instance_cache": True}
    if LAMINDATA:
        # the public instance is read anonymously
        options["api_key"] = ""
    return fsspec.filesystem("lamin", **options)


def test_instance(fs):
    assert fs.ls(INSTANCE, detail=False) == [f"{INSTANCE}/artifact"]


def test_read_an_artifact_by_uid(fs):
    files = [e for e in fs.ls(f"{INSTANCE}/artifact") if e["type"] == "file" and e.get("size")]
    if not files:
        pytest.skip("no file artifacts")
    entry = next((e for e in files if str(e.get("uri")).startswith("s3://")), files[0])
    head = fs.cat_file(entry["name"], 0, 16)
    assert len(head) == min(16, entry["size"])
    if entry["is_latest"]:
        with fs.open(f"lamin://{INSTANCE}/artifact/{entry['uid'][:16]}") as f:
            assert f.read(16) == head


@pytest.mark.skipif(not LAMINDATA, reason="uses lamindata's demo data")
def test_lamindata(fs):
    folder = f"{INSTANCE}/artifact/K415DrXcuPiJmXlX"
    assert len(fs.find(folder)) == fs.info(folder)["n_files"]
    assert fs.cat_file(f"{folder}/meta.csv", 0, 8) == b",species"
    uri = f"lamin://{INSTANCE}/artifact/K415DrXcuPiJmXlX5iXI"
    assert fs.artifact_uri(f"lamin://{folder}") == uri
    images = fs.glob(f"lamin://{folder}/*.jpg")
    assert images
    assert fs.cat_file(images[0], 0, 3) == b"\xff\xd8\xff"


# a public folder artifact in Google Cloud Storage
GCS_ARTIFACT = "laminlabs/arc-virtual-cell-atlas/artifact/y8mmJDNVpwifr09b"


def test_gcs_folder_artifact(monkeypatch):
    pytest.importorskip("gcsfs")
    # read anonymously, as without Google credentials, even if there are some
    monkeypatch.setattr(_storage, "has_ambient_google_credentials", lambda: False)
    fs = fsspec.filesystem("lamin", api_key="", skip_instance_cache=True)
    info = fs.info(GCS_ARTIFACT)
    assert info["type"] == "directory"
    assert info["storage"].startswith("gs://")
    assert len(fs.find(GCS_ARTIFACT)) == info["n_files"]
    files = [entry for entry in fs.ls(GCS_ARTIFACT) if entry["type"] == "file"]
    smallest = min(files, key=lambda entry: entry["size"])
    assert len(fs.cat_file(smallest["name"], 0, 16)) == min(16, smallest["size"])


# a public artifact on Hugging Face: an AnnData file, which is HDF5
HF_ARTIFACT = "laminlabs/arc-virtual-cell-atlas/artifact/plvz5n0YX1fVWbEp"
HF_REPO_API = "https://huggingface.co/api/models/vevotx/Tahoe-100M-SCVI-v1"


def test_hugging_face_artifact():
    pytest.importorskip("huggingface_hub")
    try:
        with urllib.request.urlopen(HF_REPO_API, timeout=20) as response:
            status = response.status
    except (urllib.error.URLError, TimeoutError) as error:
        status = getattr(error, "code", None) or error
    if status != 200:
        pytest.skip(f"huggingface.co isn't reachable: {status}")
    fs = fsspec.filesystem("lamin", api_key="", skip_instance_cache=True)
    info = fs.info(HF_ARTIFACT)
    assert info["type"] == "file"
    assert info["uri"].startswith("hf://")
    assert fs.cat_file(HF_ARTIFACT, 0, 8) == b"\x89HDF\r\n\x1a\n"
