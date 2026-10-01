from __future__ import annotations

import logging
import time

import fsspec
import pytest

from fsspec_lamindb import _storage
from fsspec_lamindb._records import Storage


def test_expiry():
    assert _storage._expiry("2099-01-01T00:00:00Z", "s3://b") == 4070908800
    # without a time zone, it's UTC
    assert _storage._expiry("2099-01-01T00:00:00", "s3://b") == 4070908800
    assert _storage._expiry("2099-01-01T00:00:00.12345+00:00", "s3://b") > 4070908800


def test_invalid_expiry_renews_after_the_fallback_lifetime(caplog):
    with caplog.at_level(logging.WARNING, logger="fsspec_lamindb._storage"):
        expires = _storage._expiry("soon", "s3://b")
    renewed = expires - _storage.REFRESH_MARGIN - time.time()
    assert abs(renewed - _storage.FALLBACK_LIFETIME) < 5
    assert "s3://b" in caplog.text
    assert "soon" in caplog.text


def test_remote_options_aliases():
    access = _storage.StorageAccess(
        None,  # ty: ignore[invalid-argument-type]
        {"gcs": {"project": "p"}, "gs://bucket": {"token": "t"}, "local": {"auto_mkdir": True}},
        asynchronous=False,
        loop=None,
    )
    gs = Storage(id=1, uid="s", root="gs://bucket")
    assert access.user_options(gs) == {"project": "p", "token": "t"}
    assert access.user_options(Storage(id=2, uid="t", root="gs://other")) == {"project": "p"}
    assert access.user_options(Storage(id=3, uid="u", root="/data")) == {"auto_mkdir": True}


async def test_missing_filesystem_suggests_the_extra(monkeypatch):
    def missing(protocol):
        raise ImportError(f"Install the package for {protocol}")

    monkeypatch.setattr(fsspec, "get_filesystem_class", missing)
    access = _storage.StorageAccess(
        None,  # ty: ignore[invalid-argument-type]
        {},
        asynchronous=False,
        loop=None,
    )
    with pytest.raises(ImportError, match=r"pip install 'fsspec-lamindb\[gcs\]'"):
        await access.filesystem(Storage(id=1, uid="s", root="gcs://bucket"))
    with pytest.raises(ImportError, match=r"pip install 'fsspec-lamindb\[hf\]'"):
        await access.filesystem(Storage(id=2, uid="t", root="hf://datasets/x"))


def test_ambient_aws_credentials(monkeypatch, tmp_path):
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "config"))
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", raising=False)
    monkeypatch.delenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", raising=False)
    monkeypatch.delenv("AWS_WEB_IDENTITY_TOKEN_FILE", raising=False)
    assert not _storage.has_ambient_aws_credentials()
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    assert _storage.has_ambient_aws_credentials()


async def test_gcs_storage():
    gcsfs = pytest.importorskip("gcsfs")
    access = _storage.StorageAccess(
        None,  # ty: ignore[invalid-argument-type]
        {"gcs": {"token": "anon"}},
        asynchronous=True,
        loop=None,
    )
    fs = await access.filesystem(Storage(id=1, uid="s", root="gs://bucket"))
    assert isinstance(fs, gcsfs.GCSFileSystem)
    assert fs.storage_options["token"] == "anon"
    assert fs.storage_options["use_listings_cache"] is False
    await access.close()


async def test_hugging_face_storage():
    hf = pytest.importorskip("huggingface_hub")
    access = _storage.StorageAccess(
        None,  # ty: ignore[invalid-argument-type]
        {"hf": {"token": False}},
        asynchronous=False,
        loop=None,
    )
    fs = await access.filesystem(Storage(id=1, uid="s", root="hf://datasets/org/repo"))
    assert isinstance(fs, hf.HfFileSystem)
    assert fs.token is False


@pytest.mark.parametrize(
    ("ambient", "remote_options", "token"),
    [
        (False, {}, "anon"),
        (True, {}, None),
        (False, {"gcs": {"token": "cloud"}}, "cloud"),
        (False, {"gs://bucket": {"token": None}}, None),
    ],
)
async def test_gcs_storage_is_read_anonymously_without_credentials(
    monkeypatch, ambient, remote_options, token
):
    checks = []

    def has_credentials():
        checks.append(True)
        return ambient

    monkeypatch.setattr(_storage, "has_ambient_google_credentials", has_credentials)
    access = _storage.StorageAccess(
        None,  # ty: ignore[invalid-argument-type]
        remote_options,
        asynchronous=False,
        loop=None,
    )
    for _ in range(2):
        options, _ = await access.options(Storage(id=1, uid="s", root="gs://bucket"))
        assert options.get("token") == token
    # the check runs once, and only if no token is given
    assert len(checks) == (0 if remote_options else 1)


@pytest.fixture
def no_google_credentials(monkeypatch, tmp_path):
    pytest.importorskip("gcsfs")
    from google.auth.compute_engine import _metadata

    monkeypatch.setenv("CLOUDSDK_CONFIG", str(tmp_path))
    monkeypatch.setattr("gcsfs.credentials.tfile", str(tmp_path / ".gcs_tokens"))
    monkeypatch.setattr(_metadata, "detect_gce_residency_linux", lambda: False)
    for name in (
        "GOOGLE_APPLICATION_CREDENTIALS",
        "NO_GCE_CHECK",
        *_storage._GOOGLE_CLOUD_VARIABLES,
    ):
        monkeypatch.delenv(name, raising=False)
    return tmp_path


def test_no_ambient_google_credentials(no_google_credentials):
    assert not _storage.has_ambient_google_credentials()


def test_ambient_google_credentials_from_files(no_google_credentials, monkeypatch):
    adc = no_google_credentials / "application_default_credentials.json"
    adc.write_text("{}")
    assert _storage.has_ambient_google_credentials()
    adc.unlink()
    (no_google_credentials / ".gcs_tokens").write_text("{}")
    assert _storage.has_ambient_google_credentials()


@pytest.mark.parametrize("variable", ["GOOGLE_APPLICATION_CREDENTIALS", "K_SERVICE"])
def test_ambient_google_credentials_from_the_environment(
    no_google_credentials, monkeypatch, variable
):
    monkeypatch.setenv(variable, "x")
    assert _storage.has_ambient_google_credentials()


def test_ambient_google_credentials_on_compute_engine(no_google_credentials, monkeypatch):
    from google.auth.compute_engine import _metadata

    monkeypatch.setattr(_metadata, "detect_gce_residency_linux", lambda: True)
    assert _storage.has_ambient_google_credentials()
    # NO_GCE_CHECK turns off the credentials of the metadata server
    monkeypatch.setenv("NO_GCE_CHECK", "true")
    assert not _storage.has_ambient_google_credentials()
    monkeypatch.setenv("K_SERVICE", "x")
    assert not _storage.has_ambient_google_credentials()
