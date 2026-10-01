"""Helpers that map LaminDB records to filesystem paths and entries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# artifacts with a virtual key are stored under this prefix of their storage root
AUTO_KEY_PREFIX = ".lamindb"

_ARTIFACT_FIELDS = (
    "uid",
    "key",
    "suffix",
    "hash",
    "kind",
    "otype",
    "description",
    "version_tag",
    "is_latest",
    "n_files",
    "n_observations",
    "created_at",
    "updated_at",
)


@dataclass(frozen=True)
class Storage:
    """A storage location of a LaminDB instance.

    Attributes
    ----------
    id : int
        Id of the storage record.
    uid : str
        Uid of the storage record.
    root : str
        Root of the storage location without trailing slash, e.g. ``"s3://bucket"``.
    type : str or None
        Type of the storage location, e.g. ``"s3"``.
    region : str or None
        Cloud region of the storage location.
    instance_uid : str or None
        Uid of the instance that manages the storage location.
    """

    id: int
    uid: str
    root: str
    type: str | None = None
    region: str | None = None
    instance_uid: str | None = None

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> Storage:
        return cls(
            id=record["id"],
            uid=record.get("uid") or "",
            root=str(record.get("root") or "").rstrip("/"),
            type=record.get("type"),
            region=record.get("region"),
            instance_uid=record.get("instance_uid"),
        )

    @property
    def protocol(self) -> str:
        return self.root.split("://", 1)[0] if "://" in self.root else "file"

    def uri(self, key: str) -> str:
        return f"{self.root}/{key}" if key else self.root


def is_artifact_entry(info: dict[str, Any]) -> bool:
    """Check whether an fsspec info is the entry of an artifact.

    Parameters
    ----------
    info : dict
        The fsspec info.

    Returns
    -------
    bool
        Whether it's the entry of an artifact; entries inside folder artifacts have no uid.
    """
    return bool(info.get("uid"))


def is_folder(record: dict[str, Any]) -> bool:
    return record.get("n_files") is not None


def storage_key(record: dict[str, Any]) -> str:
    """Return the key of an artifact in its storage location.

    This mirrors ``auto_storage_key_from_artifact`` of lamindb.

    Parameters
    ----------
    record : dict
        The artifact record.

    Returns
    -------
    str
        The key of the artifact relative to the root of its storage location.
    """
    if record.get("_real_key"):
        return record["_real_key"]
    key = record.get("key")
    if key and not record.get("_key_is_virtual", True):
        return key
    uid = record["uid"]
    if record.get("_overwrite_versions"):
        uid = uid[:16]
    return f"{AUTO_KEY_PREFIX}/{uid}{record.get('suffix') or ''}"


def artifact_info(name: str, record: dict[str, Any], storage: Storage | None) -> dict[str, Any]:
    info: dict[str, Any] = {
        "name": name,
        "size": record.get("size"),
        "type": "directory" if is_folder(record) else "file",
    }
    for field in _ARTIFACT_FIELDS:
        info[field] = record.get(field)
    if storage is not None:
        info["storage"] = storage.root
        info["uri"] = storage.uri(storage_key(record))
    return info


def directory_info(name: str) -> dict[str, Any]:
    return {"name": name, "size": 0, "type": "directory"}


def join(*parts: str) -> str:
    return "/".join(part for part in parts if part)


def is_below(path: str, prefix: str) -> bool:
    """Check whether ``path`` is ``prefix`` or inside it.

    Parameters
    ----------
    path : str
        The path to check.
    prefix : str
        The directory; everything is inside ``""``.

    Returns
    -------
    bool
        Whether ``path`` is ``prefix`` or inside it.
    """
    return not prefix or path == prefix or path.startswith(prefix + "/")


def relative(path: str, prefix: str) -> str:
    return path if not prefix else path[len(prefix) + 1 :]


def merge_entry(listing: dict[str, dict[str, Any]], child: str, info: dict[str, Any]) -> None:
    """Add an entry to a listing; directories win over files of the same name.

    Parameters
    ----------
    listing : dict of str to dict
        The entries by name; it's modified in place.
    child : str
        Name of the entry.
    info : dict
        The fsspec info of the entry.
    """
    existing = listing.get(child)
    if existing is None or info["type"] == "directory" or existing["type"] != "directory":
        listing[child] = info
