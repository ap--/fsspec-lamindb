"""Paths, targets, and other helpers of the filesystem."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from ._hub import Instance
from ._records import Storage

PROTOCOL = "lamin"
ARTIFACT = "artifact"

Listing = dict[str, dict[str, Any]]


@dataclass
class _Target:
    """What a path refers to: its entry and, for data, the storage that holds it.

    Attributes
    ----------
    info : dict
        The fsspec info of the entry.
    storage : Storage or None
        The storage location that holds the data of the entry, if any.
    """

    info: dict[str, Any]
    storage: Storage | None = None

    @property
    def uri(self) -> str | None:
        return self.info.get("uri")

    @property
    def is_dir(self) -> bool:
        return self.info["type"] == "directory"

    def location(self) -> tuple[Storage, str]:
        """Return the storage and URI that hold the data of the entry.

        Returns
        -------
        storage : Storage
            The storage location.
        uri : str
            The URI of the data.

        Raises
        ------
        FileNotFoundError
            If the storage or URI of the entry is unknown.
        """
        uri = self.uri
        if self.storage is None or uri is None:
            raise FileNotFoundError(f"the storage of {self.info['name']} is unknown")
        return self.storage, uri


@dataclass
class _State:
    """An instance and the branches visible in it.

    Attributes
    ----------
    instance : Instance
        The LaminDB instance.
    branch_ids : list of int
        Ids of the branches whose records are visible.
    branch_id : int or None
        Id of the branch selected in addition to ``main``, if any.
    """

    instance: Instance
    branch_ids: list[int]
    # the branch selected in addition to main
    branch_id: int | None = None

    @property
    def slug(self) -> str:
        return self.instance.slug

    def prefer(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Order records so that later ones win conflicts.

        Records of the selected branch win over those of ``main``, newer records win over
        older ones.

        Parameters
        ----------
        records : list of dict
            The records to order.

        Returns
        -------
        list of dict
            The records, least preferred first.
        """
        selected = self.branch_id

        def rank(record: dict[str, Any]) -> tuple[bool, int]:
            return selected is not None and record.get("branch_id") == selected, record["id"]

        return sorted(records, key=rank)


@dataclass
class _Folder:
    """A folder artifact, whose content is listed from its storage.

    Attributes
    ----------
    path : str
        Path of the folder artifact.
    info : dict
        The fsspec info of the folder artifact.
    storage : Storage
        The storage location that holds the folder.
    registered : float
        When the folder artifact was registered, as a :func:`time.monotonic` timestamp.
    """

    path: str
    info: dict[str, Any]
    storage: Storage
    registered: float = field(default_factory=time.monotonic)

    @property
    def uri(self) -> str:
        return self.info["uri"]


def _parts(path: str) -> list[str]:
    return path.split("/") if path else []


def _parent(path: str) -> str:
    return path.rsplit("/", 1)[0] if "/" in path else ""


def _join_uri(uri: str, rel: str) -> str:
    return f"{uri.rstrip('/')}/{rel}" if rel else uri


def _sorted(listing: Listing) -> list[dict[str, Any]]:
    return sorted(listing.values(), key=lambda entry: entry["name"])
