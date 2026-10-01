"""The version of the installed package."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("fsspec-lamindb")
except PackageNotFoundError:  # pragma: no cover - not installed
    __version__ = "0+unknown"
