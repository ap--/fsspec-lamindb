"""Read-only fsspec filesystem for LaminDB instances, with nf-lamin compatible URIs.

Installing the package registers the ``lamin`` protocol with fsspec.

Examples
--------
>>> import fsspec
>>> fs = fsspec.filesystem("lamin")
>>> fs.ls("lamin://laminlabs/lamindata", detail=False)
['laminlabs/lamindata/artifact']
>>> fs.cat_file("lamin://laminlabs/lamindata/artifact/K415DrXcuPiJmXlX/meta.csv")[:8]
b',species'
"""

from ._hub import LaminAPIError
from ._version import __version__ as __version__
from .core import LaminDBFileSystem, LaminFile

__all__ = [
    "LaminAPIError",
    "LaminDBFileSystem",
    "LaminFile",
]
