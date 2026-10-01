# fsspec-lamindb

[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/fsspec-lamindb)](https://pypi.org/project/fsspec-lamindb/)
[![PyPI - License](https://img.shields.io/pypi/l/fsspec-lamindb)](https://github.com/ap--/fsspec-lamindb/blob/main/LICENSE)
[![PyPI](https://img.shields.io/pypi/v/fsspec-lamindb.svg)](https://pypi.org/project/fsspec-lamindb/)

[![Tests](https://github.com/ap--/fsspec-lamindb/actions/workflows/tests.yml/badge.svg)](https://github.com/ap--/fsspec-lamindb/actions/workflows/tests.yml)
[![Lint](https://github.com/ap--/fsspec-lamindb/actions/workflows/lint.yml/badge.svg)](https://github.com/ap--/fsspec-lamindb/actions/workflows/lint.yml)
[![GitHub issues](https://img.shields.io/github/issues/ap--/fsspec-lamindb)](https://github.com/ap--/fsspec-lamindb/issues)
[![Code style: ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

fsspec-lamindb is a Python library that provides a read-only
[`filesystem_spec`][fsspec] filesystem for [LaminDB][lamindb] instances. Its
`lamin://` URIs are compatible with [nf-lamin][nf-lamin].

[fsspec]: https://filesystem-spec.readthedocs.io/en/latest/intro.html
[lamindb]: https://docs.lamin.ai
[nf-lamin]: https://github.com/laminlabs/nf-lamin

The package doesn't depend on `lamindb`, `lamindb-core` or `lamindb-setup`:

- **Metadata** comes from the [LaminHub REST API][lamin-rest].
- **Data** is read with the fsspec filesystem of each storage location, such as
  `s3fs`, `gcsfs`, Hugging Face or HTTP.
- **Credentials** for S3 storage that Lamin manages are temporary credentials
  from LaminHub.

[lamin-rest]: https://docs.lamin.ai/rest


## Installation

Install the latest version of `fsspec-lamindb` with pip. S3 and HTTP(S) storage
locations work out of the box. For other storage locations, you have to install
additional packages, which are available as extras:

### PyPI

```bash
python -m pip install fsspec-lamindb
```

### Extras

```bash
python -m pip install "fsspec-lamindb[gcs]"  # + Google Cloud Storage (gcsfs)
python -m pip install "fsspec-lamindb[hf]"   # + Hugging Face (huggingface_hub)
python -m pip install "fsspec-lamindb[all]"  # all of the above
```

### Adding fsspec-lamindb to your project

Below is a `pyproject.toml` based example for adding `fsspec-lamindb` to your
project as a dependency if you want to read from S3, HTTP(S) and Google Cloud
Storage locations:

```toml
[project]
name = "myproject"
requires-python = ">=3.11"
dependencies = [
    "fsspec-lamindb[gcs]>=0.1.0",
]
```

Installing the package registers the `lamin://` protocol with fsspec, so
`fsspec.filesystem("lamin")` and `fsspec.open("lamin://...")` work without
importing `fsspec_lamindb` first.


## Basic Usage

```pycon
# pip install fsspec-lamindb
>>> import fsspec
>>>
>>> fs = fsspec.filesystem("lamin")  # uses $LAMIN_API_KEY if set
>>> fs.ls("lamin://laminlabs/lamindata", detail=False)
['laminlabs/lamindata/artifact']
>>> fs.cat_file("lamin://laminlabs/lamindata/artifact/K415DrXcuPiJmXlX/meta.csv")[:40]
b',species,file_name,scientist,instrument\n'
>>> info = fs.info("lamin://laminlabs/lamindata/artifact/K415DrXcuPiJmXlX")
>>> info["type"], info["uid"], info["n_files"], info["uri"]
('directory', 'K415DrXcuPiJmXlX5iXI', 52, 's3://lamindata/.lamindb/K415DrXcuPiJmXlX')
>>> fs.artifact_uri("lamin://laminlabs/lamindata/artifact/K415DrXcuPiJmXlX/meta.csv")
'lamin://laminlabs/lamindata/artifact/K415DrXcuPiJmXlX5iXI/meta.csv'
>>> fs.find("lamin://laminlabs/lamindata/artifact/K415DrXcuPiJmXlX")
['laminlabs/lamindata/artifact/K415DrXcuPiJmXlX/image_collection_1/iris-0337d20a....jpg',
 ...]

```

Pass the API key as a storage option to any fsspec-aware library:

```python
import pandas as pd

df = pd.read_parquet(
    "lamin://laminlabs/lamindata/artifact/k2Mo0c0rRWaaHEOB",
    storage_options={"api_key": "..."},
)
```

Public instances can be read without an API key. Pass `api_key=""` to read
anonymously even if `LAMIN_API_KEY` is set.

Prefer the `LAMIN_API_KEY` environment variable to the `api_key` option. Like
all storage options, an `api_key` becomes part of pickles of the filesystem and
of `fs.to_json()`, e.g. in kerchunk references or intake catalogs.
`fs.to_json(include_password=False)` leaves out the API key and the
credentials in `remote_options`.

### Async usage

In async mode, use the filesystem as an async context manager, or call
`await fs.close()`, to close its sessions when you're done:

```python
import asyncio

from fsspec_lamindb import LaminDBFileSystem


async def main():
    path = "lamin://laminlabs/lamindata/artifact/K415DrXcuPiJmXlX/meta.csv"
    async with LaminDBFileSystem(asynchronous=True) as fs:
        return await fs._cat_file(path)


asyncio.run(main())
```

### Python API

The package exports three names; everything else is private:

- `LaminDBFileSystem`, the filesystem of the `lamin` protocol.
- `LaminFile`, the files that `fs.open()` returns.
- `LaminAPIError`, an `OSError` raised for unexpected responses of LaminHub.
  Missing paths raise `FileNotFoundError`, and missing permissions
  `PermissionError`.

### URIs

```text
lamin://
└── <owner>/                          owners of the instances you can access
    └── <instance>/                   instances of an owner
        └── artifact/<uid>[/<subpath>] any artifact by uid, as in nf-lamin
```

- `artifact/<uid>` addresses any artifact the way nf-lamin does. A 20-character
  uid is that exact version. A 16-character uid addresses the version family and
  resolves to the latest version. Listing `artifact/` shows every artifact on the
  visible branches by its 20-character uid. That includes all versions, artifacts
  without a key, and the artifacts lamindb creates to track runs
  (`kind="__lamindb_run__"` or `"__lamindb_config__"`).
- Folder artifacts, such as `.zarr` stores, are directories. Their content is
  listed from their storage, and `<subpath>` addresses the files and
  directories inside them.
- `fs.artifact_uri(path)` resolves a path to the `artifact/<uid>` URI of the
  exact artifact version, e.g. to pin the version that a 16-character uid
  resolves to now. Paths inside a folder artifact keep their path inside it, as
  in `lamin://<owner>/<instance>/artifact/<uid>/<subpath>`. Pass
  `version_family=True` for the 16-character uid, which resolves to the latest
  version. Paths that are neither an artifact nor inside one, such as
  `artifact/` itself, raise `ValueError`.
- `ls("lamin://")` lists the owners of the instances your account can access,
  and `ls("lamin://<owner>")` lists those instances. Other owners don't exist
  in the filesystem, and anonymous callers can't list any. Public instances can
  still be opened directly as `lamin://<owner>/<instance>`.

Artifact entries carry record metadata: `uid`, `key`, `suffix`, `hash`, `kind`,
`otype`, `description`, `version_tag`, `is_latest`, `n_files`, `n_observations`,
`created_at`, `updated_at`. They also carry `storage` (the storage root) and
`uri` (the location of the data). `fs.modified()` and `fs.created()` return the
record's timestamps. `fs.ukey()` and `fs.checksum()` are derived from the
artifact's content hash, so caches like `simplecache::` only fetch the data
again when it changes. Files inside folder artifacts use those of their
storage filesystem.

### Storage options

| Option | Default | Description |
| --- | --- | --- |
| `api_key` | `$LAMIN_API_KEY` | Lamin API key. Pass `""` for anonymous access. |
| `api_url` | `$LAMIN_API_URL`, else `https://aws.us-east-1.lamin.ai/api` | LaminHub API used to look up instances. Any LaminHub API finds any instance. Requests then go to the API in the instance's settings. |
| `hub_url` | `$LAMIN_HUB_URL`, else `https://hub.lamin.ai` | LaminHub, which grants storage credentials. |
| `hub_anon_key` | `$LAMIN_HUB_ANON_KEY`, else the public key of `hub.lamin.ai` | LaminHub's public key. |
| `branch` | `None` | Name or uid of a branch to show on top of `main`, like `lamin switch`. Its artifacts are listed under `artifact/`, and its versions win when a 16-character uid resolves to the latest version. |
| `storage_protocols` | `["s3", "gs", "hf", "http", "https"]` | Protocols of the storage locations that may be read. The list replaces the default. Add `"file"` to read local storage locations. |
| `remote_options` | `{}` | Options of the storage filesystems by protocol or storage root, e.g. `{"s3": {"profile": "x"}, "s3://bucket": {"profile": "y"}}`. Options for a root are merged over those for its protocol, and `gcs` is the same as `gs`. S3 credentials given here replace those from LaminHub. |
| `timeout` | `60` | Timeout of each request to LaminHub in seconds. |
| `max_concurrency` | `8` | Maximum number of result pages of a query fetched in parallel. |
| `max_requests` | `16` | Maximum number of concurrent requests to LaminHub. |
| `max_cached_paths` | `None` | Maximum number of paths whose lookups are cached besides the listings, e.g. the chunks of a Zarr store. The least recently used are dropped first. `None` for no limit. |
| `trusted_hosts` | `None` | Hosts your access token may be sent to besides `*.lamin.ai` and the host of `api_url`, e.g. `["lamin.example.com", "*.example.org"]` for on-premises deployments. |

The standard fsspec options are also supported: `asynchronous`, `loop`,
`use_listings_cache`, `listings_expiry_time`, and `skip_instance_cache`.
Listings expire after an hour (`listings_expiry_time=3600`), as artifacts are
added and 16-character uids resolve to their latest version. Pass
`listings_expiry_time=None` to keep them until `fs.invalidate_cache()` is
called. Use `fs.invalidate_cache()` or `fs.ls(path, refresh=True)` to pick up
changes sooner.

`use_listings_cache` and `listings_expiry_time` apply to everything the
filesystem caches from LaminHub: listings, storage locations, and folder
artifacts. Only the instance settings and the selected branch are kept
for the lifetime of the filesystem. With `use_listings_cache=False`, each call
still fetches what it needs only once.

### Storage access

Each storage location gets its own fsspec filesystem, created on first use:

| Storage | Credentials |
| --- | --- |
| S3 that Lamin manages | Temporary credentials from LaminHub, renewed before they expire, or once when S3 rejects them. They're also issued to anonymous callers for public instances. |
| Other S3 buckets | Your AWS credentials (environment, `~/.aws`, …). Without any, the bucket is read anonymously. |
| GCS | Your Google credentials: `GOOGLE_APPLICATION_CREDENTIALS`, `gcloud auth application-default login`, or the service account on Google Cloud. Without any, the bucket is read anonymously. |
| Hugging Face, HTTP(S) | Your default credentials for the filesystem, e.g. `HF_TOKEN`. |

The resulting S3 options include the storage's region. LaminHub only issues
credentials for S3.

Storage locations come from the records of an instance, so whoever owns an
instance chooses what reading it accesses. That's why only the protocols in
`storage_protocols` are read; others raise `PermissionError`. Local storage
locations only exist on the machine that created them. To read them, pass the
full list, e.g. `storage_protocols=["s3", "gs", "hf", "http", "https", "file"]`.

Without Google credentials, gcsfs waits 15 to 30 seconds for the metadata
server of Google Compute Engine before it reads anonymously. So the lamin
filesystem checks for credentials without network requests: the variable
`GOOGLE_APPLICATION_CREDENTIALS`, gcloud's application default credentials,
gcsfs's token cache, and whether it runs on Google Cloud. If there are none, it
passes `token="anon"`. Set a `token` in `remote_options`, e.g.
`{"gs": {"token": "cloud"}}`, if the check misses your credentials. Set
`NO_GCE_CHECK=true` to never use the credentials of the metadata server, which
also speeds up gcsfs elsewhere.

The storage filesystems are created with `use_listings_cache=False`, as the
lamin filesystem caches and refreshes the listings itself. Set it in
`remote_options` to change that.


## How it works

- The filesystem is an fsspec `AsyncFileSystem` built on aiohttp. Async
  consumers such as zarr v3, kerchunk, or `fs.cat` of many files get concurrency
  without threads. The storage filesystems run on the same event loop.
- An API key is exchanged for an access token, which is refreshed when it
  expires. Transient errors like `502` and `429` are retried.
- The access token gives access to your whole account. The API URLs of
  instances and storages come from their settings, so the token is only sent
  over HTTPS (or HTTP to localhost) to `*.lamin.ai`, the host of `api_url`, and
  `trusted_hosts`. Other requests fail with `PermissionError`.
- The API returns at most 200 records per request, so listing `artifact/`
  fetches all artifacts of an instance in parallel pages. Looking up the listed
  artifacts afterwards needs no further requests.
- `info()`, `open()`, and `cat()` of a single path only look up that artifact.
- `find()`, and so `glob("**")`, `du()`, and `get(recursive=True)`, search a
  folder artifact with a single recursive search of its storage. A search of an
  instance searches `artifact/`, i.e. every version of every artifact.


## Known limitations

- Read-only. Writing, copying, moving, deleting, and creating directories raise
  `NotImplementedError` before any request is sent.
- Listings are fetched in pages of 200 records. If records are deleted while a
  listing is being fetched, another record can be missing from that listing.


## Future development

The `lamin://` URIs follow nf-lamin, which only addresses artifacts by uid.
Once Lamin defines its URI schema further, fsspec-lamindb can support more of
it, for example:

- URIs that reference artifacts by their `key` instead of their uid.
- Writes, e.g. saving files as new artifacts or as new versions of existing
  ones.


## Contributing

Contributions are very welcome. To set up a development environment, install
[uv][uv] and [just][just] and run:

```bash
uv sync
just test                # unit tests against a fake LaminHub
just test-live           # tests and doc examples against laminlabs/lamindata
just test-lamindb-setup  # LaminHub defaults against the latest lamindb-setup
just lint                # lockfile, ruff, and ty
just fmt                 # format and fix the code
just coverage            # unit tests with a branch coverage report
```

Run `just` to list all recipes. Run `pre-commit install` to run the same checks
as `just lint` before each commit. Set `FSSPEC_LAMINDB_TEST_INSTANCE`,
`LAMIN_API_KEY`, and `LAMIN_API_URL` to run the live tests against another
instance. Docstrings follow the [numpydoc][numpydoc] style.

[uv]: https://docs.astral.sh/uv/
[just]: https://just.systems/
[numpydoc]: https://numpydoc.readthedocs.io/en/latest/format.html


## Changelog

See [CHANGELOG.md](https://github.com/ap--/fsspec-lamindb/blob/main/CHANGELOG.md).
To report a vulnerability, see
[SECURITY.md](https://github.com/ap--/fsspec-lamindb/blob/main/SECURITY.md).


## License

Distributed under the terms of the [MIT license][license],
*fsspec-lamindb* is free and open source software.

[license]: https://github.com/ap--/fsspec-lamindb/blob/main/LICENSE


## Issues

If you encounter any problems, please [file an issue][issues] along with a
detailed description. We are always happy to help with any problems you might
encounter.

[issues]: https://github.com/ap--/fsspec-lamindb/issues
