# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

The first release.

### Added

- A read-only fsspec filesystem for LaminDB instances, registered as the
  `lamin` protocol. Its URIs follow nf-lamin's
  `lamin://<owner>/<instance>/artifact/<uid>[/<subpath>]`: a 20-character uid
  addresses an exact version, a 16-character uid the latest version, and
  `<subpath>` the content of folder artifacts.
- `fs.artifact_uri()` resolves a path to the `artifact/<uid>` URI of the exact
  artifact version.
- Metadata comes from the LaminHub REST API, and data from the fsspec
  filesystem of each storage location, with temporary S3 credentials from
  LaminHub. Credentials are renewed before they expire, and once when S3
  rejects them. Without credentials, S3 and GCS storage locations are read
  anonymously, without waiting for the metadata server of Google Compute
  Engine.
- Async support: `LaminDBFileSystem(asynchronous=True)` can be used with
  `async with`, and `fs.close()` closes all its sessions.
- Options: `branch`, `remote_options` by protocol or storage root,
  `storage_protocols`, `trusted_hosts`, `max_concurrency`, `max_requests`,
  and `max_cached_paths`.
- `fs.ukey()` and `fs.checksum()` are derived from the content hash of
  artifacts.
- `use_listings_cache` and `listings_expiry_time` apply to everything the
  filesystem caches from LaminHub. Listings expire after an hour by default.

### Security

- The access token is only sent over HTTPS to `*.lamin.ai`, the host of
  `api_url`, and `trusted_hosts`, as the API URLs of instances and storages
  come from their settings.
- Only storage locations with the protocols in `storage_protocols` are read:
  `s3`, `gs`, `hf`, `http`, and `https` by default. Local storage locations
  need `"file"`.
- `fs.to_json(include_password=False)` and
  `fs.to_dict(include_password=False)` leave out the API key and the
  credentials in `remote_options`.

[Unreleased]: https://github.com/ap--/fsspec-lamindb/commits/main
