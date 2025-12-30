# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project follows [Semantic Versioning](https://semver.org/).

---

## [0.0.2] – 2025-12-30

### Added
- Configurable ADLS retry policy via `adls.retry.max.attempts` (set to `0` to disable retries)
- Azure SDK native retry support using exponential backoff
- Explicit detection of ADLS authentication failures (HTTP 401 / 403)

### Changed
- ADLS write errors are now classified as:
    - **Fatal** on authentication errors (invalid/expired SAS token)
    - **Retriable** on transient failures
- SinkTask fails fast on authentication errors with a clear error message

### Fixed
- Improved robustness and observability of ADLS write failures

### Tests
- Added unit tests to validate:
    - Authentication failure handling (fail-fast behavior)
    - Retry configuration propagation to Azure SDK client

---

## [0.0.1] – Initial release

- Kafka Connect Sink Connector for Azure Data Lake Storage Gen2 (ADLS Gen2)
- SAS authentication support
- Partition-aware file batching
- Optional GZIP compression
- Avro and schemaful record support
