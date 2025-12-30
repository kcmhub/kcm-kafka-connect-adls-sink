# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.0.2] - 2025-12-30

### Added
- Time-based flushing via `flush.interval.ms` (prevents low-traffic partitions from keeping buffers in memory indefinitely).
- Configurable Azure SDK retry policy via `adls.retry.max.attempts`.
- Explicit detection of ADLS authentication/authorization failures (HTTP 401/403) with fail-fast behavior.

### Changed
- `adls.retry.max.attempts` now represents the **number of retries** (additional attempts). Set to `0` to disable retries.
- ADLS write errors are classified as:
  - **Fatal** on authentication errors (invalid/expired SAS token)
  - **Retriable** on transient failures

### Fixed
- Improved robustness and observability of ADLS write failures.

### Refactor
- Extracted utility logic from `AdlsSinkTask` into dedicated helpers:
  - `SimpleJsonFormatter` (record formatting)
  - `AuthFailureDetector` (auth error detection)
  - `TimeBasedFlusher` (time-based flush decision)

### Tests
- Added unit tests to validate:
  - Authentication failure handling (fail-fast behavior)
  - Time-based flush behavior (`flush.interval.ms`)
  - Retry configuration propagation

## [0.0.1] - Initial release

- Kafka Connect Sink Connector for Azure Data Lake Storage Gen2 (ADLS Gen2)
- SAS authentication support
- Partition-aware file batching
- Optional GZIP compression
- Avro and schemaful record support
