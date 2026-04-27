# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-04-27

### Added
- Owner-bound handlers with weak auto-cleanup via `on(_:owner:priority:id:handler:)`.
- Optional OSSignposter integration through `EventBusObservability`.
- `os.Logger`-backed `EventLogger`.
- SwiftLint configuration for CI stability.
- DocC hosting workflow for GitHub Pages.
- Example SwiftUI app sources under `Examples/SwiftUIExample`.
- Release changelog.

### Changed
- Release documentation now covers production usage and observability.
- Middleware type-mismatch drops are logged through `Logger.warning`.
- Added release-grade tests for stream operators, `nextOrSuspend`, metrics, and owner cleanup.

## [0.1.0] - 2026-04-27

### Added
- Initial public package with async/await, Combine, and SwiftUI APIs.
