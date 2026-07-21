# Changelog

All notable changes to this project will be documented in this file.

The project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Until `1.0.0`, minor releases may include breaking changes; those changes will be called out here.

## [Unreleased]

### Added

- Nothing yet.

### Changed

- Nothing yet.

### Fixed

- Nothing yet.

### Security

- Nothing yet.

## [0.3.0] - Unreleased

Implementation candidate completed on 2026-07-21; this entry does not assert
that a `v0.3.0` release has been published.

### Added

- Context compaction with manual `/compact`, configured automatic thresholds, immutable source journals, and provenance-bearing summary evidence.
- Filesystem skills with global and project catalogs, explicit project trust, deterministic shadowing, frozen runtime snapshots, and the metadata-only `/skills` surface.
- Sequential depth-one subagents using the same provider and model, one active child, bounded attempts/tool calls/time, durable receipts, and restart reconciliation.
- Cumulative self-hosting acceptance on macOS and Linux with four native release archives, SPDX documents, and checksum/smoke gates.

### Changed

- JSONC configuration and its strict schema now describe model `contextWindow`, context compaction policy, project-skill policy, and sequential-child limits.
- `/reload` activates validated config and skill-catalog changes only for future runtime generations.

### Fixed

- Provider stream closure after cancellation retains the cancelled result instead of being misclassified as a malformed stream.

### Security

- Project skills remain instruction-only and cannot install hooks, execute code, or bypass ordinary permissions.
- Child mutable grants and trusted-shell acknowledgement are isolated from the parent session.
- Uncertain effects are never retried automatically; compaction never rewrites source journal history.

## [0.1.0] - 2026-07-13

### Added

- Initial open source project guides.

### Changed

- Nothing yet.

### Fixed

- Nothing yet.

### Security

- Documented the trusted, unsandboxed shell boundary and private vulnerability reporting process.
