# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2025.11.11

### Added

- Validation for `workers` count. Worker count should be a positive integer.

### Changed

- Rename module `Rapidflow` to `RapidFlow`.
- Move custom error classes from `RapidFlow::Batch` class under to `RapidFlow` module.

## [0.1.0] - 2025.11.01

### Added

- `Rapidflow::Batch` class which allows creating concurrent data processing pipelines in a batch

