# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- Improved container image labels in build

## [0.2.1] - 2022-05-10
### Fixed
- Fix default or `"none"` passed to `--epsg` gives bad EPSG value for pre-process reprojection.

## [0.2.0] - 2022-04-08
### Added
- Fill out README.
- Basic Argo Workflow example in `./example-workflow.yaml`.
### Changed
- Upgrade `gcsfs` to v2022.3.0. Required for fix to [#1](https://github.com/brews/nastyprisms/issues/1).
### Fixed
- Corrected bad docstr.
- Unexpected keyword argument 'callback_timeout' TypeError with zarr and fsspec, gcsfs. ([#1](https://github.com/brews/nastyprisms/issues/1))

## [0.1.0] - 2022-04-08
- The initial release.
