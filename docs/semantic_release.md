# Semantic Release Notes

- Semantic-release is configured via python-semantic-release in the repository root (see `pyproject.toml` and `.github/workflows/release.yml`).
- Trusted publishing to PyPI is handled by `.github/workflows/publish-pypi.yml` (OIDC) once the PyPI publisher is configured.
- The release workflow publishes the core Rust crate followed by an unpublished `virtuus-store` version, using the existing crates.io release credential. Registry errors stop publication. The store has its own version and a registry-resolvable core dependency; bump its package version before subsequent store releases.
- Versioning updates only the core package entry in the existing Cargo lockfile, preserving the dependency versions that passed CI.
