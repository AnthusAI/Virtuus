# Semantic Release Notes

- Semantic-release is configured via python-semantic-release in the repository root (see `pyproject.toml` and `.github/workflows/release.yml`).
- Trusted publishing to PyPI is handled by `.github/workflows/publish-pypi.yml` (OIDC) once the PyPI publisher is configured.
- Crates.io publish remains manual (cargo publish) until a trusted flow is added.
