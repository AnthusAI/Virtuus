.PHONY: check check-python check-rust check-parity \
        build build-rust \
        coverage coverage-python coverage-rust

# ── Top-level target ────────────────────────────────────────────────────────

check: build check-python check-rust check-parity

# ── Build ───────────────────────────────────────────────────────────────────

build: build-rust

build-rust:
	cd rust && cargo build

# ── Python ──────────────────────────────────────────────────────────────────

check-python: build-rust
	cd python && python3 -m black --check .
	cd python && python3 -m ruff check .
	cd python && python3 -m coverage run -m behave
	cd python && python3 -m coverage report --fail-under=100

coverage-python: build-rust
	cd python && python3 -m coverage run -m behave
	cd python && python3 -m coverage report --fail-under=100

# ── Rust ────────────────────────────────────────────────────────────────────

check-rust:
	cd rust && cargo fmt --check
	cd rust && cargo clippy -- -D warnings
	cd rust && cargo test
	cd rust && cargo tarpaulin --lib --fail-under 100 --exclude-files "src/bin/virtuus.rs"

coverage-rust:
	cd rust && cargo tarpaulin --lib --fail-under 100 --exclude-files "src/bin/virtuus.rs"

# ── Parity ──────────────────────────────────────────────────────────────────

check-parity:
	python3 tools/check_spec_parity.py

# ── Combined coverage ────────────────────────────────────────────────────────

coverage: coverage-python coverage-rust
