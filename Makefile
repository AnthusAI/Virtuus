PYTHON ?= python

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
	cd python && $(PYTHON) -m black --check .
	cd python && $(PYTHON) -m ruff check .
	cd python && VIRTUUS_BACKEND=python $(PYTHON) -m coverage run -m behave --exclude benchmarks
	cd python && $(PYTHON) -m coverage report --include "src/virtuus/*" --fail-under=100

coverage-python: build-rust
	cd python && VIRTUUS_BACKEND=python $(PYTHON) -m coverage run -m behave --exclude benchmarks
	cd python && $(PYTHON) -m coverage report --include "src/virtuus/*" --fail-under=100

# ── Rust ────────────────────────────────────────────────────────────────────

check-rust:
	cd rust && cargo fmt --check
	cd rust && cargo clippy -- -D warnings
	cd rust && cargo test --lib
	cd rust && CUCUMBER_FILTER_TAGS='not @python-only and not @bench' cargo tarpaulin --lib --fail-under 100 --exclude-files "src/bin/virtuus.rs"

coverage-rust:
	cd rust && cargo tarpaulin --lib --fail-under 100 --exclude-files "src/bin/virtuus.rs"

# ── Parity ──────────────────────────────────────────────────────────────────

check-parity:
	$(PYTHON) tools/check_spec_parity.py

# ── Combined coverage ────────────────────────────────────────────────────────

coverage: coverage-python coverage-rust

# ── Bench sync helpers (local + optional S3) ────────────────────────────────

.PHONY: bench-sync
bench-sync:
	$(PYTHON) tools/sync_bench_results.py $(if $(bucket),--bucket $(bucket),) $(if $(prefix),--prefix $(prefix),) $(if $(profile),--profile $(profile),)
