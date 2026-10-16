PYTHON ?= python

.PHONY: check check-python check-rust check-store-coverage check-parity \
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
	cd python && VIRTUUS_BACKEND=python $(PYTHON) -m coverage run -m behave --exclude benchmarks --tags=-rust-only
	cd python && $(PYTHON) -m coverage report --include "src/virtuus/*" --fail-under=100

coverage-python: build-rust
	cd python && VIRTUUS_BACKEND=python $(PYTHON) -m coverage run -m behave --exclude benchmarks --tags=-rust-only
	cd python && $(PYTHON) -m coverage report --include "src/virtuus/*" --fail-under=100

# ── Rust ────────────────────────────────────────────────────────────────────

check-rust:
	cd rust && cargo fmt --check
	cd rust && cargo clippy -p virtuus --all-targets -- -D warnings
	cd rust && cargo clippy -p virtuus-amplify --all-targets -- -D warnings
	cd rust && cargo clippy -p virtuus-appsync --all-targets -- -D warnings
	cd rust && cargo clippy -p virtuus-store --all-targets --features aws -- -D warnings
	cd rust && cargo test -p virtuus --lib
	cd rust && cargo test -p virtuus-amplify
	cd rust && cargo test -p virtuus-appsync
	cd rust && cargo test -p virtuus-store --features aws
	$(MAKE) check-store-coverage
	cd rust && cargo tarpaulin --skip-clean -p virtuus --lib --fail-under 100 --exclude-files "src/bin/virtuus.rs" --exclude-files "crates/*"
	cd rust && cargo tarpaulin --skip-clean -p virtuus-amplify --exclude-files 'src/*' --exclude-files 'crates/virtuus-appsync/*' --exclude-files 'crates/virtuus-store/*' --exclude-files '*/tests/*' --fail-under 100
	cd rust && cargo tarpaulin --skip-clean -p virtuus-appsync --exclude-files 'src/*' --exclude-files 'crates/virtuus-amplify/*' --exclude-files 'crates/virtuus-store/*' --exclude-files '*/tests/*' --fail-under 100

coverage-rust:
	cd rust && cargo tarpaulin --lib --fail-under 100 --exclude-files "src/bin/virtuus.rs"

# Measure the complete store, including AWS adapters, independently of peer crates.
check-store-coverage:
	cd rust && cargo tarpaulin --engine llvm --skip-clean -p virtuus-store --all-features --locked --exclude-files 'src/*' --exclude-files 'crates/virtuus-amplify/*' --exclude-files 'crates/virtuus-appsync/*' --exclude-files '*/tests/*' --fail-under 100

# ── Parity ──────────────────────────────────────────────────────────────────

check-parity:
	$(PYTHON) tools/check_spec_parity.py

# ── Combined coverage ────────────────────────────────────────────────────────

coverage: coverage-python coverage-rust

# ── Bench sync helpers (local + optional S3) ────────────────────────────────

.PHONY: bench-sync
bench-sync:
	$(PYTHON) tools/sync_bench_results.py $(if $(bucket),--bucket $(bucket),) $(if $(prefix),--prefix $(prefix),) $(if $(profile),--profile $(profile),)
