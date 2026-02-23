# Virtuus 善

A file-backed in-memory indexed table engine. Virtuus treats folders of JSON files as indexed tables — like DynamoDB tables backed by the filesystem.

[![CI](https://github.com/AnthusAI/Virtuus/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AnthusAI/Virtuus/actions/workflows/ci.yml)
![Python Coverage](https://img.shields.io/badge/python%20coverage-100%25-brightgreen)
![Rust Coverage](https://img.shields.io/badge/rust%20coverage-100%25-brightgreen)
[![PyPI](https://img.shields.io/pypi/v/virtuus.svg)](https://pypi.org/project/virtuus/)
[![Crates.io](https://img.shields.io/crates/v/virtuus.svg)](https://crates.io/crates/virtuus)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Data lives on disk as one JSON file per record. Virtuus loads it into memory, builds indexes, and provides fast query access with DynamoDB-style Global Secondary Indexes, associations, pagination, and a nested query interface. Writes persist back to disk atomically.

## Why

We built Virtuus to decouple data logic from a GraphQL control plane so it can run independently in a containerized processing farm (Kubernetes). The goal: stand up a GraphQL-equivalent API inside a container, driven only by exported JSON files, with no external services or heavy dependencies. For workloads that fit in “small” tables (≈10k records or less), this file-backed architecture is the simplest way to ship the whole data + query engine with the container.

## Guiding Values
- It's better to eliminate a problem than to solve it.  Ask whether you truly need a database and the lifetime cost it adds; the filesystem may already be enough.
- Behavior-driven design as source code.  The Gherkin spec is the single source of truth; Rust and Python implementations are generated artifacts.  Classic BDD, now accelerated by AI assistants.
- Use AI to raise the bar, not just ship faster.  We enforce Ruff, Black, docstring rules, and 100% spec coverage—standards that are hard to meet manually.
- The filesystem is the database.  JSON files back both Kanbus project management and the core table engine, keeping humans, code, and AIs aligned on a simple source of truth.
- You can’t optimize what you don’t measure.  Benchmarks are first-class so we can improve performance with evidence.

## When to Use
- You want to ship data + query engine inside the same container with no external DB.
- Your tables are “small” (sweet spot ≤10k records, still reasonable to ~100k with the Rust backend).
- You need DynamoDB-style GSIs, associations, pagination, and nested queries without bringing in DynamoDB.
- You need a drop-in GraphQL replacement for batch or edge processing, driven by JSON exports.

## When Not to Use
- You have multi-million-record tables that demand SSD-backed columnar storage.
- You need cross-node clustering or distributed consensus.
- You require ACID transactions or high write concurrency.

## Installation

Three installation paths, one codebase:

```bash
# Rust standalone (fastest)
cargo install virtuus

# Pure Python (works anywhere)
pip install virtuus

# Python with bundled Rust backend (automatic if Rust toolchain is available)
pip install virtuus
```

The Python package transparently uses the Rust backend when available, falling back to pure Python:

```python
from virtuus import Database, Table, GSI, Sort
```

## Release Automation

Semantic Release bumps versions and publishes tags, PyPI, and crates.io automatically from conventional commits. Push a `feat:` commit to cut a new minor (e.g., 0.2.0) across both ecosystems.

## Quick Start

### As a Library

```python
from virtuus import Database

# Load from a YAML schema + data directory
db = Database.from_schema("schema.yml", "./data")

# Or build programmatically from a dict schema
schema = {"tables": {"users": {"primary_key": "id", "directory": "users"}}}
db = Database.from_schema_dict(schema, "./data")

# Query by primary key
user = db.execute({"users": {"pk": "user-123"}})

# Query by index
posts = db.execute({
    "posts": {
        "index": "by_user",
        "where": {"user_id": "user-123"},
        "sort_direction": "desc",
        "limit": 10,
    }
})

# Nested query with associations
result = db.execute({
    "users": {
        "pk": "user-123",
        "include": {
            "posts": {
                "limit": 5,
                "include": {"comments": {}}
            }
        }
    }
})
```

### As a CLI

```bash
# One-shot query
virtuus query --dir ./data --table users --index by_email --where email=alice@example.com

# Persistent HTTP server
virtuus serve --dir ./data --schema schema.yml --port 8080
```

The server accepts POST requests with JSON query dicts and returns JSON results.

## Storage Model

One directory per table, one JSON file per record:

```
data/
  users/
    user-abc-123.json
    user-def-456.json
  posts/
    post-001.json
    post-002.json
```

On startup, Virtuus scans each directory, loads all JSON files into memory, and builds GSI indexes. On write (`put`/`delete`), changes go to memory and to disk via atomic temp-file + rename.

## Schema Definition

Define tables, GSIs, and associations in a declarative YAML file:

```yaml
tables:
  users:
    primary_key: id
    directory: users
    gsis:
      by_email: { partition_key: email }
      by_org: { partition_key: org_id }
    associations:
      posts: { type: has_many, table: posts, index: by_user }
  posts:
    primary_key: id
    directory: posts
    gsis:
      by_user: { partition_key: user_id, sort_key: created_at }
    associations:
      author: { type: belongs_to, table: users, foreign_key: user_id }
```

## Core Concepts

### Global Secondary Indexes (GSIs)

GSIs provide fast lookups by non-primary-key fields. Each GSI has a hash partition key and an optional sorted range key.

Sort conditions on range keys support: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `between`, `begins_with`, `contains`.

### Query Interface

`db.execute(query_dict)` accepts a nested dict and returns nested results:

| Directive | Purpose |
|-----------|---------|
| `where` | Filter by field values |
| `index` | GSI name for indexed lookup |
| `pk` | Direct primary key lookup |
| `fields` | Field projection |
| `limit` | Max records returned |
| `sort` | Sort condition on range key |
| `sort_direction` | `asc` or `desc` (default: `asc`) |
| `next_token` | Cursor for pagination |
| `include` | Nested association resolution |

### Associations

| Type | Resolution |
|------|-----------|
| `has_many` | GSI query on foreign table |
| `belongs_to` | PK lookup on foreign table |
| `has_many_through` | GSI query on junction table, then PK lookups on target |

Self-referential associations (parent/child trees) are supported.

### Cache & Freshness

Virtuus tracks file modification times and detects when data on disk has changed:

- **JIT refresh**: Stale tables are automatically refreshed before query results are returned.
- **Warm reindex**: Proactively refresh all tables with `db.warm()` before queries need it.
- **Incremental refresh**: Only added, modified, and deleted files are reloaded — not the entire table.
- **Two-tier detection**: Cheap O(1) directory mtime check first; full O(N) file scan only when the directory has changed.

### Diagnostics & Quality-of-Life

- `table.describe()` / `db.describe()` — metadata overview: name, PK, GSIs, associations, record count, staleness
- `table.count()` / `table.count(index, value)` — record counts without materializing results
- `table.check()` — dry-run refresh showing what would change without actually refreshing
- `db.validate()` — referential integrity check across all `belongs_to` associations
- `table.export(directory)` — write all in-memory records back to JSON files
- Event hooks: `on_put`, `on_delete`, `on_refresh` callback lists for logging, metrics, or reactive patterns
- Opt-in put validation: warn or error when records are missing PK or GSI-indexed fields

## Dual Implementation

Virtuus is implemented identically in both Rust and Python, driven by shared Gherkin behavior specifications:

- **Python**: Pure Python implementation in `python/src/virtuus/_python/`
- **Rust**: Native implementation in `rust/src/`
- **Shared specs**: Gherkin feature files in `features/` are the single source of truth
- **PyO3 bridge**: The Rust implementation compiles as a Python extension module via PyO3 + maturin

Philosophy: start fast in Python, flip to Rust when ready. Development can begin immediately with the pure-Python backend (no toolchain needed). In production, install a Rust toolchain and the same import automatically loads the Rust backend for a drop-in speed bump—no API changes, just a faster engine.

Both implementations maintain 100% test coverage at all times.

## Development

```bash
make check              # lint + specs + coverage + parity — the one command
make coverage-python    # behave + coverage report --fail-under=100
make coverage-rust      # cargo tarpaulin --fail-under 100
make check-parity       # verify Python and Rust step definitions cover all Gherkin steps
make bench              # run benchmarks + generate visualizations
```

## Benchmarks

Benchmark goal: validate that Virtuus stays snappy for “small” datasets and that a container can carry its own data + engine without external dependencies. Warm-cache results are shown because most workloads in containers keep hot data in memory; cold-load numbers indicate one-time costs.

### Setup
- Fixture profile: `social_media` (users/posts/comments with GSIs)
- Sizes: 100, 500, 1k, 5k, 10k, 50k, 100k total records
- Environment: local filesystem, warm cache, Python runner using Rust backend when available
- Metrics: cold load (single table and full DB), incremental refresh, PK lookup, GSI hash-only lookup, GSI sorted lookup (range key)

### Results (Rust backend, warm cache)

The Rust backend represents the “production” path: same API, faster engine, still file-backed.

![Full database cold load](benchmarks/output/charts/full_database_cold_load.png)
![Single table cold load](benchmarks/output/charts/single_table_cold_load.png)
![Incremental refresh](benchmarks/output/charts/incremental_refresh.png)
![PK lookup](benchmarks/output/charts/pk_lookup.png)
![GSI partition lookup](benchmarks/output/charts/gsi_partition_lookup.png)
![GSI sorted query](benchmarks/output/charts/gsi_sorted_query.png)

- Full DB cold load scales linearly; ~41s at 100k total records. Use for batch ingest, not hot paths.
- Single-table cold load is sub-second through 10k; ~0.7s at 100k — fine for targeted reloads.
- Incremental refresh stays low single-digit ms even at 100k when only a file changes.
- PK lookup is effectively flat (timer noise at tiny sizes); O(1) hash lookups.
- GSI partition lookup grows with partition size; ~50 ms at 100k totals for hash-only access.
- GSI sorted query adds per-partition sort/filter; ~50–65 ms at 100k totals.

### Results (Python backend, warm cache)

Python-only benchmarks were run at 100, 1k, 10k, and 100k totals to mirror the small-footprint use case:

![Full database cold load (Python)](benchmarks/output_py/charts/full_database_cold_load.png)
![Single table cold load (Python)](benchmarks/output_py/charts/single_table_cold_load.png)
![Incremental refresh (Python)](benchmarks/output_py/charts/incremental_refresh.png)
![PK lookup (Python)](benchmarks/output_py/charts/pk_lookup.png)
![GSI partition lookup (Python)](benchmarks/output_py/charts/gsi_partition_lookup.png)
![GSI sorted query (Python)](benchmarks/output_py/charts/gsi_sorted_query.png)

- Cold loads remain comfortably sub-second through 10k totals; Python overhead is visible but still small for these sizes.
- PK lookups stay effectively free; GSI lookups stay within low-ms ranges at 10k totals.
- Incremental refresh is still sub-ms to low-ms for small corpora.

### Rust vs Python comparison (p95 or timing_ms)

Side-by-side bars for each benchmark at common corpus sizes. Values are p95 for iterative benchmarks and timing_ms for cold loads.

![Rust vs Python at 1k](benchmarks/output_compare/charts/compare_1000.png)
![Rust vs Python at 10k](benchmarks/output_compare/charts/compare_10000.png)
![Rust vs Python at 100k](benchmarks/output_compare/charts/compare_100000.png)

### Memory footprint (RSS) snapshots

We measured server RSS while serving pre-generated JSON datasets, varying corpus size and GSI count (with `posts` associations enabled). Lower data sizes stay lightweight; adding multiple GSIs raises steady-state memory modestly.

![Memory RSS](benchmarks/output_memory/memory_rss.png)

Interpretation:
- 100–1k users stay in the tens of MB range even with 3 GSIs.
- At 10k users, RSS remains comfortably under typical small-container budgets; each added GSI bumps memory linearly but gently.
- Associations (users↔posts) are included here; removing them would reduce RSS further.

See `benchmarks/output_memory/results.csv` for raw numbers.

- Gap is smallest on hot-path hash lookups (PK + hash-only GSI) at small sizes; Python stays in low single-digit ms.
- Sorted GSI queries widen the gap because the Python path spends more time ordering partition results.
- Cold-load gap is most pronounced because file I/O dominates and Rust streams faster; both stay linear with corpus size.

### Interpretation
- Virtuus is an excellent fit for “relatively small” deployments (≈10k total records or less) where you want to ship data + query engine together in a container without external services.
- Use the Rust backend when available for headroom; Python-only remains viable for small tables and still delivers sub-second behavior.
- If range queries dominate, keep partitions small or pre-sort buckets on write; incremental refresh is the preferred path to keep data fresh without full reloads.
 - Cold-load costs are linear; for tiny tables (≤10k) they remain sub-second, which is why the model works well for per-pod data snapshots.
 - The Python/Rust gap is most visible on cold loads; query latencies for small corpora stay close because data fits in memory and hash lookups dominate.

### How to regenerate
```bash
VIRTUUS_BENCH_DIR=benchmarks/output VIRTUUS_BENCH_TOTALS=100,500,1000,5000,10000,50000,100000 \
  python -m behave features/benchmarks/benchmark_scenarios.feature -n "Visualization generates charts"

VIRTUUS_BENCH_BACKEND=python \
VIRTUUS_BENCH_DIR=benchmarks/output_py \
VIRTUUS_BENCH_TOTALS=100,1000,10000,100000 \
  python tools/run_python_benchmarks.py

# Generate cross-backend comparison charts
python tools/bench_compare.py
```
Outputs land in `benchmarks/output/REPORT.md`, `benchmarks/output/benchmarks.json`, and `benchmarks/output/charts/*.png`.
```bash
make bench PROFILE=social_media SCALE=2
make bench-scale    # run at 1x, 2x, 5x, 10x for scaling charts
```

## License

MIT License. See `LICENSE`.
