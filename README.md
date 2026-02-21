# Virtuus

A file-backed in-memory indexed table engine. Virtuus treats folders of JSON files as indexed tables — like DynamoDB tables backed by the filesystem.

Data lives on disk as one JSON file per record. Virtuus loads it into memory, builds indexes, and provides fast query access with DynamoDB-style Global Secondary Indexes, associations, pagination, and a nested query interface. Writes persist back to disk atomically.

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

## Quick Start

### As a Library

```python
from virtuus import Database

# Load from a YAML schema + data directory
db = Database.from_schema("schema.yml", "./data")

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

Virtuus includes a pluggable benchmark framework with fixture generation, timing measurement, and chart visualization:

- **Fixture profiles**: `social_media`, `ecommerce`, `complex_hierarchy` — generating up to 900K+ linked records
- **Load-time benchmarks**: cold load, incremental refresh, scale curves
- **Query-time benchmarks**: PK lookup, GSI query, nested includes, pagination
- **Concurrency benchmarks**: throughput scaling with thread count, reads during refresh
- **Memory benchmarks**: per-record overhead, GSI overhead, refresh leak detection
- **Visualization**: SVG charts comparing Python vs Rust, scaling behavior, latency distributions

```bash
make bench PROFILE=social_media SCALE=2
make bench-scale    # run at 1x, 2x, 5x, 10x for scaling charts
```

## License

MIT License. See `LICENSE`.
