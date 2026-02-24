# Virtuus \ ˈvər-tyü-əs \

A **virtual-table database system** — a file-backed, in-memory indexed table engine. Virtuus treats folders of JSON files as indexed tables — like DynamoDB tables backed by the filesystem.

[![CI](https://github.com/AnthusAI/Virtuus/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AnthusAI/Virtuus/actions/workflows/ci.yml)
![Python Coverage](https://img.shields.io/badge/python%20coverage-100%25-brightgreen)
![Rust Coverage](https://img.shields.io/badge/rust%20coverage-100%25-brightgreen)
[![PyPI](https://img.shields.io/pypi/v/virtuus.svg?cacheSeconds=300)](https://pypi.org/project/virtuus/)
[![Crates.io](https://img.shields.io/crates/v/virtuus.svg)](https://crates.io/crates/virtuus)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

The normal paradigm with database servers is "warm queries"—the database has already been loaded, the query engine is running, and everything is ready to go. Virtuus is different. It is built for the use case where you have structured data on disk that you need to query *fast* from a **cold start**. Virtuus makes that happen with a super-simple architecture. This use case is particularly relevant for **durable computing**, where long-running processes like **AI agents** need to periodically stop, suspend, and then resume again later as quickly as possible.  Maybe in **locked-down, containerized sandbox environments** with no network access to 'real' databases.

Data lives on disk as one JSON file per record. In many use cases, you can **eliminate the database entirely**: load JSON files from disk, build indexes and associations like DynamoDB, and serve fast queries with no external dependencies. Virtuus builds indexes in memory and, for file-backed tables, defaults to an **index-only** mode where records stay on disk while indexes remain hot. You can opt into full in-memory records when needed. Writes persist back to disk atomically.

## Motivation / Operating Context

Virtuus was built to take [Plexus](https://github.com/AnthusAI/Plexus) — one of our mission-critical production systems — into more regulated, isolated environments. Plexus uses a GraphQL control plane and serves high-availability, high-throughput, high-volume workloads under strict regulatory and information-security constraints. Our motivation is to support scenarios where workers must run in tightly regulated environments and cannot directly reach the central control plane. Shipping the data and query engine with the worker removes that dependency while keeping the API shape consistent.

## Guiding Values

- **It's better to eliminate a problem than to solve it.** Ask whether you truly need a database and the lifetime cost it adds; the filesystem may already be enough.
- **Gherkin behavior specifications are the source code.** The Gherkin spec is the single source of truth; Rust and Python implementations are generated artifacts.
- **Raise the bar.** Use AI to raise the bar, not to just create more AI slop faster.
- **The filesystem is the database.** JSON files back both Kanbus project management and the core table engine.
- **You can’t optimize what you don’t measure.** Benchmarks are first-class.

## Load-First Runtime Pattern

We increasingly run systems that are **load-first**: large ML models and multi-GB datasets are loaded before any work can begin. If multi-second or multi-minute loads are already normal for model startup, the same time-shifted assumption can simplify data querying problems: load once, index in memory, and operate fast. You don’t need traditional ETL (extract-transform-load) to get indexed access if you can load a folder and build the indexes directly in memory. That’s the core **virtual-table** mindset.

## Performance Highlights

Performance results are published from EC2 benchmark runs only. See the EC2 section below for charts and raw data.

## Eliminate the Database, Don’t Optimize It

This pattern showed up in [Kanbus](https://github.com/AnthusAI/Kanbus), inspired by [Beads](https://github.com/steveyegge/beads). Beads uses a SQLite sidecar to index a JSONL file. Kanbus asked: what if we just load the JSONL and scan it directly?

The traditional answer here is “run a local database” (Mongo, Redis, SQLite, etc.), then do ETL, orchestrate it in a container, keep it synchronized, and build repair tooling for drift and corruption. Virtuus changes the game by cutting that whole layer out.

Benchmark results are reported from EC2 runs in the section below; local charts are intentionally removed to avoid mixing machine-specific data.

## When to Use

Are you using a database to look up records quickly out of a big pile of JSON files?  Maybe you don't need to do that.  You might want to think about just loading the files directly, for things like:

- DynamoDB-style GSIs, associations, pagination, and nested queries without bringing in DynamoDB.
- Time-shift a one-time cold load to unlock extremely fast PK lookups and low-latency GSI queries.
- Ship data + query engine in the same container with no external DB, including isolated or regulated environments where the control plane cannot be reached.
- A drop-in GraphQL replacement for batch or edge processing, driven by JSON exports.

## When Not to Use

It's not a real database!  Don't use it if:

- You require ACID transactions or high write concurrency.
- You need cross-node clustering or distributed consensus.
- You have multi-million-record tables that demand SSD-backed columnar storage.
- You cannot tolerate noticeable cold-start latency or your memory budget is tight for in-memory indexing.

## How Virtuus Compares

Two tools come up often when people discover Virtuus: TinyDB and DuckDB. All three work with structured data outside a traditional server database, but they are built for different problems.

A core motivation for Virtuus is that **the JSON files on disk are the single source of truth**. If you already have structured data as JSON files — exported from an API, generated by a pipeline, or synced from a control plane — and you want to query it fast, the traditional answer is to import it into a separate database and keep the two in sync. You end up with two copies of the truth and a synchronization layer between them. Virtuus eliminates that layer entirely: indexes are derived at load time from whatever files are on disk, never persisted as a separate artifact, and there is no import step and no drift to repair.

Data lives as **one JSON file per record, identified by UUID**. This isn't just a storage choice — it's a concurrency and distribution strategy. When multiple distributed processes write to the same dataset (e.g., workers in different containers or branches), separate files mean merge conflicts are rare: two writers only conflict if they modify the same record. This makes Virtuus data **naturally Git-friendly and eventually-consistent** across distributed environments. TinyDB, DuckDB, and SQLite all use monolithic storage (a single file or binary database), which makes concurrent distributed writes and Git-based merging fundamentally harder.

### vs. TinyDB

TinyDB stores every document in a **single JSON file** and queries by scanning the entire file on every read. There are no secondary indexes, sorting, pagination, associations, or keyword search — by design. TinyDB is built for truly tiny datasets where simplicity matters more than performance, and it does that job well.

Virtuus stores one JSON file per record, builds DynamoDB-style GSIs on load, and supports indexed lookups, associations, cursor-based pagination, sort conditions on range keys, and keyword search on configured fields. The one-file-per-record layout also means that **concurrent writes from distributed processes are far less likely to conflict** — two workers only collide if they touch the same record. With TinyDB's single-file model, any two concurrent writes to different records still conflict on the same file.

TinyDB's single-file, single-process model means it is **not designed for distributed runtime environments**. If your dataset is a few hundred records, writes come from a single process, and you have no need for indexes, relationships, or text search, TinyDB is a great fit. If you need indexed queries, keyword search, relationships between tables, cold-start performance at thousands of records, or merge-friendly distributed writes, Virtuus is designed for that.

TinyDB is Python-only and in maintenance mode (stable, not adding features). Virtuus has **dual Rust/Python implementations**, so you can start in pure Python and switch to the Rust backend for production speed with no API changes.

### vs. DuckDB

DuckDB is an **embedded analytical (OLAP) SQL database** — columnar storage, vectorized execution, parallel processing. It excels at aggregations, joins, and filters across large datasets. Virtuus is more of an **OLTP-style operational engine** for record-level access: primary-key lookups, GSI queries, association traversal, and paginated results. Virtuus can handle aggregate work too, but a warm-start OLAP engine like DuckDB will do that more efficiently. If you think in SQL and need to answer questions like "average order value by region," DuckDB is the right tool. If you think in access patterns and need to answer questions like "get user X and their 10 most recent posts with comments," Virtuus is the right tool.

DuckDB is optimized for bulk columnar scans over large flat files or Parquet datasets, not for serving as an indexed document store over directories of per-record JSON files. Virtuus treats that directory-of-JSON-files layout as a first-class storage model, builds GSIs from it on cold start, and defaults to index-only mode where records stay on disk. **The two tools are complementary**: Virtuus is a utilitarian tool for moving data around simply and transforming it in containers — more of a factor in the ETL stage — while DuckDB shines for the final analysis. In some architectures you might use both.

For keyword search, DuckDB has a full-text search extension that uses BM25 ranking — more sophisticated than Virtuus's keyword search, but the **index must be manually rebuilt** when data changes. Virtuus builds its search index automatically from configured fields on load, the same way it builds GSIs: derived, never persisted separately, and always in sync with the data on disk.

### A note on SQLite FTS5

SQLite comes up in similar conversations, and its FTS5 module is worth knowing about. FTS5 (Full-Text Search version 5) is SQLite's built-in full-text search engine — it builds **persistent search indexes** that survive process restarts, update automatically on writes, and support advanced features like phrase queries, prefix matching, and BM25 ranking. For pure full-text search capability, **FTS5 is the most mature option in this space**. However, SQLite shares the same monolithic-file limitation as TinyDB and DuckDB: a single database file that doesn't lend itself to distributed writes or Git-based merging. Virtuus's keyword search is simpler than FTS5 (token intersection without ranking or phrase queries), but it fits the same **derived-index philosophy** as the rest of Virtuus — indexes are built from the JSON files on disk and never need to be persisted, merged, or synchronized separately.

## Core Concepts & Features

### Storage Model

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

On startup, Virtuus scans each directory, builds in-memory indexes, and (by default for file-backed tables) keeps only those indexes resident while records remain on disk. On write (`put`/`delete`), changes go to indexes in memory and to disk via atomic temp-file + rename.

Storage modes:
- `index_only`: default for file-backed tables, records stay on disk.
- `memory`: keep full records in RAM (opt-in via schema).

**Index-only storage:**
Virtuus keeps the lightweight indexes in memory and leaves the full records on disk. That means memory growth is driven mostly by the indexes, not by the size of each record. It’s a practical way to handle large datasets without keeping every JSON document resident in RAM. When you query, Virtuus uses the index to find matching record IDs, then reads only the records it needs.

### Schema Definition

Define tables, GSIs, and associations in a declarative YAML file:

```yaml
tables:
  users:
    primary_key: id
    directory: users
    storage: index_only
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

Optional keyword search configuration:

```yaml
tables:
  news:
    primary_key: id
    directory: news
    search:
      fields: [title, body]
```
```


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
| `search` | Keyword search across configured fields |
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

### Dual Implementation

Virtuus is implemented identically in both Rust and Python, driven by shared Gherkin behavior specifications:

- **Python**: Pure Python implementation in `python/src/virtuus/_python/`
- **Rust**: Native implementation in `rust/src/`
- **Shared specs**: Gherkin feature files in `features/` are the single source of truth
- **PyO3 bridge**: The Rust implementation compiles as a Python extension module via PyO3 + maturin

Philosophy: start fast in Python, flip to Rust when ready. Development can begin immediately with the pure-Python backend (no toolchain needed). In production, install a Rust toolchain and the same import automatically loads the Rust backend for a drop-in speed bump—no API changes, just a faster engine.

Both implementations maintain 100% test coverage at all times.

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

## Examples (Sakila)

Examples live under `examples/sakila/` and use a small Sakila-derived dataset. Generate the JSON data with:

```bash
conda run -n virtuus python examples/sakila/build_sakila.py
```

Lessons and runnable examples (Python + Rust):

- [Lesson 01: Load a Table](examples/sakila/01_load_table.md) — shows how to load the schema and fetch a single record by primary key, which is the simplest and fastest query path. Code: [python](examples/sakila/python/01_load_table.py), [rust](examples/sakila/rust/src/bin/01_load_table.rs).
- [Lesson 02: GSI Query](examples/sakila/02_gsi_query.md) — demonstrates querying by a non-primary key using a Global Secondary Index and explains the required partition key constraint. Code: [python](examples/sakila/python/02_gsi_query.py), [rust](examples/sakila/rust/src/bin/02_gsi_query.rs).
- [Lesson 03: Associations](examples/sakila/03_associations.md) — uses `include` to resolve `belongs_to` and `has_many` relationships in a single query response, showing how Virtuus emulates joins through indexes. Code: [python](examples/sakila/python/03_associations.py), [rust](examples/sakila/rust/src/bin/03_associations.rs).
- [Lesson 04: has_many_through](examples/sakila/04_has_many_through.md) — walks a many-to-many association through a junction table, illustrating how to model cross-table relationships without SQL joins. Code: [python](examples/sakila/python/04_has_many_through.py), [rust](examples/sakila/rust/src/bin/04_has_many_through.rs).
- [Lesson 05: Pagination](examples/sakila/05_pagination.md) — demonstrates cursor-style pagination with `limit` and `next_token`, plus field projection to keep payloads lean. Code: [python](examples/sakila/python/05_pagination.py), [rust](examples/sakila/rust/src/bin/05_pagination.rs).

## Benchmarks (EC2 Only)

All benchmark results and charts in this README are sourced from EC2 runs. Local benchmark outputs have been removed to avoid mixing machine-specific results.

### EC2 benchmark harness (current source of truth)

We run the storage-mode suite and cold-start suite on EC2 across three instance types:
- `t3.nano` (intentionally underpowered)
- `t3.medium`
- `r6i.large`

Results are uploaded to S3 during the run and synced locally for charting.

Sync the latest EC2 artifacts and rebuild charts:
```bash
python3 tools/sync_bench_results.py \
  --bucket ec2benchstack-benchresults03ded75d-kfwkjrgqavpy \
  --prefix virtuus-bench \
  --profile default
```

EC2 charts are generated under `benchmarks/output_storage/charts/` and embedded below once EC2 data is synced. If you don’t see charts here, the EC2 runs are still in progress.

### Choosing a Storage Mode (EC2 Results)

Virtuus supports two storage modes per table:
- `index_only` (default for file-backed tables): keep only indexes in memory; read full records from disk on demand.
- `memory`: keep full records resident in RAM alongside indexes.

When to pick which:
- **`index_only`**: large/variable record sizes, disk-backed datasets, and hot paths that are indexed (PK, GSI, keyword search on selected fields). Memory scales with indexes, not payload size.
- **`memory`**: the table fits in RAM and you want the lowest-latency scans or wide projections.
- You can mix per table (e.g., `users` in memory, `posts` index-only).

## Development

```bash
make check              # lint + specs + coverage + parity — the one command
make coverage-python    # behave + coverage report --fail-under=100
make coverage-rust      # cargo tarpaulin --fail-under 100
make check-parity       # verify Python and Rust step definitions cover all Gherkin steps
make bench              # run benchmarks + generate visualizations
```

### EC2 benchmark harness (optional, long-run)

We provide a helper to run the full storage-mode suite on EC2 for 1M-record scale tests across multiple instance types:

```bash
python tools/run_ec2_storage_benchmarks.py \
  --ami <ami-id> \
  --subnet-id <subnet-xxxx> \
  --security-group-id <sg-xxxx> \
  --instance-types t3.nano,t3.small,m6i.large \
  --totals 1000,10000,100000,1000000 \
  --s3-bucket my-bucket --s3-prefix virtuus-bench \
  --profile default \
  --no-dry-run
```

Defaults: runs both backends, both storage modes, record sizes 0.5/2/10 KB, profiles `single_table` and `social_media`, with timeouts and RSS enabled. Results/charts stay on the instance and can optionally upload to S3 when `--s3-bucket` is provided. Dry-run is on by default; supply `--no-dry-run` to launch.
You can copy `tools/ec2_params.example.json`, fill in real IDs, and translate it into the CLI args above.
Or invoke from the JSON directly:
```bash
python tools/run_ec2_from_params.py --params tools/ec2_params.example.json --no-dry-run
```
Optional sanity check before launching:
```bash
python tools/check_ec2_params.py --params tools/ec2_params.example.json
```

Charts gallery:
- Use `python tools/render_storage_gallery.py` to build a local HTML gallery (not committed).

### Cold-start benchmarks (SQLite, DuckDB, TinyDB, Virtuus)

Cold-start benchmarks measure “time to first query” in a fresh process. This compares Virtuus against SQLite, DuckDB, and TinyDB for a PK lookup and a simple full‑text‑like search.

Run locally (outputs are generated under `benchmarks/output_cold_start/`, which is git‑ignored):
```bash
PYTHONPATH=python/src python tools/bench_cold_start.py
PYTHONPATH=python/src python tools/bench_cold_start_charts.py
```

Configuration knobs:
- `VIRTUUS_COLD_TOTALS` (e.g., `10000,100000`)
- `VIRTUUS_COLD_RECORD_SIZES_KB` (e.g., `0.5,2`)
- `VIRTUUS_COLD_ITERATIONS` (e.g., `5`)
- `VIRTUUS_COLD_ENGINES` (e.g., `sqlite,duckdb,tinydb,virtuus`)
- `VIRTUUS_COLD_STORAGE_MODES` (e.g., `index_only,memory`)

The EC2 CDK stack runs these cold‑start benchmarks automatically alongside the storage‑mode suite.


## Release Automation

Semantic Release bumps versions and publishes tags, PyPI, and crates.io automatically from conventional commits. Push a `feat:` commit to cut a new minor (e.g., 0.2.0) across both ecosystems; fixes/docs/chore still ship as patch releases. Releases are gated on CI success to avoid partial publishes. Conventional commits are the single switch for automated releases.

## License

MIT License. See `LICENSE`.
