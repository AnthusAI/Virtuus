# Sakila Examples

These lessons use a small Sakila-derived dataset to demonstrate Virtuus basics.

## Setup

Generate the JSON tables and schema-ready data directory:

```bash
conda run -n virtuus python examples/sakila/build_sakila.py
```

This creates `examples/sakila/data/` with one JSON file per record.

## Run Examples

Python:

```bash
conda run -n virtuus python examples/sakila/python/01_load_table.py
```

Rust:

```bash
cargo run --manifest-path examples/sakila/rust/Cargo.toml --bin 01_load_table
```

## Lessons

- [Lesson 01: Load a table](01_load_table.md)
- [Lesson 02: GSI query](02_gsi_query.md)
- [Lesson 03: Associations](03_associations.md)
- [Lesson 04: has_many_through](04_has_many_through.md)
- [Lesson 05: Pagination](05_pagination.md)

## DynamoDB-Style Limits

Virtuus is modeled on DynamoDB-style access patterns. Plan your schema up front and expect these constraints:

- Queries target a single table at a time. There are no arbitrary SQL joins.
- GSI queries require the partition key; additional filters are limited.
- Range-key sorting happens only on the index sort key.
- Associations resolve through GSIs or primary keys; complex cross-table joins are out of scope.
- `include` supports `fields` and nested `include`, but does not apply `limit` or sorting inside associations.
