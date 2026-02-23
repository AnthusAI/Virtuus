# Lesson 02: GSI Query

Use a Global Secondary Index (GSI) to query by a non-primary key field. GSI queries must include the partition key in `where`.

Run it:

1. `conda run -n virtuus python examples/sakila/python/02_gsi_query.py`
2. `cargo run --manifest-path examples/sakila/rust/Cargo.toml --bin 02_gsi_query`

Python (from `examples/sakila/python/02_gsi_query.py`):

```python
query = {
    "customers": {
        "index": "by_email",
        "where": {"email": "CLARA.SHAW@sakilacustomer.org"},
    }
}
result = db.execute(query)
```

Rust (from `examples/sakila/rust/src/bin/02_gsi_query.rs`):

```rust
let query = json!({
    "customers": {
        "index": "by_email",
        "where": {"email": "CLARA.SHAW@sakilacustomer.org"}
    }
});
let result = db.execute(&query);
```
