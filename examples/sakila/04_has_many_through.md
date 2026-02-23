# Lesson 04: has_many_through

Use a junction table to resolve many-to-many relationships. Here a film includes its actors via the `film_actor` table.

Run it:

1. `conda run -n virtuus python examples/sakila/python/04_has_many_through.py`
2. `cargo run --manifest-path examples/sakila/rust/Cargo.toml --bin 04_has_many_through`

Python (from `examples/sakila/python/04_has_many_through.py`):

```python
query = {
    "films": {
        "pk": "714",
        "include": {
            "actors": {"fields": ["actor_id", "first_name", "last_name"]}
        },
    }
}
result = db.execute(query)
```

Rust (from `examples/sakila/rust/src/bin/04_has_many_through.rs`):

```rust
let query = json!({
    "films": {
        "pk": "714",
        "include": {
            "actors": {"fields": ["actor_id", "first_name", "last_name"]}
        }
    }
});
let result = db.execute(&query);
```
