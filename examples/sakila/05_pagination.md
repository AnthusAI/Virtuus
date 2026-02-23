# Lesson 05: Pagination

Pagination uses `limit` with a `next_token` cursor. The token is an offset into the result set.

Run it:

1. `conda run -n virtuus python examples/sakila/python/05_pagination.py`
2. `cargo run --manifest-path examples/sakila/rust/Cargo.toml --bin 05_pagination`

Python (from `examples/sakila/python/05_pagination.py`):

```python
query = {
    "rentals": {
        "index": "by_customer",
        "where": {"customer_id": "144"},
        "sort_direction": "desc",
        "limit": 5,
        "fields": ["rental_id", "rental_date", "customer_id"],
    }
}

page1 = db.execute(query)
if "next_token" in page1:
    query["rentals"]["next_token"] = page1["next_token"]
    page2 = db.execute(query)
```

Rust (from `examples/sakila/rust/src/bin/05_pagination.rs`):

```rust
let mut query = json!({
    "rentals": {
        "index": "by_customer",
        "where": {"customer_id": "144"},
        "sort_direction": "desc",
        "limit": 5,
        "fields": ["rental_id", "rental_date", "customer_id"]
    }
});

let page1 = db.execute(&query);
if let Some(next_token) = page1.get("next_token").and_then(|v| v.as_str()) {
    if let Some(table) = query.get_mut("rentals").and_then(|v| v.as_object_mut()) {
        table.insert("next_token".to_string(), json!(next_token));
    }
    let page2 = db.execute(&query);
}
```
