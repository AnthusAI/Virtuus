# Lesson 03: Associations

Associations resolve related records through GSIs or primary keys. Here we load one inventory record and include its film plus the rentals, each with the customer attached.

Run it:

1. `conda run -n virtuus python examples/sakila/python/03_associations.py`
2. `cargo run --manifest-path examples/sakila/rust/Cargo.toml --bin 03_associations`

Python (from `examples/sakila/python/03_associations.py`):

```python
query = {
    "inventory": {
        "pk": "3243",
        "include": {
            "film": {"fields": ["film_id", "title", "rating"]},
            "rentals": {
                "fields": ["rental_id", "rental_date", "customer_id", "customer"],
                "include": {
                    "customer": {
                        "fields": [
                            "customer_id",
                            "first_name",
                            "last_name",
                            "email",
                        ]
                    }
                },
            },
        },
    }
}
result = db.execute(query)
```

Rust (from `examples/sakila/rust/src/bin/03_associations.rs`):

```rust
let query = json!({
    "inventory": {
        "pk": "3243",
        "include": {
            "film": {"fields": ["film_id", "title", "rating"]},
            "rentals": {
                "fields": ["rental_id", "rental_date", "customer_id", "customer"],
                "include": {
                    "customer": {
                        "fields": [
                            "customer_id",
                            "first_name",
                            "last_name",
                            "email"
                        ]
                    }
                }
            }
        }
    }
});
let result = db.execute(&query);
```
