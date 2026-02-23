# Lesson 01: Load a Table

Load the schema, then fetch a customer by primary key.

Run it:

1. `conda run -n virtuus python examples/sakila/python/01_load_table.py`
2. `cargo run --manifest-path examples/sakila/rust/Cargo.toml --bin 01_load_table`

Python (from `examples/sakila/python/01_load_table.py`):

```python
from common import load_db

db = load_db()
result = db.execute({"customers": {"pk": "144"}})
```

Rust (from `examples/sakila/rust/src/bin/01_load_table.rs`):

```rust
let mut db = load_db();
let result = db.execute(&json!({"customers": {"pk": "144"}}));
```
