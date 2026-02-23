use serde_json::json;
use virtuus_sakila_examples::load_db;

fn main() {
    let mut db = load_db();
    let result = db.execute(&json!({"customers": {"pk": "144"}}));
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
}
