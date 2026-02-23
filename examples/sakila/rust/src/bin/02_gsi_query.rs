use serde_json::json;
use virtuus_sakila_examples::load_db;

fn main() {
    let mut db = load_db();
    let query = json!({
        "customers": {
            "index": "by_email",
            "where": {"email": "CLARA.SHAW@sakilacustomer.org"}
        }
    });
    let result = db.execute(&query);
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
}
