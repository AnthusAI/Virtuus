use serde_json::json;
use virtuus_sakila_examples::load_db;

fn main() {
    let mut db = load_db();
    let query = json!({
        "films": {
            "pk": "714",
            "include": {
                "actors": {"fields": ["actor_id", "first_name", "last_name"]}
            }
        }
    });
    let result = db.execute(&query);
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
}
