use serde_json::json;
use virtuus_sakila_examples::load_db;

fn main() {
    let mut db = load_db();
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
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
}
