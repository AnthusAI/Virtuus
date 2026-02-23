use serde_json::{json, Value};
use virtuus_sakila_examples::load_db;

fn count_items(result: &Value) -> usize {
    result
        .get("items")
        .and_then(|v| v.as_array())
        .map(|items| items.len())
        .unwrap_or(0)
}

fn main() {
    let mut db = load_db();
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
    println!("page1 items: {}", count_items(&page1));
    println!("page1 next_token: {}", page1.get("next_token").unwrap_or(&Value::Null));

    if let Some(next_token) = page1.get("next_token").and_then(|v| v.as_str()) {
        if let Some(table) = query.get_mut("rentals").and_then(|v| v.as_object_mut()) {
            table.insert("next_token".to_string(), json!(next_token));
        }
        let page2 = db.execute(&query);
        println!("page2 items: {}", count_items(&page2));
        println!("page2 next_token: {}", page2.get("next_token").unwrap_or(&Value::Null));
    }
}
