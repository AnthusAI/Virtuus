use cucumber::gherkin;
use cucumber::{given, then, when, World};
use virtuus_amplify::{Contract, Engine, EngineOptions};

#[derive(World, Debug, Default)]
pub struct AppWorld {
    contract: Option<Contract>,
    engine: Option<Engine>,
    error: Option<String>,
}

#[given("a blog contract")]
fn load_blog_contract(world: &mut AppWorld) {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = format!(
        "{}/../../../features/amplify/fixtures/blog.contract.json",
        manifest_dir
    );
    let json = std::fs::read_to_string(&path).expect("Failed to read blog fixture");

    match Contract::from_json(&json) {
        Ok(contract) => {
            world.contract = Some(contract);
        }
        Err(e) => {
            world.error = Some(e.to_string());
        }
    }
}

#[given("an apricity contract")]
fn load_apricity_contract(world: &mut AppWorld) {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = format!(
        "{}/../../../features/amplify/fixtures/apricity.contract.json",
        manifest_dir
    );
    let json = std::fs::read_to_string(&path).expect("Failed to read apricity fixture");

    match Contract::from_json(&json) {
        Ok(contract) => {
            world.contract = Some(contract);
        }
        Err(e) => {
            world.error = Some(e.to_string());
        }
    }
}

#[given(regex = "^the blog contract with (removed|replaced by:) at \"([^\"]+)\" and value (.*)$")]
fn load_mutated_blog_contract(
    world: &mut AppWorld,
    op: String,
    pointer: String,
    value_str: String,
) {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = format!(
        "{}/../../../features/amplify/fixtures/blog.contract.json",
        manifest_dir
    );
    let json_str = std::fs::read_to_string(&path).expect("Failed to read blog fixture");
    let mut value: serde_json::Value =
        serde_json::from_str(&json_str).expect("Failed to parse blog fixture");

    // Apply mutation
    if op == "removed" {
        // Find parent and key
        let parts: Vec<&str> = pointer.trim_start_matches('/').split('/').collect();
        if !parts.is_empty() {
            let key = parts.last().unwrap().to_string();
            let parent_pointer = if parts.len() > 1 {
                format!("/{}", parts[..parts.len() - 1].join("/"))
            } else {
                String::new()
            };
            if let Some(parent) = if parent_pointer.is_empty() {
                Some(&mut value)
            } else {
                value.pointer_mut(&parent_pointer)
            } {
                if let Some(obj) = parent.as_object_mut() {
                    obj.remove(&key);
                } else if let Some(arr) = parent.as_array_mut() {
                    if let Ok(idx) = key.parse::<usize>() {
                        arr.remove(idx);
                    }
                }
            }
        }
    } else if op == "replaced by:" {
        // Parse the value as JSON
        let replacement: serde_json::Value =
            serde_json::from_str(&value_str).unwrap_or(serde_json::json!(value_str));
        if let Some(obj) = value.pointer_mut(&pointer) {
            *obj = replacement;
        }
    }

    match Contract::from_json(&value.to_string()) {
        Ok(contract) => {
            world.contract = Some(contract);
        }
        Err(e) => {
            world.error = Some(e.to_string());
        }
    }
}

#[when("I open the engine")]
fn open_engine(world: &mut AppWorld) {
    if let Some(contract) = world.contract.clone() {
        match Engine::open(None, contract, EngineOptions) {
            Ok(engine) => {
                world.engine = Some(engine);
            }
            Err(e) => {
                world.error = Some(e.to_string());
            }
        }
    }
}

#[then("the engine has a Blog table")]
fn check_blog_table(world: &mut AppWorld) {
    if let Some(engine) = &world.engine {
        let description = engine.describe();
        let tables = description.get("tables").unwrap().as_array().unwrap();
        let blog_table = tables
            .iter()
            .find(|t| t.get("name").unwrap().as_str() == Some("Blog"));
        assert!(blog_table.is_some(), "Blog table not found");
    }
}

#[then("the engine has a Post table with postsByBlog index")]
fn check_post_table_index(world: &mut AppWorld) {
    if let Some(engine) = &world.engine {
        let description = engine.describe();
        let tables = description.get("tables").unwrap().as_array().unwrap();
        let post_table = tables
            .iter()
            .find(|t| t.get("name").unwrap().as_str() == Some("Post"));
        assert!(post_table.is_some(), "Post table not found");

        let post = post_table.unwrap();
        let indexes = post.get("indexes").unwrap().as_array().unwrap();
        let posts_by_blog = indexes
            .iter()
            .find(|i| i.get("queryField").unwrap().as_str() == Some("postsByBlog"));
        assert!(posts_by_blog.is_some(), "postsByBlog index not found");
    }
}

#[then("the engine has a Comment table with commentsByPost index")]
fn check_comment_table_index(world: &mut AppWorld) {
    if let Some(engine) = &world.engine {
        let description = engine.describe();
        let tables = description.get("tables").unwrap().as_array().unwrap();
        let comment_table = tables
            .iter()
            .find(|t| t.get("name").unwrap().as_str() == Some("Comment"));
        assert!(comment_table.is_some(), "Comment table not found");

        let comment = comment_table.unwrap();
        let indexes = comment.get("indexes").unwrap().as_array().unwrap();
        let comments_by_post = indexes
            .iter()
            .find(|i| i.get("queryField").unwrap().as_str() == Some("commentsByPost"));
        assert!(
            comments_by_post.is_some(),
            "commentsByPost implicit index not found"
        );
    }
}

#[then("the engine has a Tag table with composite key")]
fn check_tag_table(world: &mut AppWorld) {
    if let Some(engine) = &world.engine {
        let description = engine.describe();
        let tables = description.get("tables").unwrap().as_array().unwrap();
        let tag_table = tables
            .iter()
            .find(|t| t.get("name").unwrap().as_str() == Some("Tag"));
        assert!(tag_table.is_some(), "Tag table not found");

        let tag = tag_table.unwrap();
        assert!(
            tag.get("partitionKey").is_some(),
            "Tag should have partitionKey"
        );
        assert!(tag.get("sortKey").is_some(), "Tag should have sortKey");
    }
}

#[then("the contract is valid")]
fn check_contract_valid(world: &mut AppWorld) {
    if let Some(error) = &world.error {
        panic!("Error occurred: {}", error);
    }
    assert!(world.contract.is_some(), "Contract should be loaded");
    assert!(world.error.is_none(), "No error should have occurred");
}

#[then(regex = "^the error contains '([^']+)'$")]
fn check_error_message_single(world: &mut AppWorld, expected: String) {
    assert!(world.error.is_some(), "Expected an error but none occurred");
    let error_msg = world.error.as_ref().unwrap();
    assert!(
        error_msg.contains(&expected),
        "Error '{}' does not contain '{}'",
        error_msg,
        expected
    );
}

#[then(regex = "^the error contains \"([^\"]+)\"$")]
fn check_error_message_double(world: &mut AppWorld, expected: String) {
    assert!(world.error.is_some(), "Expected an error but none occurred");
    let error_msg = world.error.as_ref().unwrap();
    assert!(
        error_msg.contains(&expected),
        "Error '{}' does not contain '{}'",
        error_msg,
        expected
    );
}

#[then("the engine description is:")]
fn check_engine_description(world: &mut AppWorld, step: &gherkin::Step) {
    if let Some(engine) = &world.engine {
        let expected_str = step.docstring().expect("Expected docstring");
        let expected: serde_json::Value =
            serde_json::from_str(expected_str).expect("Failed to parse expected JSON");
        let actual = engine.describe();

        assert_eq!(
            actual,
            expected,
            "Engine description does not match.\nExpected:\n{}\n\nActual:\n{}",
            serde_json::to_string_pretty(&expected).unwrap_or_default(),
            serde_json::to_string_pretty(&actual).unwrap_or_default()
        );
    } else {
        panic!("No engine loaded");
    }
}

#[then("the engine has tables:")]
fn check_engine_tables(world: &mut AppWorld, step: &gherkin::Step) {
    if let Some(engine) = &world.engine {
        let tables_str = step.docstring().expect("Expected docstring");
        let expected_tables: Vec<&str> = tables_str.split(',').map(|s| s.trim()).collect();

        let description = engine.describe();
        let tables = description.get("tables").unwrap().as_array().unwrap();
        let actual_tables: Vec<&str> = tables
            .iter()
            .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
            .collect();

        let mut actual_sorted = actual_tables.clone();
        actual_sorted.sort();
        let mut expected_sorted = expected_tables.clone();
        expected_sorted.sort();

        assert_eq!(
            actual_sorted, expected_sorted,
            "Table names do not match. Expected: {:?}, Actual: {:?}",
            expected_sorted, actual_sorted
        );
    } else {
        panic!("No engine loaded");
    }
}

#[then(regex = "^the tables have these indexes:$")]
fn check_tables_indexes(world: &mut AppWorld, step: &gherkin::Step) {
    if let Some(engine) = &world.engine {
        let table_str = step.docstring().expect("Expected docstring");
        let lines: Vec<&str> = table_str.lines().collect();

        let description = engine.describe();
        let tables = description.get("tables").unwrap().as_array().unwrap();

        // Parse the table
        for line in lines {
            if line.trim().is_empty() {
                continue;
            }

            let parts: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
            if parts.len() < 2 {
                continue;
            }

            let model_name = parts[0];
            let expected_indexes: Vec<&str> = parts[1].split(',').map(|s| s.trim()).collect();

            let model_table = tables
                .iter()
                .find(|t| t.get("name").and_then(|n| n.as_str()) == Some(model_name))
                .unwrap_or_else(|| panic!("Model {} not found", model_name));

            let indexes = model_table.get("indexes").unwrap().as_array().unwrap();
            let mut actual_indexes: Vec<String> = indexes
                .iter()
                .filter_map(|i| {
                    i.get("name")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
            actual_indexes.sort();
            let mut expected_sorted: Vec<String> =
                expected_indexes.iter().map(|s| s.to_string()).collect();
            expected_sorted.sort();

            assert_eq!(
                actual_indexes, expected_sorted,
                "Indexes for model {} do not match. Expected: {:?}, Actual: {:?}",
                model_name, expected_sorted, actual_indexes
            );
        }
    } else {
        panic!("No engine loaded");
    }
}

#[then(regex = "^table \"([^\"]+)\" has partition \"([^\"]+)\" and sort \"([^\"]+)\"$")]
fn check_table_partition_sort(
    world: &mut AppWorld,
    table_name: String,
    partition: String,
    sort: String,
) {
    if let Some(engine) = &world.engine {
        let description = engine.describe();
        let tables = description.get("tables").unwrap().as_array().unwrap();

        let table = tables
            .iter()
            .find(|t| t.get("name").and_then(|n| n.as_str()) == Some(&table_name))
            .unwrap_or_else(|| panic!("Table {} not found", table_name));

        let actual_partition = table
            .get("partitionKey")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("partitionKey not found for table {}", table_name));

        let actual_sort = table
            .get("sortKey")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("sortKey not found for table {}", table_name));

        assert_eq!(
            actual_partition, partition,
            "Partition key mismatch for table {}",
            table_name
        );
        assert_eq!(
            actual_sort, sort,
            "Sort key mismatch for table {}",
            table_name
        );
    } else {
        panic!("No engine loaded");
    }
}

#[when("I put these Post records:")]
fn put_post_records(world: &mut AppWorld, step: &gherkin::Step) {
    if let Some(engine) = &mut world.engine {
        let json_str = step.docstring().expect("Expected JSON docstring");
        let records: Vec<serde_json::Value> =
            serde_json::from_str(json_str).expect("Failed to parse records JSON");

        if let Some(table) = engine.table_mut("Post") {
            for record in records {
                table.put(record);
            }
        } else {
            panic!("Post table not found");
        }
    } else {
        panic!("No engine loaded");
    }
}

#[then(regex = "^querying postsByBlog for blogId \"([^\"]+)\" returns ids \\[(.+)\\]$")]
fn query_posts_by_blog(world: &mut AppWorld, blog_id: String, expected_ids_str: String) {
    if let Some(engine) = &mut world.engine {
        // First verify the table and GSI exist using the immutable accessor
        let table = engine.table("Post");
        assert!(
            table.is_some(),
            "Post table should be accessible via engine.table()"
        );

        let table = table.unwrap();
        assert!(
            table.gsis().contains_key("postsByBlog"),
            "postsByBlog index should exist on Post table"
        );

        // Now query the GSI using the mutable accessor
        if let Some(table_mut) = engine.table_mut("Post") {
            let blog_id_value = serde_json::json!(blog_id);
            let results = table_mut.query_gsi("postsByBlog", &blog_id_value, None, false);

            let result_ids: Vec<String> = results
                .iter()
                .filter_map(|record| {
                    record
                        .get("id")
                        .and_then(|id| id.as_str())
                        .map(|s| s.to_string())
                })
                .collect();

            // Parse expected ids from step parameter
            let expected_ids: Vec<String> = expected_ids_str
                .split(',')
                .map(|s| s.trim().trim_matches('"').to_string())
                .collect();

            assert_eq!(
                result_ids, expected_ids,
                "Query results don't match. Got {:?}, expected {:?}",
                result_ids, expected_ids
            );
        } else {
            panic!("Post table not found");
        }
    } else {
        panic!("No engine loaded");
    }
}

#[tokio::main]
async fn main() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let features_path = format!(
        "{}/../../../features/amplify/contract.feature",
        manifest_dir
    );
    AppWorld::run(&features_path).await;
}
