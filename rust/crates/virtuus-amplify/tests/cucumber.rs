use cucumber::gherkin;
use cucumber::{given, then, when, World};
use regex::Regex;
use serde_json::{json, Value};
use std::path::PathBuf;
use uuid::Uuid;
use virtuus_amplify::{Contract, Engine, EngineOptions};

#[derive(World, Debug, Default)]
pub struct AppWorld {
    contract: Option<Contract>,
    engine: Option<Engine>,
    error: Option<String>,
    last_operation_data: Option<Value>,
    last_operation_errors: Option<Vec<Value>>,
    last_model: Option<String>,
    directory: Option<PathBuf>,
    last_next_token: Option<String>,
    selection_set: Option<Vec<String>>,
    relationship_args: Option<Value>,
    stored_next_tokens: std::collections::HashMap<String, String>,
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

#[given(regex = "^a (\\w+) exists with:$")]
fn given_resource_exists(world: &mut AppWorld, model: String, step: &gherkin::Step) {
    // First ensure engine is open
    if world.engine.is_none() {
        if let Some(contract) = world.contract.clone() {
            match Engine::open(None, contract, EngineOptions) {
                Ok(engine) => {
                    world.engine = Some(engine);
                }
                Err(e) => {
                    world.error = Some(e.to_string());
                    return;
                }
            }
        }
    }

    // Now create the record
    if let Some(engine) = &mut world.engine {
        let json_str = step.docstring().expect("Expected JSON docstring");
        let input: Value = serde_json::from_str(json_str).expect("Failed to parse input JSON");

        world.last_model = Some(model.clone());

        match engine.call(&model, "create", &input) {
            Ok((data, errors)) => {
                world.last_operation_data = Some(data);
                world.last_operation_errors = errors;
                world.error = None;
                // Assertion: given step should succeed
                if let Some(ref errs) = world.last_operation_errors {
                    if !errs.is_empty() {
                        world.error = Some(format!("Given step failed: {:?}", errs));
                    }
                }
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

#[when(regex = "^I create a (\\w+) with:$")]
fn create_resource(world: &mut AppWorld, model: String, step: &gherkin::Step) {
    if let Some(engine) = &mut world.engine {
        let json_str = step.docstring().expect("Expected JSON docstring");
        let input: Value = serde_json::from_str(json_str).expect("Failed to parse input JSON");

        world.last_model = Some(model.clone());

        match engine.call(&model, "create", &input) {
            Ok((data, errors)) => {
                world.last_operation_data = Some(data);
                world.last_operation_errors = errors;
                world.error = None;
            }
            Err(e) => {
                world.error = Some(e.to_string());
                world.last_operation_data = None;
                world.last_operation_errors = None;
            }
        }
    } else {
        panic!("No engine loaded");
    }
}

#[when(regex = "^I get the (\\w+) with id \"([^\"]+)\"$")]
fn get_resource(world: &mut AppWorld, model: String, id: String) {
    if let Some(engine) = &mut world.engine {
        world.last_model = Some(model.clone());

        let input = json!({"id": id});

        match engine.call(&model, "get", &input) {
            Ok((data, errors)) => {
                world.last_operation_data = Some(data);
                world.last_operation_errors = errors;
                world.error = None;
            }
            Err(e) => {
                world.error = Some(e.to_string());
            }
        }
    } else {
        panic!("No engine loaded");
    }
}

#[when(regex = "^I update the (\\w+) with:$")]
fn update_resource(world: &mut AppWorld, model: String, step: &gherkin::Step) {
    if let Some(engine) = &mut world.engine {
        let json_str = step.docstring().expect("Expected JSON docstring");
        let input: Value = serde_json::from_str(json_str).expect("Failed to parse input JSON");

        world.last_model = Some(model.clone());

        match engine.call(&model, "update", &input) {
            Ok((data, errors)) => {
                world.last_operation_data = Some(data);
                world.last_operation_errors = errors;
                world.error = None;
            }
            Err(e) => {
                world.error = Some(e.to_string());
            }
        }
    } else {
        panic!("No engine loaded");
    }
}

#[when(regex = "^I delete the (\\w+) with id \"([^\"]+)\"$")]
fn delete_resource(world: &mut AppWorld, model: String, id: String) {
    if let Some(engine) = &mut world.engine {
        world.last_model = Some(model.clone());

        let input = json!({"id": id});

        match engine.call(&model, "delete", &input) {
            Ok((data, errors)) => {
                world.last_operation_data = Some(data);
                world.last_operation_errors = errors;
                world.error = None;
            }
            Err(e) => {
                world.error = Some(e.to_string());
            }
        }
    } else {
        panic!("No engine loaded");
    }
}

#[then("the operation succeeds")]
fn check_operation_succeeds(world: &mut AppWorld) {
    if let Some(ref error) = world.error {
        panic!("Expected success but got error: {}", error);
    }
    if let Some(ref errors) = world.last_operation_errors {
        assert!(
            errors.is_empty(),
            "Expected success but got errors: {:?}",
            errors
        );
    }
}

#[then("the operation fails with errorType")]
fn check_operation_fails_with_error_type(world: &mut AppWorld) {
    if world.error.is_none() && world.last_operation_errors.is_none() {
        panic!("Expected operation to fail but it succeeded");
    }
}

#[then(regex = "^the operation fails with errorType \"([^\"]+)\"$")]
fn check_operation_fails_with_specific_error(world: &mut AppWorld, error_type: String) {
    if let Some(ref errors) = world.last_operation_errors {
        let found = errors.iter().any(|e| {
            if let Some(et) = e.get("errorType").and_then(|v| v.as_str()) {
                et == error_type
            } else {
                false
            }
        });
        if !found {
            panic!("Expected error type '{}' but got: {:?}", error_type, errors);
        }
    } else {
        panic!(
            "Expected operation to fail with error type '{}' but got: {}",
            error_type,
            world.error.as_ref().unwrap_or(&"unknown".to_string())
        );
    }
}

#[then(regex = "^the operation fails with error containing \"([^\"]+)\"$")]
fn check_operation_fails_with_message(world: &mut AppWorld, substring: String) {
    if let Some(ref errors) = world.last_operation_errors {
        let found = errors.iter().any(|e| {
            if let Some(msg) = e.get("message").and_then(|v| v.as_str()) {
                msg.to_lowercase().contains(&substring.to_lowercase())
            } else {
                false
            }
        });
        if !found {
            panic!(
                "Expected error message containing '{}' but got: {:?}",
                substring, errors
            );
        }
    } else if let Some(ref err) = world.error {
        assert!(
            err.to_lowercase().contains(&substring.to_lowercase()),
            "Expected error containing '{}' but got: {}",
            substring,
            err
        );
    } else {
        panic!(
            "Expected operation to fail with message containing '{}' but it succeeded",
            substring
        );
    }
}

#[then("the result data has a uuid id")]
fn check_result_has_uuid_id(world: &mut AppWorld) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(id_str) = data.get("id").and_then(|v| v.as_str()) {
            // Try to parse as UUID
            match Uuid::parse_str(id_str) {
                Ok(_) => {} // Valid UUID
                Err(_) => panic!("Expected UUID but got: {}", id_str),
            }
        } else {
            panic!("Result data does not have id field");
        }
    } else {
        panic!("No operation result data");
    }
}

#[then(regex = "^the result data id is \"([^\"]+)\"$")]
fn check_result_id(world: &mut AppWorld, expected_id: String) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(id_str) = data.get("id").and_then(|v| v.as_str()) {
            assert_eq!(id_str, expected_id, "ID mismatch");
        } else {
            panic!("Result data does not have id field");
        }
    } else {
        panic!("No operation result data");
    }
}

#[then(regex = "^the result data has __typename \"([^\"]+)\"$")]
fn check_result_typename(world: &mut AppWorld, expected_type: String) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(type_str) = data.get("__typename").and_then(|v| v.as_str()) {
            assert_eq!(type_str, expected_type, "__typename mismatch");
        } else {
            panic!("Result data does not have __typename field");
        }
    } else {
        panic!("No operation result data");
    }
}

#[then("the result data has createdAt in ISO-8601 with milliseconds")]
fn check_result_created_at(world: &mut AppWorld) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(created_at) = data.get("createdAt").and_then(|v| v.as_str()) {
            // Check ISO-8601 format with milliseconds: YYYY-MM-DDTHH:MM:SS.sssZ
            let re = Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z?$")
                .expect("Failed to compile regex");
            assert!(
                re.is_match(created_at),
                "createdAt not in ISO-8601 with milliseconds format: {}",
                created_at
            );
        } else {
            panic!("Result data does not have createdAt field");
        }
    } else {
        panic!("No operation result data");
    }
}

#[then("the result data has updatedAt in ISO-8601 with milliseconds")]
fn check_result_updated_at(world: &mut AppWorld) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(updated_at) = data.get("updatedAt").and_then(|v| v.as_str()) {
            // Check ISO-8601 format with milliseconds: YYYY-MM-DDTHH:MM:SS.sssZ
            let re = Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z?$")
                .expect("Failed to compile regex");
            assert!(
                re.is_match(updated_at),
                "updatedAt not in ISO-8601 with milliseconds format: {}",
                updated_at
            );
        } else {
            panic!("Result data does not have updatedAt field");
        }
    } else {
        panic!("No operation result data");
    }
}

#[then(regex = "^the result\\.createdAt is \"([^\"]+)\"$")]
fn check_result_created_at_exact(world: &mut AppWorld, expected_value: String) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(created_at) = data.get("createdAt").and_then(|v| v.as_str()) {
            assert_eq!(created_at, expected_value, "createdAt mismatch");
        } else {
            panic!("Result data does not have createdAt field");
        }
    } else {
        panic!("No operation result data");
    }
}

#[then("the result.createdAt matches ISO-8601 timestamp")]
fn check_result_created_at_format(world: &mut AppWorld) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(created_at) = data.get("createdAt").and_then(|v| v.as_str()) {
            let re = Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
                .expect("Failed to compile regex");
            assert!(
                re.is_match(created_at),
                "createdAt not in ISO-8601 format: {}",
                created_at
            );
        } else {
            panic!("Result data does not have createdAt field");
        }
    } else {
        panic!("No operation result data");
    }
}

#[then("the result.updatedAt matches ISO-8601 timestamp")]
fn check_result_updated_at_format(world: &mut AppWorld) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(updated_at) = data.get("updatedAt").and_then(|v| v.as_str()) {
            let re = Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
                .expect("Failed to compile regex");
            assert!(
                re.is_match(updated_at),
                "updatedAt not in ISO-8601 format: {}",
                updated_at
            );
        } else {
            panic!("Result data does not have updatedAt field");
        }
    } else {
        panic!("No operation result data");
    }
}

#[then(regex = "^the result\\.updatedAt is \"([^\"]+)\"$")]
fn check_result_updated_at_exact(world: &mut AppWorld, expected_value: String) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(updated_at) = data.get("updatedAt").and_then(|v| v.as_str()) {
            assert_eq!(updated_at, expected_value, "updatedAt mismatch");
        } else {
            panic!("Result data does not have updatedAt field");
        }
    } else {
        panic!("No operation result data");
    }
}

#[then(regex = "^the result data status is \"([^\"]+)\"$")]
fn check_result_status(world: &mut AppWorld, expected_status: String) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(status) = data.get("status").and_then(|v| v.as_str()) {
            assert_eq!(status, expected_status, "status mismatch");
        } else {
            panic!("Result data does not have status field");
        }
    } else {
        panic!("No operation result data");
    }
}

#[then(regex = "^the result data postId is \"([^\"]+)\"$")]
fn check_result_postid(world: &mut AppWorld, expected: String) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(value) = data.get("postId").and_then(|v| v.as_str()) {
            assert_eq!(value, expected, "postId mismatch");
        } else {
            panic!("Result data does not have postId field or it's not a string");
        }
    } else {
        panic!("No operation result data");
    }
}

#[then(regex = "^the result data name is \"([^\"]+)\"$")]
fn check_result_name(world: &mut AppWorld, expected: String) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(value) = data.get("name").and_then(|v| v.as_str()) {
            assert_eq!(value, expected, "name mismatch");
        } else {
            panic!("Result data does not have name field or it's not a string");
        }
    } else {
        panic!("No operation result data");
    }
}

#[then(regex = "^the result data title is \"([^\"]+)\"$")]
fn check_result_title(world: &mut AppWorld, expected: String) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(value) = data.get("title").and_then(|v| v.as_str()) {
            assert_eq!(value, expected, "title mismatch");
        } else {
            panic!("Result data does not have title field or it's not a string");
        }
    } else {
        panic!("No operation result data");
    }
}

#[then(regex = "^the result data field \"([^\"]+)\" is \"([^\"]+)\"$")]
fn check_result_field(world: &mut AppWorld, field: String, expected: String) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(value) = data.get(&field).and_then(|v| v.as_str()) {
            assert_eq!(value, expected, "{} mismatch", field);
        } else {
            panic!(
                "Result data does not have {} field or it's not a string",
                field
            );
        }
    } else {
        panic!("No operation result data");
    }
}

#[then("the result data is null")]
fn check_result_data_null(world: &mut AppWorld) {
    if let Some(ref data) = world.last_operation_data {
        if !data.is_null() {
            panic!("Expected result data to be null but got: {:?}", data);
        }
    } else {
        panic!("No operation result data");
    }
}

#[then("there are no errors")]
fn check_no_errors(world: &mut AppWorld) {
    if let Some(ref errors) = world.last_operation_errors {
        if !errors.is_empty() {
            panic!("Expected no errors but got: {:?}", errors);
        }
    }
}

#[then("the result data status is null or missing")]
fn check_result_status_null_or_missing(world: &mut AppWorld) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(status) = data.get("status") {
            assert!(
                status.is_null(),
                "Expected status to be null but got: {:?}",
                status
            );
        }
        // If missing, that's fine too
    } else {
        panic!("No operation result data");
    }
}

#[then(regex = "^the result data has (.+) as \"([^\"]+)\" followed by ISO-8601$")]
fn check_result_composite_field(world: &mut AppWorld, field: String, prefix: String) {
    if let Some(ref data) = world.last_operation_data {
        if let Some(value) = data.get(&field).and_then(|v| v.as_str()) {
            assert!(
                value.starts_with(&prefix),
                "Expected {} to start with '{}' but got: {}",
                field,
                prefix,
                value
            );
            // Check the rest is ISO-8601 datetime
            let rest = &value[prefix.len()..];
            let re = Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z?$")
                .expect("Failed to compile regex");
            assert!(
                re.is_match(rest),
                "{} second part not in ISO-8601 format: {}",
                field,
                rest
            );
        } else {
            panic!("Result data does not have {} field", field);
        }
    } else {
        panic!("No operation result data");
    }
}

#[when(regex = "^I call operation \"([^\"]+)\"$")]
fn when_call_operation(world: &mut AppWorld, operation: String) {
    if let Some(engine) = &mut world.engine {
        // Try to call an unknown operation on a valid model - this should fail
        match engine.call("Blog", &operation, &json!({})) {
            Ok((data, errors)) => {
                world.last_operation_data = Some(data);
                world.last_operation_errors = errors;
                world.error = None;
            }
            Err(e) => {
                world.error = Some(e.to_string());
                world.last_operation_data = None;
                world.last_operation_errors = Some(vec![json!({"message": e.to_string()})]);
            }
        }
    } else {
        panic!("No engine loaded");
    }
}

#[when(
    regex = "^I get the Vote with keys postId \"([^\"]+)\" and userId \"([^\"]+)\" and kind \"([^\"]+)\"$"
)]
fn when_get_vote(world: &mut AppWorld, post_id: String, user_id: String, kind: String) {
    if let Some(engine) = &mut world.engine {
        let pk = json!({
            "postId": post_id,
            "userId": user_id,
            "kind": kind
        });

        match engine.call("Vote", "get", &pk) {
            Ok((data, errors)) => {
                world.last_operation_data = Some(data);
                world.last_operation_errors = errors;
                world.error = None;
            }
            Err(e) => {
                world.error = Some(e.to_string());
            }
        }
    } else {
        panic!("No engine loaded");
    }
}

#[then("the operation fails")]
fn check_operation_fails(world: &mut AppWorld) {
    assert!(
        world.error.is_some() || world.last_operation_errors.is_some(),
        "Expected operation to fail"
    );
}

#[when("I open the engine with a temporary directory")]
fn open_engine_with_temp_dir(world: &mut AppWorld) {
    if let Some(contract) = world.contract.clone() {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let dir_path = temp_dir.path().to_path_buf();
        world.directory = Some(dir_path.clone());

        // Keep the temp directory alive by storing it in a static or returning it
        // For now, we'll leak it to keep it alive for the test
        let _ = Box::leak(Box::new(temp_dir));

        match Engine::open(Some(dir_path), contract, EngineOptions) {
            Ok(engine) => {
                world.engine = Some(engine);
                world.error = None;
            }
            Err(e) => {
                world.error = Some(e.to_string());
            }
        }
    }
}

#[when("I close and reopen the engine")]
fn reopen_engine(world: &mut AppWorld) {
    // Drop the current engine to ensure everything is flushed
    world.engine = None;

    if let Some(contract) = world.contract.clone() {
        if let Some(dir) = &world.directory {
            match Engine::open(Some(dir.clone()), contract, EngineOptions) {
                Ok(engine) => {
                    world.engine = Some(engine);
                    world.error = None;
                }
                Err(e) => {
                    world.error = Some(e.to_string());
                }
            }
        } else {
            world.error = Some("No directory set for reopening".to_string());
        }
    }
}

#[then("the storage directory has per-model folders")]
fn check_model_folders(world: &mut AppWorld) {
    if let Some(dir) = &world.directory {
        let blog_path = dir.join("Blog");
        let post_path = dir.join("Post");
        let vote_path = dir.join("Vote");

        assert!(
            blog_path.exists(),
            "Blog folder should exist at {:?}",
            blog_path
        );
        assert!(
            post_path.exists(),
            "Post folder should exist at {:?}",
            post_path
        );
        assert!(
            vote_path.exists(),
            "Vote folder should exist at {:?}",
            vote_path
        );
    } else {
        panic!("No directory set");
    }
}

#[given(regex = "^these (\\w+) records exist:$")]
fn given_records_exist(world: &mut AppWorld, model: String, step: &gherkin::Step) {
    if let Some(engine) = &mut world.engine {
        if let Some(content) = &step.docstring {
            if let Ok(records) = serde_json::from_str::<Vec<Value>>(content) {
                for record in records {
                    if let Err(e) = engine.call(&model, "create", &record) {
                        world.error = Some(e.to_string());
                        panic!("Failed to create record: {}", e);
                    }
                }
            } else {
                panic!("Invalid JSON array in step");
            }
        }
    } else {
        panic!("No engine loaded");
    }
}

#[when(regex = "^I list all (\\w+) with:$")]
fn when_list_with(world: &mut AppWorld, model: String, step: &gherkin::Step) {
    if let Some(engine) = &mut world.engine {
        if let Some(content) = &step.docstring {
            if let Ok(args) = serde_json::from_str::<Value>(content) {
                match engine.call(&model, "list", &args) {
                    Ok((data, errors)) => {
                        world.last_operation_data = Some(data);
                        world.last_operation_errors = errors;
                        world.error = None;
                    }
                    Err(e) => {
                        world.error = Some(e.to_string());
                    }
                }
            } else {
                world.error = Some("Invalid JSON in step".to_string());
            }
        }
    } else {
        panic!("No engine loaded");
    }
}

#[when(regex = "^I query (\\w+) by (\\w+) with:$")]
fn when_query_index(world: &mut AppWorld, model: String, index: String, step: &gherkin::Step) {
    if let Some(engine) = &mut world.engine {
        if let Some(content) = &step.docstring {
            if let Ok(args) = serde_json::from_str::<Value>(content) {
                match engine.call(&model, &index, &args) {
                    Ok((data, errors)) => {
                        world.last_operation_data = Some(data);
                        world.last_operation_errors = errors;
                        world.error = None;
                    }
                    Err(e) => {
                        world.error = Some(e.to_string());
                    }
                }
            } else {
                world.error = Some("Invalid JSON in step".to_string());
            }
        }
    } else {
        panic!("No engine loaded");
    }
}

#[then(regex = "^the result data has exactly (\\d+) record with id \"([^\"]+)\"$")]
fn then_result_data_has_one_record(world: &mut AppWorld, count: usize, id: String) {
    assert_eq!(count, 1, "Count must be 1 for single record check");
    if let Some(records) = world.last_operation_data.as_ref().and_then(get_items_array) {
        assert_eq!(records.len(), 1, "Expected exactly 1 record");
        if let Some(Value::Object(record)) = records.first() {
            if let Some(Value::String(record_id)) = record.get("id") {
                assert_eq!(record_id, &id, "Record id should match");
            } else {
                panic!("Record has no id field");
            }
        } else {
            panic!("Expected record to be an object");
        }
    } else {
        panic!("Expected result data to be an array of records");
    }
}

#[then(regex = "^the result data contains exactly these ids \\[([^\\]]+)\\]$")]
fn then_result_data_contains_ids(world: &mut AppWorld, ids_str: String) {
    let expected_ids: Vec<&str> = ids_str
        .split(',')
        .map(|s| s.trim().trim_matches('"'))
        .collect();
    if let Some(records) = world.last_operation_data.as_ref().and_then(get_items_array) {
        let actual_ids: Vec<String> = records
            .iter()
            .filter_map(|r| {
                if let Value::Object(obj) = r {
                    obj.get("id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(
            actual_ids.len(),
            expected_ids.len(),
            "Expected {} ids but got {}",
            expected_ids.len(),
            actual_ids.len()
        );

        for expected_id in &expected_ids {
            assert!(
                actual_ids.contains(&expected_id.to_string()),
                "Expected to find id {} in results",
                expected_id
            );
        }
    } else {
        panic!("Expected result data to be an array of records");
    }
}

#[then(regex = "^the result data contains exactly these ids \\[\\]$")]
fn then_result_data_empty(world: &mut AppWorld) {
    if let Some(records) = world.last_operation_data.as_ref().and_then(get_items_array) {
        assert_eq!(
            records.len(),
            0,
            "Expected 0 records but got {}",
            records.len()
        );
    } else {
        panic!("Expected result data to be an array of records");
    }
}

#[then("the operation has validation error")]
fn then_operation_has_validation_error(world: &mut AppWorld) {
    if let Some(errors) = &world.last_operation_errors {
        if errors.is_empty() {
            panic!("Expected operation to have validation error but got none");
        }
        for error in errors {
            if let Some(error_type) = error.get("errorType") {
                if error_type == "ValidationException" {
                    return;
                }
            }
        }
        panic!("Expected ValidationException error but got: {:?}", errors);
    } else {
        panic!("Expected operation to have validation error but got none");
    }
}

#[then(regex = "^the result has exactly (\\d+) item with id \"([^\"]+)\"$")]
fn then_result_has_item(world: &mut AppWorld, count: usize, id: String) {
    if let Some(records) = world.last_operation_data.as_ref().and_then(get_items_array) {
        assert_eq!(
            records.len(),
            count,
            "Expected {} items but got {}",
            count,
            records.len()
        );
        if count > 0 {
            let ids: Vec<String> = records
                .iter()
                .filter_map(|r| {
                    if let Value::Object(obj) = r {
                        obj.get("id")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                    } else {
                        None
                    }
                })
                .collect();
            assert!(ids.contains(&id), "Expected to find id {}", id);
        }
    } else {
        panic!("Expected result data to be available");
    }
}

#[then(regex = "^the result has exactly (\\d+) items?$")]
fn then_result_has_count(world: &mut AppWorld, count: usize) {
    if let Some(records) = world.last_operation_data.as_ref().and_then(get_items_array) {
        assert_eq!(
            records.len(),
            count,
            "Expected {} items but got {}",
            count,
            records.len()
        );
    } else {
        panic!("Expected result data to be available");
    }
}

#[then(regex = "^the result contains exactly these ids \\[([^\\]]+)\\]$")]
fn then_result_contains_ids(world: &mut AppWorld, ids_str: String) {
    let expected_ids: Vec<&str> = ids_str
        .split(',')
        .map(|s| s.trim().trim_matches('"'))
        .collect();
    if let Some(records) = world.last_operation_data.as_ref().and_then(get_items_array) {
        let actual_ids: Vec<String> = records
            .iter()
            .filter_map(|r| {
                if let Value::Object(obj) = r {
                    obj.get("id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(
            actual_ids.len(),
            expected_ids.len(),
            "Expected {} ids but got {}",
            expected_ids.len(),
            actual_ids.len()
        );
        for (i, expected_id) in expected_ids.iter().enumerate() {
            assert_eq!(
                actual_ids[i], *expected_id,
                "Expected id {} at position {}",
                expected_id, i
            );
        }
    } else {
        panic!("Expected result data to be available");
    }
}

#[then(regex = "^the result contains exactly these ids \\[\\]$")]
fn then_result_contains_empty_ids(world: &mut AppWorld) {
    if let Some(records) = world.last_operation_data.as_ref().and_then(get_items_array) {
        assert_eq!(
            records.len(),
            0,
            "Expected 0 items but got {}",
            records.len()
        );
    } else {
        panic!("Expected result data to be available");
    }
}

#[then("the nextToken is null")]
fn then_next_token_is_null(world: &mut AppWorld) {
    if let Some(token) = world.last_operation_data.as_ref().and_then(get_next_token) {
        assert!(token.is_none(), "Expected nextToken to be null");
    } else {
        panic!("Expected result to have nextToken field");
    }
}

#[then("the nextToken is not null")]
fn then_next_token_is_not_null(world: &mut AppWorld) {
    if let Some(token) = world.last_operation_data.as_ref().and_then(get_next_token) {
        assert!(token.is_some(), "Expected nextToken to be non-null");
        world.last_next_token = token;
    } else {
        panic!("Expected result to have nextToken field");
    }
}

#[when(regex = "^I list all ([A-Za-z]+) with the previous nextToken:?$")]
fn when_list_with_previous_token(world: &mut AppWorld, entity_type: String, step: &gherkin::Step) {
    if let Some(token) = &world.last_next_token {
        if let Some(engine) = &mut world.engine {
            // Get parameters from docstring if provided, otherwise use just the token
            let mut params = if let Some(content) = &step.docstring {
                serde_json::from_str::<Value>(content).expect("Failed to parse parameters")
            } else {
                json!({})
            };

            // Always add/override the nextToken
            params["nextToken"] = Value::String(token.clone());

            match engine.call(&entity_type, "list", &params) {
                Ok((data, errors)) => {
                    world.last_operation_data = Some(data);
                    world.last_operation_errors = errors;
                    world.error = None;
                }
                Err(e) => {
                    world.error = Some(e.to_string());
                }
            }
        } else {
            panic!("No engine loaded");
        }
    } else {
        panic!("No previous nextToken available");
    }
}

#[when(regex = "^I collect all pages of ([A-Za-z]+) with:$")]
fn when_collect_all_pages(world: &mut AppWorld, entity_type: String, step: &gherkin::Step) {
    if let Some(engine) = &mut world.engine {
        if let Some(content) = &step.docstring {
            let params: Value = serde_json::from_str(content).expect("Failed to parse parameters");

            // Collect all items across pages
            let mut all_items = Vec::new();
            let mut next_token: Option<String> = None;
            let mut seen_tokens = std::collections::HashSet::new();

            for page in 0.. {
                assert!(
                    page < 1000,
                    "collecting pages did not terminate after 1000 pages"
                );
                let mut current_params = params.clone();
                if let Some(token) = next_token {
                    current_params["nextToken"] = Value::String(token);
                }

                match engine.call(&entity_type, "list", &current_params) {
                    Ok((data, errors)) => {
                        world.last_operation_data = Some(data.clone());
                        world.last_operation_errors = errors;
                        world.error = None;

                        if let Some(records) = get_items_array(&data) {
                            all_items.extend(records);
                        }

                        match get_next_token(&data).flatten() {
                            Some(token) => {
                                assert!(
                                    seen_tokens.insert(token.clone()),
                                    "nextToken repeated; pagination is not advancing: {token}"
                                );
                                next_token = Some(token);
                            }
                            None => break,
                        }
                    }
                    Err(e) => {
                        world.error = Some(e.to_string());
                        break;
                    }
                }
            }

            // Store the collected items as the result
            world.last_operation_data = Some(Value::Array(all_items));
        } else {
            panic!("No docstring in step");
        }
    } else {
        panic!("No engine loaded");
    }
}

#[when(regex = "^I query ([A-Za-z]+) by ([A-Za-z]+) with the previous nextToken:?$")]
fn when_query_with_previous_token(
    world: &mut AppWorld,
    entity_type: String,
    index_name: String,
    step: &gherkin::Step,
) {
    if let Some(token) = &world.last_next_token {
        if let Some(engine) = &mut world.engine {
            // Get parameters from docstring if provided
            let mut params = if let Some(content) = &step.docstring {
                serde_json::from_str::<Value>(content).expect("Failed to parse parameters")
            } else {
                json!({})
            };

            // Always add/override the nextToken
            params["nextToken"] = Value::String(token.clone());

            match engine.call(&entity_type, index_name.as_str(), &params) {
                Ok((data, errors)) => {
                    world.last_operation_data = Some(data);
                    world.last_operation_errors = errors;
                    world.error = None;
                }
                Err(e) => {
                    world.error = Some(e.to_string());
                }
            }
        } else {
            panic!("No engine loaded");
        }
    } else {
        panic!("No previous nextToken available");
    }
}

/// Extract items array from paginated or direct response
fn get_items_array(data: &Value) -> Option<Vec<Value>> {
    match data {
        Value::Object(obj) => {
            // Paginated response: {"items": [...], "nextToken": ...}
            obj.get("items").and_then(|v| v.as_array()).cloned()
        }
        Value::Array(arr) => {
            // Direct array response (for backward compatibility)
            Some(arr.clone())
        }
        _ => None,
    }
}

/// Extract nextToken from paginated response
fn get_next_token(data: &Value) -> Option<Option<String>> {
    match data {
        Value::Object(obj) => Some(
            obj.get("nextToken")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        ),
        _ => None,
    }
}

#[when(regex = "^I get the (\\w+) with id \"([^\"]+)\" and selectionSet:$")]
fn get_with_selection_set(world: &mut AppWorld, model: String, id: String, step: &gherkin::Step) {
    if let Some(engine) = &mut world.engine {
        world.last_model = Some(model.clone());

        let selection_json = step.docstring().expect("Expected JSON docstring");
        let selection_set: Vec<String> =
            serde_json::from_str(selection_json).expect("Failed to parse selectionSet JSON");

        world.selection_set = Some(selection_set.clone());

        let mut input = json!({"id": id});
        if let Some(args) = &world.relationship_args {
            input["relationshipArgs"] = args.clone();
        }
        input["selectionSet"] = json!(selection_set);

        match engine.call(&model, "get", &input) {
            Ok((data, errors)) => {
                world.last_operation_data = Some(data);
                world.last_operation_errors = errors;
                world.error = None;
            }
            Err(e) => {
                world.error = Some(e.to_string());
            }
        }
    } else {
        panic!("No engine loaded");
    }
}

#[when(regex = "^I get the (\\w+) with id \"([^\"]+)\" and selectionSet and relationshipArgs:$")]
fn get_with_selection_and_relationship_args(
    world: &mut AppWorld,
    model: String,
    id: String,
    step: &gherkin::Step,
) {
    if let Some(engine) = &mut world.engine {
        world.last_model = Some(model.clone());

        let input_json = step.docstring().expect("Expected JSON docstring");
        let input_obj: Value =
            serde_json::from_str(input_json).expect("Failed to parse input JSON");

        let selection_set: Vec<String> = input_obj["selectionSet"]
            .as_array()
            .expect("selectionSet must be an array")
            .iter()
            .map(|v| {
                v.as_str()
                    .expect("selectionSet items must be strings")
                    .to_string()
            })
            .collect();

        let relationship_args = input_obj.get("relationshipArgs").cloned();

        world.selection_set = Some(selection_set.clone());
        world.relationship_args = relationship_args.clone();

        let mut input = json!({"id": id});
        if let Some(args) = &relationship_args {
            input["relationshipArgs"] = args.clone();
        }
        input["selectionSet"] = json!(selection_set);

        match engine.call(&model, "get", &input) {
            Ok((data, errors)) => {
                world.last_operation_data = Some(data);
                world.last_operation_errors = errors;
                world.error = None;
            }
            Err(e) => {
                world.error = Some(e.to_string());
            }
        }
    } else {
        panic!("No engine loaded");
    }
}

#[when(
    regex = "^I get the (\\w+) with id \"([^\"]+)\" and selectionSet and relationshipArgs with stored nextToken:$"
)]
fn get_with_stored_next_token(
    world: &mut AppWorld,
    model: String,
    id: String,
    step: &gherkin::Step,
) {
    if let Some(engine) = &mut world.engine {
        world.last_model = Some(model.clone());

        let input_json = step.docstring().expect("Expected JSON docstring");
        let mut input_obj: Value =
            serde_json::from_str(input_json).expect("Failed to parse input JSON");

        let selection_set: Vec<String> = input_obj["selectionSet"]
            .as_array()
            .expect("selectionSet must be an array")
            .iter()
            .map(|v| {
                v.as_str()
                    .expect("selectionSet items must be strings")
                    .to_string()
            })
            .collect();

        // Add stored nextToken to relationshipArgs
        if let Some(stored_token) = world.stored_next_tokens.get("comments") {
            if let Some(rel_args) = input_obj.get_mut("relationshipArgs") {
                if let Some(comments_args) = rel_args.get_mut("comments") {
                    if let Some(obj) = comments_args.as_object_mut() {
                        obj.insert("nextToken".to_string(), json!(stored_token));
                    }
                }
            }
        }

        let relationship_args = input_obj.get("relationshipArgs").cloned();

        world.selection_set = Some(selection_set.clone());
        world.relationship_args = relationship_args.clone();

        let mut input = json!({"id": id});
        if let Some(args) = &relationship_args {
            input["relationshipArgs"] = args.clone();
        }
        input["selectionSet"] = json!(selection_set);

        match engine.call(&model, "get", &input) {
            Ok((data, errors)) => {
                world.last_operation_data = Some(data);
                world.last_operation_errors = errors;
                world.error = None;
            }
            Err(e) => {
                world.error = Some(e.to_string());
            }
        }
    } else {
        panic!("No engine loaded");
    }
}

#[then("I store the nextToken for comments")]
fn store_next_token_for_comments(world: &mut AppWorld) {
    if let Some(Value::Object(data)) = &world.last_operation_data {
        if let Some(Value::Object(comments)) = data.get("comments") {
            if let Some(Value::String(token)) = comments.get("nextToken") {
                world
                    .stored_next_tokens
                    .insert("comments".to_string(), token.clone());
            }
        }
    }
}

#[then(regex = "^the result\\.([\\w.]+) has exactly (\\d+) record(?:s)?$")]
fn assert_has_exactly_n_records(world: &mut AppWorld, path: String, count: usize) {
    if let Some(Value::Object(data)) = &world.last_operation_data {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = Value::Object(data.clone());

        for part in parts {
            if let Some(next) = current.get(part) {
                current = next.clone();
            } else {
                panic!("Path {} not found in result", path);
            }
        }

        if let Some(arr) = current.as_array() {
            assert_eq!(
                arr.len(),
                count,
                "Expected {} records, got {}",
                count,
                arr.len()
            );
        } else {
            panic!("Expected array at path {}", path);
        }
    } else {
        panic!("No result data");
    }
}

#[then(regex = "^the result\\.([\\w.]+) has exactly (\\d+) records? with ids \\[(.*)\\]$")]
fn assert_records_with_ids(world: &mut AppWorld, path: String, _count: usize, ids_str: String) {
    if let Some(Value::Object(data)) = &world.last_operation_data {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = Value::Object(data.clone());

        for part in parts {
            if let Some(next) = current.get(part) {
                current = next.clone();
            } else {
                panic!("Path {} not found in result", path);
            }
        }

        if let Some(arr) = current.as_array() {
            let expected_ids: Vec<&str> = ids_str
                .split(',')
                .map(|s| s.trim().trim_matches('"'))
                .collect();
            let actual_ids: Vec<String> = arr
                .iter()
                .filter_map(|v| {
                    v.get("id")
                        .and_then(|id| id.as_str())
                        .map(|s| s.to_string())
                })
                .collect();

            assert_eq!(actual_ids, expected_ids, "IDs don't match");
        } else {
            panic!("Expected array at path {}", path);
        }
    } else {
        panic!("No result data");
    }
}

#[then(regex = "^the result has field \"([^\"]+)\"$")]
fn assert_result_has_field(world: &mut AppWorld, field: String) {
    if let Some(Value::Object(data)) = &world.last_operation_data {
        assert!(
            data.contains_key(&field),
            "Field '{}' not found in result",
            field
        );
    } else {
        panic!("No result data");
    }
}

#[then(regex = "^the result does not have field \"([^\"]+)\"$")]
fn assert_result_does_not_have_field(world: &mut AppWorld, field: String) {
    if let Some(Value::Object(data)) = &world.last_operation_data {
        assert!(
            !data.contains_key(&field),
            "Field '{}' should not be in result",
            field
        );
    } else {
        panic!("No result data");
    }
}

#[then(regex = "^the result\\.([\\w.]+) is null$")]
fn assert_field_is_null(world: &mut AppWorld, path: String) {
    if let Some(Value::Object(data)) = &world.last_operation_data {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = Value::Object(data.clone());

        for part in parts {
            if let Some(next) = current.get(part) {
                current = next.clone();
            } else {
                panic!("Path {} not found in result", path);
            }
        }

        assert!(
            current.is_null(),
            "Expected null at path {}, got {:?}",
            path,
            current
        );
    } else {
        panic!("No result data");
    }
}

#[then(regex = "^the result\\.([\\w.]+) is not null$")]
fn assert_field_is_not_null(world: &mut AppWorld, path: String) {
    if let Some(Value::Object(data)) = &world.last_operation_data {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = Value::Object(data.clone());

        for part in parts {
            if let Some(next) = current.get(part) {
                current = next.clone();
            } else {
                panic!("Path {} not found in result", path);
            }
        }

        assert!(!current.is_null(), "Expected non-null at path {}", path);
    } else {
        panic!("No result data");
    }
}

#[then("the result has:")]
fn assert_result_has(world: &mut AppWorld, step: &gherkin::Step) {
    if let Some(Value::Object(data)) = &world.last_operation_data {
        let expected_str = step.docstring().expect("Expected JSON docstring");
        let expected: Value =
            serde_json::from_str(expected_str).expect("Failed to parse expected JSON");

        if let Value::Object(expected_obj) = expected {
            for (key, expected_val) in expected_obj {
                let actual_val = data.get(&key).cloned().unwrap_or(Value::Null);
                assert_eq!(
                    actual_val, expected_val,
                    "Field '{}' mismatch: expected {:?}, got {:?}",
                    key, expected_val, actual_val
                );
            }
        } else {
            panic!("Expected JSON object in docstring");
        }
    } else {
        panic!("No result data");
    }
}

#[then(regex = "^the result\\.([\\w.]+) has field \"([^\"]+)\"$")]
fn assert_nested_has_field(world: &mut AppWorld, path: String, field: String) {
    if let Some(Value::Object(data)) = &world.last_operation_data {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = Value::Object(data.clone());

        for part in parts {
            if let Some(next) = current.get(part) {
                current = next.clone();
            } else {
                panic!("Path {} not found in result", path);
            }
        }

        if let Value::Object(obj) = current {
            assert!(
                obj.contains_key(&field),
                "Field '{}' not found at path {}",
                field,
                path
            );
        } else {
            panic!("Expected object at path {}", path);
        }
    } else {
        panic!("No result data");
    }
}

#[tokio::main]
async fn main() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let features_path = format!("{}/../../../features/amplify", manifest_dir);
    AppWorld::run(&features_path).await;
}
