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

#[given("an apricitus contract")]
fn load_apricitus_contract(world: &mut AppWorld) {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = format!(
        "{}/../../../features/amplify/fixtures/apricitus.contract.json",
        manifest_dir
    );
    let json = std::fs::read_to_string(&path).expect("Failed to read apricitus fixture");

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
        match Engine::open(None, contract, EngineOptions::default()) {
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

#[tokio::main]
async fn main() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let features_path = format!(
        "{}/../../../features/amplify/contract.feature",
        manifest_dir
    );
    AppWorld::run(&features_path).await;
}
