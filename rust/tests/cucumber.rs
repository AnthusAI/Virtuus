use std::path::PathBuf;
use std::process::Command;

use cucumber::{given, then, when, World};
use serde_json::Value;
use virtuus::sort::SortCondition;

#[derive(Debug, Default, World)]
pub struct VirtuusWorld {
    pub version_file: Option<PathBuf>,
    pub python_version: Option<String>,
    pub rust_version: Option<String>,
    pub predicate: Option<SortCondition>,
    pub predicate_result: Option<bool>,
}

// ---------------------------------------------------------------------------
// Scenario: Library loads and reports version
// ---------------------------------------------------------------------------

#[given("the virtuus library is available")]
async fn given_library_available(_world: &mut VirtuusWorld) {
    // The Rust library is always available since we compiled it.
}

#[then("it should report a valid version string")]
async fn then_valid_version(_world: &mut VirtuusWorld) {
    let v = virtuus::version();
    assert!(!v.is_empty(), "version() returned an empty string");
}

// ---------------------------------------------------------------------------
// Scenario: Version is read from the shared VERSION file
// ---------------------------------------------------------------------------

#[given("a VERSION file at the repository root")]
async fn given_version_file(world: &mut VirtuusWorld) {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let version_file = manifest_dir.parent().unwrap().join("VERSION");
    assert!(
        version_file.exists(),
        "VERSION file not found at {}",
        version_file.display()
    );
    world.version_file = Some(version_file);
}

#[then("the library version should match the contents of that file")]
async fn then_version_matches_file(world: &mut VirtuusWorld) {
    let path = world.version_file.as_ref().unwrap();
    let expected = std::fs::read_to_string(path).unwrap().trim().to_string();
    let actual = virtuus::VERSION;
    assert_eq!(
        actual, expected,
        "Library VERSION {actual:?} != VERSION file {expected:?}"
    );
}

// ---------------------------------------------------------------------------
// Scenario: Python and Rust report the same version
// ---------------------------------------------------------------------------

#[given("the Python virtuus library is available")]
async fn given_python_library(world: &mut VirtuusWorld) {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let python_src = manifest_dir.parent().unwrap().join("python").join("src");
    // {:?} gives a Rust debug-quoted string (e.g. "/path/to/src"), which is
    // a valid Python string literal.
    let src = python_src.to_str().unwrap();
    let script = format!(
        "import sys; sys.path.insert(0, {src:?}); import virtuus; print(virtuus.__version__)"
    );
    let output = Command::new("python3")
        .args(["-c", &script])
        .output()
        .expect("Failed to run python3 — is it on PATH?");
    assert!(
        output.status.success(),
        "Python command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    world.python_version = Some(String::from_utf8_lossy(&output.stdout).trim().to_string());
}

#[given("the Rust virtuus binary is available")]
async fn given_rust_binary(world: &mut VirtuusWorld) {
    // Use the library version directly (the binary reports the same value).
    world.rust_version = Some(virtuus::VERSION.to_string());
}

#[then("both should report the same version string")]
async fn then_same_version(world: &mut VirtuusWorld) {
    let py = world.python_version.as_deref().unwrap_or("");
    let rs = world.rust_version.as_deref().unwrap_or("");
    assert_eq!(py, rs, "Python version {py:?} != Rust version {rs:?}");
}

// ---------------------------------------------------------------------------
// Sort condition helpers
// ---------------------------------------------------------------------------

/// Parse a step table value string into a serde_json Value, coercing numbers.
fn parse_value(s: &str) -> Value {
    if let Ok(n) = s.parse::<i64>() {
        return Value::Number(n.into());
    }
    if let Ok(f) = s.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return Value::Number(n);
        }
    }
    Value::String(s.to_string())
}

// ---------------------------------------------------------------------------
// Sort condition steps
// ---------------------------------------------------------------------------

#[given(
    regex = r#"^a sort condition of "(eq|ne|lt|lte|gt|gte|begins_with|contains)" with value "([^"]*)"$"#
)]
async fn given_sort_condition_single(world: &mut VirtuusWorld, op: String, value: String) {
    let v = parse_value(&value);
    world.predicate = Some(match op.as_str() {
        "eq" => SortCondition::Eq(v),
        "ne" => SortCondition::Ne(v),
        "lt" => SortCondition::Lt(v),
        "lte" => SortCondition::Lte(v),
        "gt" => SortCondition::Gt(v),
        "gte" => SortCondition::Gte(v),
        "begins_with" => SortCondition::BeginsWith(value),
        "contains" => SortCondition::Contains(value),
        _ => panic!("Unknown sort operator: {op}"),
    });
}

#[given(regex = r#"^a sort condition of "between" with low "([^"]*)" and high "([^"]*)"$"#)]
async fn given_sort_condition_between(world: &mut VirtuusWorld, low: String, high: String) {
    world.predicate = Some(SortCondition::Between(
        parse_value(&low),
        parse_value(&high),
    ));
}

#[when(regex = r#"^evaluated against "([^"]*)"$"#)]
async fn when_evaluated_against(world: &mut VirtuusWorld, input: String) {
    let v = parse_value(&input);
    let result = world.predicate.as_ref().unwrap().evaluate(&v);
    world.predicate_result = Some(result);
}

#[when("evaluated against a null value")]
async fn when_evaluated_against_null(world: &mut VirtuusWorld) {
    let result = world.predicate.as_ref().unwrap().evaluate(&Value::Null);
    world.predicate_result = Some(result);
}

#[then("the result should be true")]
async fn then_result_true(world: &mut VirtuusWorld) {
    assert_eq!(
        world.predicate_result,
        Some(true),
        "Expected true but got {:?}",
        world.predicate_result
    );
}

#[then("the result should be false")]
async fn then_result_false(world: &mut VirtuusWorld) {
    assert_eq!(
        world.predicate_result,
        Some(false),
        "Expected false but got {:?}",
        world.predicate_result
    );
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    VirtuusWorld::run("../features").await;
}
