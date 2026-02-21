use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;

#[allow(unused_imports)]
use cucumber::step;
use cucumber::{given, then, when, World};
use serde_json::Value;
use virtuus::gsi::Gsi;
use virtuus::sort::SortCondition;

#[derive(Debug, Default, World)]
pub struct VirtuusWorld {
    pub version_file: Option<PathBuf>,
    pub python_version: Option<String>,
    pub rust_version: Option<String>,
    pub predicate: Option<SortCondition>,
    pub predicate_result: Option<bool>,
    pub gsis: HashMap<String, Gsi>,
    pub current_gsi_name: Option<String>,
    pub last_result: Vec<String>,
    pub last_update: Option<LastUpdate>,
}

#[derive(Debug, Clone)]
pub struct LastUpdate {
    pub pk: String,
    pub old_created_at: String,
    pub new_created_at: String,
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

fn table_to_records(table: &cucumber::gherkin::Table) -> Vec<HashMap<String, Value>> {
    if table.rows.is_empty() {
        return Vec::new();
    }
    let headers = &table.rows[0];
    let mut records = Vec::new();
    for row in table.rows.iter().skip(1) {
        let mut record: HashMap<String, Value> = HashMap::new();
        for (header, cell) in headers.iter().zip(row.iter()) {
            record.insert(header.clone(), parse_value(cell));
        }
        records.push(record);
    }
    records
}

fn ensure_gsis(world: &mut VirtuusWorld) -> &mut HashMap<String, Gsi> {
    &mut world.gsis
}

fn set_current_gsi(world: &mut VirtuusWorld, name: &str) {
    world.current_gsi_name = Some(name.to_string());
}

fn current_gsi(world: &VirtuusWorld) -> &Gsi {
    let name = world.current_gsi_name.as_ref().expect("No current GSI");
    world.gsis.get(name).expect("GSI not found")
}

fn current_gsi_mut(world: &mut VirtuusWorld) -> &mut Gsi {
    let name = world.current_gsi_name.as_ref().expect("No current GSI");
    world.gsis.get_mut(name).expect("GSI not found")
}

fn infer_partition_key(name: &str) -> String {
    if let Some(stripped) = name.strip_prefix("by_") {
        return stripped.to_string();
    }
    name.to_string()
}

fn record_to_value(record: &HashMap<String, Value>) -> Value {
    let mut map = serde_json::Map::new();
    for (k, v) in record {
        map.insert(k.clone(), v.clone());
    }
    Value::Object(map)
}

fn record_pk(record: &HashMap<String, Value>) -> String {
    match record.get("pk") {
        Some(Value::String(s)) => s.clone(),
        Some(value) => value.to_string(),
        None => panic!("record missing pk"),
    }
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
// GSI steps
// ---------------------------------------------------------------------------

#[given(regex = r#"^a GSI named "([^"]*)" with partition key "([^"]*)"$"#)]
async fn given_gsi_named_hash_only(world: &mut VirtuusWorld, name: String, partition_key: String) {
    let gsi = Gsi::new(&name, &partition_key, None);
    ensure_gsis(world).insert(name.clone(), gsi);
    set_current_gsi(world, &name);
}

#[given(regex = r#"^a GSI named "([^"]*)" with partition key "([^"]*)" and sort key "([^"]*)"$"#)]
async fn given_gsi_named_hash_range(
    world: &mut VirtuusWorld,
    name: String,
    partition_key: String,
    sort_key: String,
) {
    let gsi = Gsi::new(&name, &partition_key, Some(&sort_key));
    ensure_gsis(world).insert(name.clone(), gsi);
    set_current_gsi(world, &name);
}

#[given(regex = r#"^a hash-only GSI "([^"]*)" with partition key "([^"]*)"$"#)]
async fn given_hash_only_gsi(world: &mut VirtuusWorld, name: String, partition_key: String) {
    let gsi = Gsi::new(&name, &partition_key, None);
    ensure_gsis(world).insert(name.clone(), gsi);
    set_current_gsi(world, &name);
}

#[given(
    regex = r#"^a hash\+range GSI "([^"]*)" with partition key "([^"]*)" and sort key "([^"]*)"$"#
)]
async fn given_hash_range_gsi(
    world: &mut VirtuusWorld,
    name: String,
    partition_key: String,
    sort_key: String,
) {
    let gsi = Gsi::new(&name, &partition_key, Some(&sort_key));
    ensure_gsis(world).insert(name.clone(), gsi);
    set_current_gsi(world, &name);
}

#[given(regex = r#"^a hash-only GSI "([^"]*)" populated with:$"#)]
async fn given_hash_only_populated(
    world: &mut VirtuusWorld,
    name: String,
    #[step] step: &cucumber::gherkin::Step,
) {
    let partition_key = infer_partition_key(&name);
    let gsi = Gsi::new(&name, &partition_key, None);
    let table = step.table().expect("Missing table");
    let records = table_to_records(table);
    let mut gsi = gsi;
    for record in records {
        let pk = record_pk(&record);
        gsi.put(&pk, &record_to_value(&record));
    }
    ensure_gsis(world).insert(name.clone(), gsi);
    set_current_gsi(world, &name);
}

#[given(
    regex = r#"^a hash\+range GSI "([^"]*)" with partition key "([^"]*)" and sort key "([^"]*)" populated with:$"#
)]
async fn given_hash_range_populated(
    world: &mut VirtuusWorld,
    name: String,
    partition_key: String,
    sort_key: String,
    #[step] step: &cucumber::gherkin::Step,
) {
    let mut gsi = Gsi::new(&name, &partition_key, Some(&sort_key));
    let table = step.table().expect("Missing table");
    let records = table_to_records(table);
    for record in records {
        let pk = record_pk(&record);
        gsi.put(&pk, &record_to_value(&record));
    }
    ensure_gsis(world).insert(name.clone(), gsi);
    set_current_gsi(world, &name);
}

#[given(regex = r#"^a hash-only GSI "([^"]*)" with no records$"#)]
async fn given_hash_only_empty(world: &mut VirtuusWorld, name: String) {
    let partition_key = infer_partition_key(&name);
    let gsi = Gsi::new(&name, &partition_key, None);
    ensure_gsis(world).insert(name.clone(), gsi);
    set_current_gsi(world, &name);
}

#[when(regex = r#"^I query the GSI for partition "([^"]*)"$"#)]
async fn when_query_partition(world: &mut VirtuusWorld, partition_value: String) {
    let gsi = current_gsi(world);
    world.last_result = gsi.query(&parse_value(&partition_value), None, false);
}

#[when(
    regex = r#"^I query the GSI for partition "([^"]*)" with sort condition (eq|ne|lt|lte|gt|gte|begins_with|contains) "([^"]*)"$"#
)]
async fn when_query_partition_with_condition(
    world: &mut VirtuusWorld,
    partition_value: String,
    op: String,
    value: String,
) {
    let gsi = current_gsi(world);
    let condition = match op.as_str() {
        "eq" => SortCondition::Eq(parse_value(&value)),
        "ne" => SortCondition::Ne(parse_value(&value)),
        "lt" => SortCondition::Lt(parse_value(&value)),
        "lte" => SortCondition::Lte(parse_value(&value)),
        "gt" => SortCondition::Gt(parse_value(&value)),
        "gte" => SortCondition::Gte(parse_value(&value)),
        "begins_with" => SortCondition::BeginsWith(value),
        "contains" => SortCondition::Contains(value),
        _ => panic!("Unknown sort operator: {op}"),
    };
    world.last_result = gsi.query(&parse_value(&partition_value), Some(&condition), false);
}

#[when(
    regex = r#"^I query the GSI for partition "([^"]*)" with sort condition between "([^"]*)" and "([^"]*)"$"#
)]
async fn when_query_partition_with_between(
    world: &mut VirtuusWorld,
    partition_value: String,
    low: String,
    high: String,
) {
    let gsi = current_gsi(world);
    let condition = SortCondition::Between(parse_value(&low), parse_value(&high));
    world.last_result = gsi.query(&parse_value(&partition_value), Some(&condition), false);
}

#[when(regex = r#"^I query the GSI for partition "([^"]*)" with sort direction "([^"]*)"$"#)]
async fn when_query_partition_with_direction(
    world: &mut VirtuusWorld,
    partition_value: String,
    direction: String,
) {
    let gsi = current_gsi(world);
    let descending = direction == "desc";
    world.last_result = gsi.query(&parse_value(&partition_value), None, descending);
}

#[then(regex = r#"^the result should contain PKs "([^"]*)" and "([^"]*)"$"#)]
async fn then_result_contains_two(world: &mut VirtuusWorld, pk1: String, pk2: String) {
    assert!(world.last_result.contains(&pk1));
    assert!(world.last_result.contains(&pk2));
}

#[then(regex = r#"^the result should not contain "([^"]*)"$"#)]
async fn then_result_not_contains(world: &mut VirtuusWorld, pk: String) {
    assert!(!world.last_result.contains(&pk));
}

#[then(regex = r#"^the result should return PKs in order: "([^"]*)", "([^"]*)", "([^"]*)"$"#)]
async fn then_result_order_three(world: &mut VirtuusWorld, pk1: String, pk2: String, pk3: String) {
    assert_eq!(world.last_result, vec![pk1, pk2, pk3]);
}

#[then(regex = r#"^the result should contain only "([^"]*)"$"#)]
async fn then_result_only(world: &mut VirtuusWorld, pk: String) {
    assert_eq!(world.last_result, vec![pk]);
}

#[then("the result should be empty")]
async fn then_result_empty_gsi(world: &mut VirtuusWorld) {
    assert!(world.last_result.is_empty());
}

#[then(regex = r#"^the GSI should exist with partition key "([^"]*)"$"#)]
async fn then_gsi_exists_partition(world: &mut VirtuusWorld, partition_key: String) {
    let gsi = current_gsi(world);
    assert_eq!(gsi.partition_key(), partition_key);
}

#[then("the GSI should have no sort key")]
async fn then_gsi_no_sort_key(world: &mut VirtuusWorld) {
    let gsi = current_gsi(world);
    assert!(gsi.sort_key().is_none());
}

#[then(regex = r#"^the GSI should have sort key "([^"]*)"$"#)]
async fn then_gsi_has_sort_key(world: &mut VirtuusWorld, sort_key: String) {
    let gsi = current_gsi(world);
    assert_eq!(gsi.sort_key(), Some(sort_key.as_str()));
}

#[then("both GSIs should exist independently")]
async fn then_both_gsis_exist(world: &mut VirtuusWorld) {
    assert_eq!(world.gsis.len(), 2);
}

#[when(regex = r#"^I put a record with pk "([^"]*)" and status "([^"]*)"$"#)]
async fn when_put_hash_only(world: &mut VirtuusWorld, pk: String, status: String) {
    let gsi = current_gsi_mut(world);
    let record = serde_json::Map::from_iter([("status".to_string(), Value::String(status))]);
    gsi.put(&pk, &Value::Object(record));
}

#[when(regex = r#"^I put a record with pk "([^"]*)", org_id "([^"]*)", and created_at "([^"]*)"$"#)]
async fn when_put_hash_range(
    world: &mut VirtuusWorld,
    pk: String,
    org_id: String,
    created_at: String,
) {
    let gsi = current_gsi_mut(world);
    let record = serde_json::Map::from_iter([
        ("org_id".to_string(), Value::String(org_id)),
        ("created_at".to_string(), Value::String(created_at)),
    ]);
    gsi.put(&pk, &Value::Object(record));
}

#[given(regex = r#"^a record with pk "([^"]*)" and status "([^"]*)" is indexed$"#)]
async fn given_record_indexed_hash_only(world: &mut VirtuusWorld, pk: String, status: String) {
    let gsi = current_gsi_mut(world);
    let record = serde_json::Map::from_iter([("status".to_string(), Value::String(status))]);
    gsi.put(&pk, &Value::Object(record));
}

#[given(
    regex = r#"^a record with pk "([^"]*)", org_id "([^"]*)", and created_at "([^"]*)" is indexed$"#
)]
async fn given_record_indexed_hash_range(
    world: &mut VirtuusWorld,
    pk: String,
    org_id: String,
    created_at: String,
) {
    let gsi = current_gsi_mut(world);
    let record = serde_json::Map::from_iter([
        ("org_id".to_string(), Value::String(org_id)),
        ("created_at".to_string(), Value::String(created_at)),
    ]);
    gsi.put(&pk, &Value::Object(record));
}

#[when(regex = r#"^I remove the record with pk "([^"]*)" and status "([^"]*)"$"#)]
async fn when_remove_hash_only(world: &mut VirtuusWorld, pk: String, status: String) {
    let gsi = current_gsi_mut(world);
    let record = serde_json::Map::from_iter([("status".to_string(), Value::String(status))]);
    gsi.remove(&pk, &Value::Object(record));
}

#[when(
    regex = r#"^I remove the record with pk "([^"]*)", org_id "([^"]*)", and created_at "([^"]*)"$"#
)]
async fn when_remove_hash_range(
    world: &mut VirtuusWorld,
    pk: String,
    org_id: String,
    created_at: String,
) {
    let gsi = current_gsi_mut(world);
    let record = serde_json::Map::from_iter([
        ("org_id".to_string(), Value::String(org_id)),
        ("created_at".to_string(), Value::String(created_at)),
    ]);
    gsi.remove(&pk, &Value::Object(record));
}

#[when(regex = r#"^I update the record with pk "([^"]*)" from status "([^"]*)" to "([^"]*)"$"#)]
async fn when_update_partition_change(
    world: &mut VirtuusWorld,
    pk: String,
    old_status: String,
    new_status: String,
) {
    let gsi = current_gsi_mut(world);
    let old_record =
        serde_json::Map::from_iter([("status".to_string(), Value::String(old_status))]);
    let new_record =
        serde_json::Map::from_iter([("status".to_string(), Value::String(new_status))]);
    gsi.update(&pk, &Value::Object(old_record), &Value::Object(new_record));
}

#[when(
    regex = r#"^I update the record with pk "([^"]*)" to created_at "([^"]*)" \(same org_id\)$"#
)]
async fn when_update_sort_change(world: &mut VirtuusWorld, pk: String, new_created_at: String) {
    world.last_update = Some(LastUpdate {
        pk: pk.clone(),
        old_created_at: "2025-01-15".to_string(),
        new_created_at: new_created_at.clone(),
    });
    let gsi = current_gsi_mut(world);
    let old_record = serde_json::Map::from_iter([
        ("org_id".to_string(), Value::String("org-a".to_string())),
        (
            "created_at".to_string(),
            Value::String("2025-01-15".to_string()),
        ),
    ]);
    let new_record = serde_json::Map::from_iter([
        ("org_id".to_string(), Value::String("org-a".to_string())),
        ("created_at".to_string(), Value::String(new_created_at)),
    ]);
    gsi.update(&pk, &Value::Object(old_record), &Value::Object(new_record));
}

#[when(regex = r#"^I put records "([^"]*)", "([^"]*)", "([^"]*)" all with status "([^"]*)"$"#)]
async fn when_put_multiple_hash_only(
    world: &mut VirtuusWorld,
    pk1: String,
    pk2: String,
    pk3: String,
    status: String,
) {
    let gsi = current_gsi_mut(world);
    for pk in [pk1, pk2, pk3] {
        let record =
            serde_json::Map::from_iter([("status".to_string(), Value::String(status.clone()))]);
        gsi.put(&pk, &Value::Object(record));
    }
}

#[when(regex = r#"^I put a record with pk "([^"]*)" and missing status$"#)]
async fn when_put_missing_partition(world: &mut VirtuusWorld, pk: String) {
    let gsi = current_gsi_mut(world);
    gsi.put(&pk, &Value::Object(serde_json::Map::new()));
}

#[when(regex = r#"^I put a record with pk "([^"]*)", org_id "([^"]*)", and missing created_at$"#)]
async fn when_put_missing_sort(world: &mut VirtuusWorld, pk: String, org_id: String) {
    let gsi = current_gsi_mut(world);
    let record = serde_json::Map::from_iter([("org_id".to_string(), Value::String(org_id))]);
    gsi.put(&pk, &Value::Object(record));
}

#[then(regex = r#"^querying the GSI for partition "([^"]*)" should include "([^"]*)"$"#)]
async fn then_query_partition_includes(
    world: &mut VirtuusWorld,
    partition_value: String,
    pk: String,
) {
    let gsi = current_gsi(world);
    let result = gsi.query(&parse_value(&partition_value), None, false);
    assert!(result.contains(&pk));
}

#[then(regex = r#"^querying the GSI for partition "([^"]*)" should not include "([^"]*)"$"#)]
async fn then_query_partition_not_includes(
    world: &mut VirtuusWorld,
    partition_value: String,
    pk: String,
) {
    let gsi = current_gsi(world);
    let result = gsi.query(&parse_value(&partition_value), None, false);
    assert!(!result.contains(&pk));
}

#[then(regex = r#"^querying the GSI for partition "([^"]*)" should return all 3 PKs$"#)]
async fn then_query_partition_all_three(world: &mut VirtuusWorld, partition_value: String) {
    let gsi = current_gsi(world);
    let result = gsi.query(&parse_value(&partition_value), None, false);
    assert_eq!(result.len(), 3);
}

#[then(regex = r#"^the record should appear at the new sort position in partition "([^"]*)"$"#)]
async fn then_record_new_sort_position(world: &mut VirtuusWorld, partition_value: String) {
    let update = world.last_update.clone().expect("Missing update context");
    let gsi = current_gsi(world);
    let new_condition = SortCondition::Eq(parse_value(&update.new_created_at));
    let old_condition = SortCondition::Eq(parse_value(&update.old_created_at));
    let new_result = gsi.query(&parse_value(&partition_value), Some(&new_condition), false);
    let old_result = gsi.query(&parse_value(&partition_value), Some(&old_condition), false);
    assert!(new_result.contains(&update.pk));
    assert!(!old_result.contains(&update.pk));
}

#[then(regex = r#"^querying the GSI for partition "([^"]*)" should be empty$"#)]
async fn then_query_partition_empty(world: &mut VirtuusWorld, partition_value: String) {
    let gsi = current_gsi(world);
    let result = gsi.query(&parse_value(&partition_value), None, false);
    assert!(result.is_empty());
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    VirtuusWorld::run("../features").await;
}
