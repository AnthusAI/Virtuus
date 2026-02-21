use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};

#[allow(unused_imports)]
use cucumber::step;
use cucumber::{given, then, when, World};
use serde_json::{json, Value};
use virtuus::gsi::Gsi;
use virtuus::sort::SortCondition;
use virtuus::table::{Table, ValidationMode};

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
    pub tables: HashMap<String, Table>,
    pub current_table: Option<String>,
    pub last_record: Option<Value>,
    pub last_records: Vec<Value>,
    pub last_count: Option<usize>,
    pub temp_dir: Option<PathBuf>,
    pub export_dir: Option<PathBuf>,
    pub error: Option<String>,
    pub hook_calls: Option<std::sync::Arc<std::sync::Mutex<Vec<Value>>>>,
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

fn ensure_tables(world: &mut VirtuusWorld) -> &mut HashMap<String, Table> {
    &mut world.tables
}

fn set_current_table(world: &mut VirtuusWorld, name: &str) {
    world.current_table = Some(name.to_string());
}

fn current_table(world: &mut VirtuusWorld) -> &mut Table {
    let name = world.current_table.as_ref().expect("No current table");
    world.tables.get_mut(name).expect("Table not found")
}

fn create_table(
    world: &mut VirtuusWorld,
    name: &str,
    primary_key: Option<&str>,
    partition_key: Option<&str>,
    sort_key: Option<&str>,
    directory: Option<PathBuf>,
    validation: ValidationMode,
) {
    let table = Table::new(
        name,
        primary_key,
        partition_key,
        sort_key,
        directory,
        validation,
    );
    ensure_tables(world).insert(name.to_string(), table);
    set_current_table(world, name);
}

fn temp_dir(world: &mut VirtuusWorld) -> PathBuf {
    if world.temp_dir.is_none() {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "virtuus_table_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        world.temp_dir = Some(dir);
    }
    world.temp_dir.clone().unwrap()
}

fn parse_record(text: &str) -> Value {
    serde_json::from_str(text).expect("record json")
}

fn panic_message(err: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = err.downcast_ref::<&str>() {
        return message.to_string();
    }
    if let Some(message) = err.downcast_ref::<String>() {
        return message.clone();
    }
    "panic".to_string()
}

fn table_key_fields(table: &Table) -> (Option<String>, Option<String>, Option<String>) {
    let description = table.describe();
    let pk = description
        .get("primary_key")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string());
    let partition_key = description
        .get("partition_key")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string());
    let sort_key = description
        .get("sort_key")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string());
    (pk, partition_key, sort_key)
}

fn build_record_for_table(table: &Table, index: usize) -> Value {
    let (pk, partition_key, sort_key) = table_key_fields(table);
    if let Some(key) = pk {
        return json!({ key: format!("item-{index}") });
    }
    let partition = partition_key.expect("partition key");
    let sort = sort_key.expect("sort key");
    json!({
        partition: format!("item-{index}"),
        sort: format!("sort-{index}"),
    })
}

fn parse_step_table(step: &cucumber::gherkin::Step) -> Vec<Value> {
    let table = step.table().expect("Missing table");
    let headers = &table.rows[0];
    let mut records = Vec::new();
    for row in table.rows.iter().skip(1) {
        let mut map = serde_json::Map::new();
        for (header, cell) in headers.iter().zip(row.iter()) {
            map.insert(header.clone(), Value::String(cell.clone()));
        }
        records.push(Value::Object(map));
    }
    records
}

fn validation_mode(mode: &str) -> ValidationMode {
    match mode {
        "silent" => ValidationMode::Silent,
        "warn" => ValidationMode::Warn,
        "error" => ValidationMode::Error,
        _ => panic!("invalid validation mode"),
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
// Table steps
// ---------------------------------------------------------------------------

#[given(regex = r#"^I create a table "([^"]*)" with primary key "([^"]*)"$"#)]
async fn given_create_table_simple(world: &mut VirtuusWorld, name: String, primary_key: String) {
    create_table(
        world,
        &name,
        Some(&primary_key),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
}

#[given(
    regex = r#"^I create a table "([^"]*)" with partition key "([^"]*)" and sort key "([^"]*)"$"#
)]
async fn given_create_table_composite(
    world: &mut VirtuusWorld,
    name: String,
    partition_key: String,
    sort_key: String,
) {
    create_table(
        world,
        &name,
        None,
        Some(&partition_key),
        Some(&sort_key),
        None,
        ValidationMode::Silent,
    );
}

#[then(regex = r#"^the table "([^"]*)" should exist$"#)]
async fn then_table_exists(world: &mut VirtuusWorld, name: String) {
    assert!(world.tables.contains_key(&name));
}

#[then(regex = r#"^the table should use "([^"]*)" as its primary key$"#)]
async fn then_table_primary_key(world: &mut VirtuusWorld, primary_key: String) {
    let table = current_table(world);
    let description = table.describe();
    assert_eq!(
        description.get("primary_key").and_then(|v| v.as_str()),
        Some(primary_key.as_str())
    );
}

#[then(regex = r#"^the table should use "([^"]*)" as partition key and "([^"]*)" as sort key$"#)]
async fn then_table_composite_keys(
    world: &mut VirtuusWorld,
    partition_key: String,
    sort_key: String,
) {
    let table = current_table(world);
    let description = table.describe();
    assert_eq!(
        description.get("partition_key").and_then(|v| v.as_str()),
        Some(partition_key.as_str())
    );
    assert_eq!(
        description.get("sort_key").and_then(|v| v.as_str()),
        Some(sort_key.as_str())
    );
}

#[then(regex = r#"^the table should contain (\d+) records$"#)]
async fn then_table_contains_records(world: &mut VirtuusWorld, count: usize) {
    let table = current_table(world);
    assert_eq!(table.count(None, None), count);
}

#[given(regex = r#"^a table "([^"]*)" with primary key "([^"]*)"$"#)]
async fn given_table_simple(world: &mut VirtuusWorld, name: String, primary_key: String) {
    create_table(
        world,
        &name,
        Some(&primary_key),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
}

#[given(regex = r#"^a table "([^"]*)" with partition key "([^"]*)" and sort key "([^"]*)"$"#)]
async fn given_table_composite(
    world: &mut VirtuusWorld,
    name: String,
    partition_key: String,
    sort_key: String,
) {
    create_table(
        world,
        &name,
        None,
        Some(&partition_key),
        Some(&sort_key),
        None,
        ValidationMode::Silent,
    );
}

#[when(regex = r#"^I put a record (\{.*\})$"#)]
async fn when_put_record(world: &mut VirtuusWorld, record_text: String) {
    let record = parse_record(&record_text);
    world.last_record = Some(record.clone());
    world.error = None;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let table = current_table(world);
        table.put(record);
    }));
    if let Err(err) = result {
        world.error = Some(panic_message(err));
    }
}

#[then(regex = r#"^getting record "([^"]*)" should return (.+)$"#)]
async fn then_get_record_returns(world: &mut VirtuusWorld, pk: String, record_text: String) {
    let table = current_table(world);
    let expected = parse_record(&record_text);
    if expected.is_null() {
        assert!(table.get(&pk, None).is_none());
    } else {
        let result = table.get(&pk, None).expect("record missing");
        assert_eq!(result, expected);
    }
}

#[given(regex = r#"^a record (\{.*\})$"#)]
async fn given_record(world: &mut VirtuusWorld, record_text: String) {
    let record = parse_record(&record_text);
    let table = current_table(world);
    table.put(record);
}

#[when(regex = r#"^I get record "([^"]*)"$"#)]
async fn when_get_record(world: &mut VirtuusWorld, pk: String) {
    let table = current_table(world);
    world.last_record = table.get(&pk, None);
}

#[then("the result should be null")]
async fn then_result_null(world: &mut VirtuusWorld) {
    assert!(world.last_record.is_none());
}

#[when(regex = r#"^I delete record "([^"]*)"$"#)]
async fn when_delete_record(world: &mut VirtuusWorld, pk: String) {
    let table = current_table(world);
    table.delete(&pk, None);
}

#[then("no error should occur")]
async fn then_no_error(world: &mut VirtuusWorld) {
    assert!(world.error.is_none());
}

#[given("records:")]
async fn given_records_table(world: &mut VirtuusWorld, step: &cucumber::gherkin::Step) {
    let records = parse_step_table(step);
    let table = current_table(world);
    for record in records {
        table.put(record);
    }
}

#[when("I scan the table")]
async fn when_scan_table(world: &mut VirtuusWorld) {
    let table = current_table(world);
    world.last_records = table.scan();
}

#[then(regex = r#"^the result should contain (\d+) records$"#)]
async fn then_result_contains(world: &mut VirtuusWorld, count: usize) {
    assert_eq!(world.last_records.len(), count);
}

#[when(regex = r#"^I bulk load (\d+) records$"#)]
async fn when_bulk_load(world: &mut VirtuusWorld, count: usize) {
    let table = current_table(world);
    let mut records = Vec::new();
    for i in 0..count {
        records.push(build_record_for_table(table, i));
    }
    table.bulk_load(records);
}

#[then("no error or warning should occur")]
async fn then_no_error_or_warning(world: &mut VirtuusWorld) {
    assert!(world.error.is_none());
    let warnings_empty = current_table(world).warnings().is_empty();
    assert!(warnings_empty);
}

#[then("a warning should be logged about the missing primary key")]
async fn then_warning_missing_pk(world: &mut VirtuusWorld) {
    let table = current_table(world);
    assert!(table
        .warnings()
        .iter()
        .any(|w| w.contains("missing primary key")));
}

#[then("an error should be raised about the missing primary key")]
async fn then_error_missing_pk(world: &mut VirtuusWorld) {
    let error = world.error.as_deref().unwrap_or("");
    assert!(error.contains("missing primary key"));
}

#[given(regex = r#"^a table "([^"]*)" with primary key "([^"]*)" and validation "([^"]*)"$"#)]
async fn given_table_validation(
    world: &mut VirtuusWorld,
    name: String,
    primary_key: String,
    validation: String,
) {
    create_table(
        world,
        &name,
        Some(&primary_key),
        None,
        None,
        None,
        validation_mode(&validation),
    );
}

#[given(regex = r#"^a GSI "([^"]*)" with partition key "([^"]*)"$"#)]
async fn given_gsi_for_table(world: &mut VirtuusWorld, name: String, partition_key: String) {
    let table = current_table(world);
    table.add_gsi(&name, &partition_key, None);
}

#[then(regex = r#"^a warning should be logged about the missing GSI field "([^"]*)"$"#)]
async fn then_warning_missing_gsi(world: &mut VirtuusWorld, field: String) {
    let table = current_table(world);
    assert!(table.warnings().iter().any(|w| w.contains(&field)));
}

#[when("I put a record")]
async fn when_put_blank_record(world: &mut VirtuusWorld) {
    let record = {
        let table = current_table(world);
        build_record_for_table(table, 0)
    };
    world.last_record = Some(record.clone());
    world.error = None;
    let table = current_table(world);
    table.put(record);
}

#[given(regex = r#"^a table "([^"]*)" backed by a directory$"#)]
async fn given_table_backed_dir(world: &mut VirtuusWorld, name: String) {
    let directory = temp_dir(world);
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        Some(directory),
        ValidationMode::Silent,
    );
}

#[given(regex = r#"^a table "([^"]*)" with primary key "([^"]*)" backed by a directory$"#)]
async fn given_table_pk_backed_dir(world: &mut VirtuusWorld, name: String, primary_key: String) {
    let directory = temp_dir(world);
    create_table(
        world,
        &name,
        Some(&primary_key),
        None,
        None,
        Some(directory),
        ValidationMode::Silent,
    );
}

#[given(
    regex = r#"^a table "([^"]*)" with partition key "([^"]*)" and sort key "([^"]*)" backed by a directory$"#
)]
async fn given_table_composite_backed_dir(
    world: &mut VirtuusWorld,
    name: String,
    partition_key: String,
    sort_key: String,
) {
    let directory = temp_dir(world);
    create_table(
        world,
        &name,
        None,
        Some(&partition_key),
        Some(&sort_key),
        Some(directory),
        ValidationMode::Silent,
    );
}

#[then(regex = r#"^a JSON file for "([^"]*)" should exist in the directory$"#)]
async fn then_json_file_exists(world: &mut VirtuusWorld, pk: String) {
    let directory = temp_dir(world);
    let path = directory.join(format!("{pk}.json"));
    assert!(path.exists());
}

#[then("the file should contain the record data")]
async fn then_file_contains_data(world: &mut VirtuusWorld) {
    let record = world.last_record.clone().expect("Missing record");
    let table = current_table(world);
    let (pk, partition_key, sort_key) = table_key_fields(table);
    let filename = if let Some(pk_field) = pk {
        let value = record
            .get(&pk_field)
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        format!("{value}.json")
    } else {
        let partition = partition_key.expect("partition key");
        let sort = sort_key.expect("sort key");
        let partition_value = record
            .get(&partition)
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let sort_value = record
            .get(&sort)
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        format!("{partition_value}__{sort_value}.json")
    };
    let directory = temp_dir(world);
    let path = directory.join(filename);
    let data: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(data, record);
}

#[given(regex = r#"^a record (\{.*\}) persisted to disk$"#)]
async fn given_record_persisted(world: &mut VirtuusWorld, record_text: String) {
    let record = parse_record(&record_text);
    let table = current_table(world);
    table.put(record.clone());
    world.last_record = Some(record);
}

#[then(regex = r#"^the JSON file for "([^"]*)" should not exist in the directory$"#)]
async fn then_json_file_not_exists(world: &mut VirtuusWorld, pk: String) {
    let directory = temp_dir(world);
    let path = directory.join(format!("{pk}.json"));
    assert!(!path.exists());
}

#[given("a directory with 5 JSON files representing user records")]
async fn given_directory_with_json(world: &mut VirtuusWorld) {
    let directory = temp_dir(world);
    for i in 0..5 {
        let record = json!({"id": format!("user-{i}"), "name": format!("User {i}")});
        let path = directory.join(format!("user-{i}.json"));
        fs::write(path, serde_json::to_vec(&record).unwrap()).unwrap();
    }
}

#[when(regex = r#"^I create a table "([^"]*)" and load from that directory$"#)]
async fn when_create_table_load_dir(world: &mut VirtuusWorld, name: String) {
    let directory = temp_dir(world);
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        Some(directory),
        ValidationMode::Silent,
    );
    let table = current_table(world);
    table.load_from_dir(None);
}

#[then("each record should match its source file")]
async fn then_records_match_files(world: &mut VirtuusWorld) {
    let table = current_table(world);
    for record in table.scan() {
        let pk = record
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let path = temp_dir(world).join(format!("{pk}.json"));
        let data: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(data, record);
    }
}

#[when("I load the table from that directory")]
async fn when_load_table_from_dir(world: &mut VirtuusWorld) {
    if world.current_table.is_none() {
        let directory = temp_dir(world);
        create_table(
            world,
            "users",
            Some("id"),
            None,
            None,
            Some(directory),
            ValidationMode::Silent,
        );
    }
    let table = current_table(world);
    table.load_from_dir(None);
}

#[given("a directory with 3 JSON files and 2 non-JSON files")]
async fn given_directory_with_non_json(world: &mut VirtuusWorld) {
    let directory = temp_dir(world);
    for i in 0..3 {
        let record = json!({"id": format!("user-{i}"), "name": format!("User {i}")});
        let path = directory.join(format!("user-{i}.json"));
        fs::write(path, serde_json::to_vec(&record).unwrap()).unwrap();
    }
    for i in 0..2 {
        let path = directory.join(format!("note-{i}.txt"));
        fs::write(path, b"ignore").unwrap();
    }
}

#[then("the non-JSON files should be untouched")]
async fn then_non_json_untouched(world: &mut VirtuusWorld) {
    let directory = temp_dir(world);
    for i in 0..2 {
        let path = directory.join(format!("note-{i}.txt"));
        assert!(path.exists());
    }
}

#[then("the write should use a temporary file followed by an atomic rename")]
async fn then_atomic_write(world: &mut VirtuusWorld) {
    let table = current_table(world);
    assert!(table.last_write_used_atomic());
}

#[then(regex = r#"^the file should be named "([^"]*)"$"#)]
async fn then_file_named(world: &mut VirtuusWorld, filename: String) {
    let directory = temp_dir(world);
    let path = directory.join(filename);
    assert!(path.exists());
}

#[then("an error should be raised about invalid PK characters")]
async fn then_invalid_pk_error(world: &mut VirtuusWorld) {
    let error = world.error.as_deref().unwrap_or("");
    assert!(error.contains("invalid PK"));
}

#[when(regex = r#"^I put a record (\{.*\}) missing the "([^"]*)" field$"#)]
async fn when_put_record_missing_field(
    world: &mut VirtuusWorld,
    record_text: String,
    field: String,
) {
    let mut record = parse_record(&record_text);
    if let Value::Object(map) = &mut record {
        map.remove(&field);
    }
    world.error = None;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let table = current_table(world);
        table.put(record.clone());
    }));
    if let Err(err) = result {
        world.error = Some(panic_message(err));
    }
}

#[given(regex = r#"^a table "([^"]*)" with a GSI "([^"]*)" on "([^"]*)"$"#)]
async fn given_table_with_gsi(
    world: &mut VirtuusWorld,
    name: String,
    gsi_name: String,
    field: String,
) {
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
    let table = current_table(world);
    table.add_gsi(&gsi_name, &field, None);
}

#[given(
    regex = r#"^a table "([^"]*)" with a GSI "([^"]*)" on "([^"]*)" and a GSI "([^"]*)" on "([^"]*)"$"#
)]
async fn given_table_with_two_gsis(
    world: &mut VirtuusWorld,
    name: String,
    gsi_one: String,
    field_one: String,
    gsi_two: String,
    field_two: String,
) {
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
    let table = current_table(world);
    table.add_gsi(&gsi_one, &field_one, None);
    table.add_gsi(&gsi_two, &field_two, None);
}

#[when(regex = r#"^I add a GSI "([^"]*)" with partition key "([^"]*)"$"#)]
async fn when_add_gsi(world: &mut VirtuusWorld, gsi_name: String, partition_key: String) {
    let table = current_table(world);
    table.add_gsi(&gsi_name, &partition_key, None);
}

#[then(regex = r#"^the table should have a GSI named "([^"]*)"$"#)]
async fn then_table_has_gsi(world: &mut VirtuusWorld, gsi_name: String) {
    let table = current_table(world);
    assert!(table.gsis().contains_key(&gsi_name));
}

#[then(regex = r#"^querying GSI "([^"]*)" for "([^"]*)" should return "([^"]*)"$"#)]
async fn then_query_gsi_returns(
    world: &mut VirtuusWorld,
    gsi_name: String,
    value: String,
    pk: String,
) {
    let table = current_table(world);
    let gsi = table.gsis().get(&gsi_name).expect("missing gsi");
    let result = gsi.query(&parse_value(&value), None, false);
    assert!(result.contains(&pk));
}

#[then(regex = r#"^querying GSI "([^"]*)" for "([^"]*)" should return empty$"#)]
async fn then_query_gsi_empty(world: &mut VirtuusWorld, gsi_name: String, value: String) {
    let table = current_table(world);
    let gsi = table.gsis().get(&gsi_name).expect("missing gsi");
    let result = gsi.query(&parse_value(&value), None, false);
    assert!(result.is_empty());
}

#[when(regex = r#"^I query the table via GSI "([^"]*)" for "([^"]*)"$"#)]
async fn when_query_table_gsi(world: &mut VirtuusWorld, gsi_name: String, value: String) {
    let table = current_table(world);
    world.last_records = table.query_gsi(&gsi_name, &parse_value(&value));
}

#[then("the result should contain 2 full records with all fields")]
async fn then_result_full_records(world: &mut VirtuusWorld) {
    assert_eq!(world.last_records.len(), 2);
    for record in &world.last_records {
        assert!(record.get("id").is_some());
        assert!(record.get("name").is_some());
        assert!(record.get("status").is_some());
    }
}

#[given(regex = r#"^a table "([^"]*)" with (\d+) records$"#)]
async fn given_table_with_count(world: &mut VirtuusWorld, name: String, count: usize) {
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
    let table = current_table(world);
    for i in 0..count {
        table.put(json!({"id": format!("user-{i}"), "name": format!("User {i}")}));
    }
}

#[when("I call count on the table")]
async fn when_call_count(world: &mut VirtuusWorld) {
    let table = current_table(world);
    world.last_count = Some(table.count(None, None));
}

#[then(regex = r#"^the result should be (\d+)$"#)]
async fn then_count_result(world: &mut VirtuusWorld, count: usize) {
    assert_eq!(world.last_count, Some(count));
}

#[given(regex = r#"^(\d+) records with status "([^"]*)" and (\d+) with status "([^"]*)"$"#)]
async fn given_records_with_status(
    world: &mut VirtuusWorld,
    count: usize,
    status: String,
    count_two: usize,
    status_two: String,
) {
    let table = current_table(world);
    for i in 0..count {
        table.put(json!({"id": format!("active-{i}"), "status": status}));
    }
    for i in 0..count_two {
        table.put(json!({"id": format!("inactive-{i}"), "status": status_two}));
    }
}

#[given(regex = r#"^(\d+) records with status "([^"]*)"$"#)]
async fn given_records_with_status_single(world: &mut VirtuusWorld, count: usize, status: String) {
    let table = current_table(world);
    for i in 0..count {
        table.put(json!({"id": format!("{status}-{i}"), "status": status}));
    }
}

#[when(regex = r#"^I call count on index "([^"]*)" for value "([^"]*)"$"#)]
async fn when_count_index(world: &mut VirtuusWorld, gsi_name: String, value: String) {
    let table = current_table(world);
    world.last_count = Some(table.count(Some(&gsi_name), Some(&parse_value(&value))));
}

#[given(regex = r#"^an empty table "([^"]*)"$"#)]
async fn given_empty_table(world: &mut VirtuusWorld, name: String) {
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
}

#[given(regex = r#"^a table "([^"]*)" with 10 records in memory$"#)]
async fn given_table_ten_records(world: &mut VirtuusWorld, name: String) {
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
    let table = current_table(world);
    for i in 0..10 {
        table.put(json!({"id": format!("user-{i}"), "name": format!("User {i}")}));
    }
}

#[when("I export the table to a new directory")]
async fn when_export_table(world: &mut VirtuusWorld) {
    let mut dir = temp_dir(world);
    dir.push(format!(
        "export_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    world.export_dir = Some(dir.clone());
    let table = current_table(world);
    table.export(dir);
}

#[when("I export the table to a directory")]
async fn when_export_table_existing(world: &mut VirtuusWorld) {
    let mut dir = temp_dir(world);
    dir.push(format!(
        "export_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    world.export_dir = Some(dir.clone());
    let table = current_table(world);
    table.export(dir);
}

#[then(regex = r#"^the directory should contain (\d+) JSON files$"#)]
async fn then_directory_contains(world: &mut VirtuusWorld, count: usize) {
    let dir = world.export_dir.clone().expect("missing export dir");
    let files = fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("json"))
        .count();
    assert_eq!(files, count);
}

#[then("each file should contain a valid JSON record")]
async fn then_each_file_valid_json(world: &mut VirtuusWorld) {
    let dir = world.export_dir.clone().expect("missing export dir");
    for entry in fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let data = fs::read_to_string(path).unwrap();
        let _: Value = serde_json::from_str(&data).unwrap();
    }
}

#[given(regex = r#"^a table "([^"]*)" with records in memory$"#)]
async fn given_table_with_records(world: &mut VirtuusWorld, name: String) {
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
    let table = current_table(world);
    table.put(json!({"id": "user-1", "name": "Alice"}));
}

#[then("each file should be written atomically via temp+rename")]
async fn then_export_atomic(world: &mut VirtuusWorld) {
    let table = current_table(world);
    assert!(table.last_write_used_atomic());
}

#[then("the directory should exist and contain 0 files")]
async fn then_export_empty(world: &mut VirtuusWorld) {
    let dir = world.export_dir.clone().expect("missing export dir");
    assert!(dir.exists());
    let files = fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("json"))
        .count();
    assert_eq!(files, 0);
}

#[when("I create a new table and load from that directory")]
async fn when_new_table_load(world: &mut VirtuusWorld) {
    let directory = world.export_dir.clone().expect("missing export dir");
    create_table(
        world,
        "users",
        Some("id"),
        None,
        None,
        Some(directory),
        ValidationMode::Silent,
    );
    let table = current_table(world);
    table.load_from_dir(None);
}

#[then("the new table should contain the same 5 records")]
async fn then_new_table_same_records(world: &mut VirtuusWorld) {
    let table = current_table(world);
    assert_eq!(table.count(None, None), 5);
}

#[given(regex = r#"^a table "([^"]*)" with an on_put hook registered$"#)]
async fn given_table_on_put(world: &mut VirtuusWorld, name: String) {
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_clone = calls.clone();
    let table = current_table(world);
    table.register_on_put(Box::new(move |record| {
        calls_clone.lock().unwrap().push(record.clone());
    }));
    world.hook_calls = Some(calls);
}

#[then("the on_put hook should have been called with the record")]
async fn then_on_put_called(world: &mut VirtuusWorld) {
    let calls = world.hook_calls.as_ref().expect("missing hook calls");
    assert!(!calls.lock().unwrap().is_empty());
}

#[given(regex = r#"^a table "([^"]*)" with an on_delete hook registered$"#)]
async fn given_table_on_delete(world: &mut VirtuusWorld, name: String) {
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_clone = calls.clone();
    let table = current_table(world);
    table.register_on_delete(Box::new(move |record| {
        calls_clone.lock().unwrap().push(record.clone());
    }));
    world.hook_calls = Some(calls);
}

#[then("the on_delete hook should have been called with the record")]
async fn then_on_delete_called(world: &mut VirtuusWorld) {
    let calls = world.hook_calls.as_ref().expect("missing hook calls");
    assert!(!calls.lock().unwrap().is_empty());
}

#[given(regex = r#"^a table "([^"]*)" with 3 on_put hooks registered$"#)]
async fn given_table_three_hooks(world: &mut VirtuusWorld, name: String) {
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
    let calls = Arc::new(Mutex::new(Vec::new()));
    let table = current_table(world);
    for idx in 0..3 {
        let calls_clone = calls.clone();
        table.register_on_put(Box::new(move |_| {
            calls_clone.lock().unwrap().push(json!(idx));
        }));
    }
    world.hook_calls = Some(calls);
}

#[then("all 3 hooks should fire in registration order")]
async fn then_hooks_in_order(world: &mut VirtuusWorld) {
    let calls = world.hook_calls.as_ref().expect("missing hook calls");
    let values = calls.lock().unwrap().clone();
    assert_eq!(values, vec![json!(0), json!(1), json!(2)]);
}

#[given(regex = r#"^a table "([^"]*)" with an on_put hook that raises an error$"#)]
async fn given_table_hook_error(world: &mut VirtuusWorld, name: String) {
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
    let table = current_table(world);
    table.register_on_put(Box::new(|_| panic!("hook error")));
}

#[then("the record should be stored successfully")]
async fn then_record_stored(world: &mut VirtuusWorld) {
    let table = current_table(world);
    assert_eq!(table.count(None, None), 1);
}

#[then("the hook error should be logged")]
async fn then_hook_error_logged(world: &mut VirtuusWorld) {
    let table = current_table(world);
    assert!(!table.hook_errors().is_empty());
}

#[then("the hook should receive all fields of the record")]
async fn then_hook_received_full_record(world: &mut VirtuusWorld) {
    let calls = world.hook_calls.as_ref().expect("missing hook calls");
    let guard = calls.lock().unwrap();
    let record = guard.last().expect("missing record");
    assert!(record.get("id").is_some());
    assert!(record.get("name").is_some());
    assert!(record.get("email").is_some());
}

#[then("the result should include:")]
async fn then_describe_includes(world: &mut VirtuusWorld, step: &cucumber::gherkin::Step) {
    let table = current_table(world);
    let description = table.describe();
    let table_rows = step.table().expect("missing table");
    for row in table_rows.rows.iter().skip(1) {
        let field = &row[0];
        let value = &row[1];
        let actual = description
            .get(field)
            .map(|v| v.to_string())
            .unwrap_or_default();
        assert_eq!(actual.trim_matches('"'), value);
    }
}

#[then(regex = r#"^the result should list GSI "([^"]*)"$"#)]
async fn then_describe_lists_gsi(world: &mut VirtuusWorld, gsi_name: String) {
    let table = current_table(world);
    let description = table.describe();
    let gsis = description
        .get("gsis")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(gsis.iter().any(|v| v.as_str() == Some(&gsi_name)));
}

#[then(regex = r#"^the result should list association "([^"]*)"$"#)]
async fn then_describe_lists_association(world: &mut VirtuusWorld, association: String) {
    let table = current_table(world);
    let description = table.describe();
    let associations = description
        .get("associations")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(associations
        .iter()
        .any(|v| v.as_str() == Some(&association)));
}

#[then(regex = r#"^the result should include record_count of (\d+)$"#)]
async fn then_describe_record_count(world: &mut VirtuusWorld, count: usize) {
    let table = current_table(world);
    let description = table.describe();
    assert_eq!(
        description.get("record_count").and_then(|v| v.as_u64()),
        Some(count as u64)
    );
}

#[given(regex = r#"^a has_many association "([^"]*)" to table "([^"]*)"$"#)]
async fn given_has_many_association(
    world: &mut VirtuusWorld,
    association: String,
    _target: String,
) {
    let table = current_table(world);
    table.add_association(&association);
}

#[given(regex = r#"^a table "([^"]*)" with a has_many association "([^"]*)" to table "([^"]*)"$"#)]
async fn given_table_has_many(
    world: &mut VirtuusWorld,
    name: String,
    association: String,
    _target: String,
) {
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
    let table = current_table(world);
    table.add_association(&association);
}

#[then("the result should contain 2 posts for user-1")]
async fn then_result_posts(world: &mut VirtuusWorld) {
    assert_eq!(world.last_records.len(), 2);
}

#[given(regex = r#"^a table "([^"]*)" with primary key "([^"]*)" and (\d+) records$"#)]
async fn given_table_pk_records(
    world: &mut VirtuusWorld,
    name: String,
    primary_key: String,
    count: usize,
) {
    create_table(
        world,
        &name,
        Some(&primary_key),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
    let table = current_table(world);
    for i in 0..count {
        table.put(json!({primary_key.clone(): format!("user-{i}")}));
    }
}

#[then(
    regex = r#"^getting record with partition "([^"]*)" and sort "([^"]*)" should return that record$"#
)]
async fn then_get_composite_record(world: &mut VirtuusWorld, partition: String, sort: String) {
    let table = current_table(world);
    assert!(table.get(&partition, Some(&sort)).is_some());
}

#[then(
    regex = r#"^getting record with partition "([^"]*)" and sort "([^"]*)" should return null$"#
)]
async fn then_get_composite_null(world: &mut VirtuusWorld, partition: String, sort: String) {
    let table = current_table(world);
    assert!(table.get(&partition, Some(&sort)).is_none());
}

#[then(
    regex = r#"^getting record with partition "([^"]*)" and sort "([^"]*)" should return score (\d+)$"#
)]
async fn then_get_composite_score(
    world: &mut VirtuusWorld,
    partition: String,
    sort: String,
    score: i64,
) {
    let table = current_table(world);
    let record = table.get(&partition, Some(&sort)).expect("record missing");
    assert_eq!(record.get("score").and_then(|v| v.as_i64()), Some(score));
}

#[when(regex = r#"^I delete record with partition "([^"]*)" and sort "([^"]*)"$"#)]
async fn when_delete_composite(world: &mut VirtuusWorld, partition: String, sort: String) {
    let table = current_table(world);
    table.delete(&partition, Some(&sort));
}

#[when(regex = r#"^I put a record missing the "([^"]*)" field$"#)]
async fn when_put_missing_field_simple(world: &mut VirtuusWorld, field: String) {
    let mut record = json!({"name": "Missing"});
    if let Value::Object(map) = &mut record {
        map.remove(&field);
    }
    world.error = None;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let table = current_table(world);
        table.put(record.clone());
    }));
    if let Err(err) = result {
        world.error = Some(panic_message(err));
    }
}

#[given(regex = r#"^an empty table "([^"]*)" with primary key "([^"]*)"$"#)]
async fn given_empty_table_with_pk(world: &mut VirtuusWorld, name: String, primary_key: String) {
    create_table(
        world,
        &name,
        Some(&primary_key),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
}

#[given(regex = r#"^(\d+) records loaded$"#)]
async fn given_records_loaded(world: &mut VirtuusWorld, count: usize) {
    let table = current_table(world);
    for i in 0..count {
        let record = build_record_for_table(table, i);
        table.put(record);
    }
}

#[when("I call describe on the table")]
async fn when_call_describe(world: &mut VirtuusWorld) {
    let table = current_table(world);
    world.last_record = Some(table.describe());
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    VirtuusWorld::run("../features").await;
}
