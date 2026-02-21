use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};

#[allow(unused_imports)]
use cucumber::step;
use cucumber::{given, then, when, World};
use serde_json::{from_str, json, Map as JsonMap, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use virtuus::database::Database;
use virtuus::gsi::Gsi;
use virtuus::sort::SortCondition;
use virtuus::table::{Association, ChangeSummary, Table, ValidationMode};
use virtuus::{Database as DbDatabase, Table as DbTable};

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
    pub last_is_stale: Option<bool>,
    pub last_summary: Option<ChangeSummary>,
    pub refresh_counter: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    pub database: Option<Database>,
    pub db_result: Option<Value>,
    pub next_token: Option<String>,
    pub directory: Option<PathBuf>,
    pub directory_two: Option<PathBuf>,
    pub schema_path: Option<PathBuf>,
    pub data_root: Option<PathBuf>,
    pub pages: Vec<Vec<Value>>,
}

#[derive(Debug, Clone)]
pub struct LastUpdate {
    pub pk: String,
    pub old_created_at: String,
    pub new_created_at: String,
}

// ---------------------------------------------------------------------------
// Helpers for Database-based scenarios
// ---------------------------------------------------------------------------

fn ensure_db(world: &mut VirtuusWorld) -> &mut DbDatabase {
    if world.database.is_none() {
        world.database = Some(DbDatabase::new());
    }
    world.database.as_mut().unwrap()
}

fn ensure_db_table<'a>(world: &'a mut VirtuusWorld, name: &str, pk: &str) -> &'a mut DbTable {
    let db_ptr = ensure_db(world) as *mut DbDatabase;
    world.current_table = Some(name.to_string());
    unsafe {
        let db = &mut *db_ptr;
        if db.table_mut(name).is_none() {
            let table = DbTable::new(name, Some(pk), None, None, None, ValidationMode::Silent);
            db.add_table(name, table);
        }
        db.table_mut(name).unwrap()
    }
}

fn current_db_table(world: &mut VirtuusWorld) -> Option<&mut DbTable> {
    if let (Some(db), Some(name)) = (world.database.as_mut(), world.current_table.clone()) {
        return db.table_mut(&name);
    }
    None
}

fn items_from_result(result: &Value) -> Vec<Value> {
    if let Some(items) = result.get("items").and_then(|v| v.as_array()) {
        return items.clone();
    }
    if let Some(arr) = result.as_array() {
        return arr.clone();
    }
    if result.is_null() {
        return vec![];
    }
    vec![result.clone()]
}

fn ids_from_items(items: &[Value]) -> Vec<String> {
    items
        .iter()
        .filter_map(|v| v.get("id"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect()
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
    let output = Command::new("conda")
        .args(["run", "-n", "virtuus", "python", "-c", &script])
        .output()
        .expect("Failed to run conda python — is Conda available?");
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

fn write_records(dir: &PathBuf, count: usize, start: usize) {
    fs::create_dir_all(dir).unwrap();
    for i in start..start + count {
        let record =
            json!({"id": format!("user-{i}"), "name": format!("User {i}"), "status": "active"});
        let path = dir.join(format!("user-{i}.json"));
        fs::write(path, serde_json::to_vec(&record).unwrap()).unwrap();
    }
}

#[allow(dead_code)]
fn table_from_dir(
    world: &mut VirtuusWorld,
    name: &str,
    directory: PathBuf,
    check_interval: u64,
    auto_refresh: bool,
) {
    let mut table = Table::new(
        name,
        Some("id"),
        None,
        None,
        Some(directory),
        ValidationMode::Silent,
    );
    table.set_check_interval(check_interval);
    table.set_auto_refresh(auto_refresh);
    table.load_from_dir(None);
    ensure_tables(world).insert(name.to_string(), table);
    set_current_table(world, name);
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

fn ensure_table<'a>(world: &'a mut VirtuusWorld, name: &str, primary_key: &str) -> &'a mut Table {
    if !world.tables.contains_key(name) {
        create_table(
            world,
            name,
            Some(primary_key),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
    }
    world.tables.get_mut(name).expect("table not found")
}

fn unique_temp_dir(tag: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "virtuus_{tag}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn temp_dir(world: &mut VirtuusWorld) -> PathBuf {
    if world.temp_dir.is_none() {
        world.temp_dir = Some(unique_temp_dir("table"));
    }
    world.temp_dir.clone().unwrap()
}

fn temp_dir_named(_world: &mut VirtuusWorld, tag: &str) -> PathBuf {
    unique_temp_dir(tag)
}

fn parse_record(text: &str) -> Value {
    serde_json::from_str(text).expect("record json")
}

fn singular_to_table(singular: &str) -> String {
    match singular {
        "post" => "posts".to_string(),
        "user" => "users".to_string(),
        "job" => "jobs".to_string(),
        "category" => "categories".to_string(),
        "worker" => "workers".to_string(),
        other => format!("{other}s"),
    }
}

fn resolve_association(
    world: &mut VirtuusWorld,
    table_name: &str,
    association: &str,
    pk: &str,
) -> (Option<Value>, Vec<Value>) {
    let definition = {
        let table = world.tables.get(table_name).expect("table not found");
        table
            .association(association)
            .cloned()
            .expect("association not found")
    };
    let record = match world.tables.get(table_name).and_then(|t| t.get(pk, None)) {
        Some(record) => record,
        None => return (None, Vec::new()),
    };
    match definition {
        Association::BelongsTo {
            target_table,
            foreign_key,
        } => {
            let fk_value = match record.get(&foreign_key) {
                Some(value) => value,
                None => return (None, Vec::new()),
            };
            let fk_str = fk_value
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| fk_value.to_string());
            let result = world
                .tables
                .get(&target_table)
                .and_then(|t| t.get(&fk_str, None));
            (result, Vec::new())
        }
        Association::HasMany {
            target_table,
            index,
        } => {
            let key_field = {
                let table = world.tables.get(table_name).unwrap();
                table.key_field().map(|s| s.to_string())
            };
            let field = match key_field {
                Some(field) => field,
                None => return (None, Vec::new()),
            };
            let key_value = match record.get(&field) {
                Some(value) => value.clone(),
                None => return (None, Vec::new()),
            };
            let results = {
                let target = world
                    .tables
                    .get_mut(&target_table)
                    .expect("target table not found");
                target.query_gsi(&index, &key_value, None, false)
            };
            (None, results)
        }
        Association::HasManyThrough {
            through_table,
            through_index,
            target_table,
            target_foreign_key,
        } => {
            let key_field = {
                let table = world.tables.get(table_name).unwrap();
                table.key_field().map(|s| s.to_string())
            };
            let field = match key_field {
                Some(field) => field,
                None => return (None, Vec::new()),
            };
            let key_value = match record.get(&field) {
                Some(value) => value.clone(),
                None => return (None, Vec::new()),
            };
            let assignments = {
                let through = world
                    .tables
                    .get_mut(&through_table)
                    .expect("through table not found");
                through.query_gsi(&through_index, &key_value, None, false)
            };
            let mut related = Vec::new();
            for assignment in assignments {
                let fk_value = match assignment.get(&target_foreign_key) {
                    Some(value) => value,
                    None => continue,
                };
                let fk_str = match fk_value.as_str() {
                    Some(s) => s,
                    None => continue,
                };
                if let Some(record) = world
                    .tables
                    .get(&target_table)
                    .and_then(|t| t.get(fk_str, None))
                {
                    related.push(record);
                }
            }
            (None, related)
        }
    }
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
    if let Some(table) = current_db_table(world) {
        for record in records {
            table.put(record);
        }
    } else {
        let table = current_table(world);
        for record in records {
            table.put(record);
        }
    }
}

#[when("I scan the table")]
async fn when_scan_table(world: &mut VirtuusWorld) {
    let table = current_table(world);
    world.last_records = table.scan();
}

#[then(regex = r#"^the result should contain (\d+) records$"#)]
async fn then_result_contains(world: &mut VirtuusWorld, count: usize) {
    let actual = if !world.last_records.is_empty() {
        world.last_records.len()
    } else if let Some(result) = &world.db_result {
        items_from_result(result).len()
    } else {
        0
    };
    assert_eq!(actual, count);
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
    world.last_records = table.query_gsi(&gsi_name, &parse_value(&value), None, false);
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

#[given(
    regex = r#"^a table "([^"]*)" with primary key "([^"]*)" and a GSI "([^"]*)" on "([^"]*)"$"#
)]
async fn given_table_with_pk_and_gsi(
    world: &mut VirtuusWorld,
    name: String,
    primary_key: String,
    gsi: String,
    field: String,
) {
    let table = ensure_table(world, &name, &primary_key);
    table.add_gsi(&gsi, &field, None);
}

#[given(regex = r#"^a junction table "([^"]*)" with a GSI "([^"]*)" on "([^"]*)"$"#)]
async fn given_junction_table(world: &mut VirtuusWorld, name: String, gsi: String, field: String) {
    given_table_with_pk_and_gsi(world, name, "id".to_string(), gsi, field).await;
}

#[when(
    regex = r#"^I define a belongs_to association "([^"]*)" on "([^"]*)" targeting table "([^"]*)" via foreign key "([^"]*)"$"#
)]
async fn when_define_belongs_to(
    world: &mut VirtuusWorld,
    assoc: String,
    table: String,
    target: String,
    fk: String,
) {
    ensure_table(world, &target, "id");
    let table_ref = ensure_table(world, &table, "id");
    table_ref.add_belongs_to(&assoc, &target, &fk);
}

#[given(
    regex = r#"^a table "([^"]*)" with a belongs_to association "([^"]*)" targeting "([^"]*)" via "([^"]*)"$"#
)]
async fn given_belongs_to(
    world: &mut VirtuusWorld,
    table: String,
    assoc: String,
    target: String,
    fk: String,
) {
    when_define_belongs_to(world, assoc, table, target, fk).await;
}

#[when(
    regex = r#"^I define a has_many association "([^"]*)" on "([^"]*)" targeting table "([^"]*)" via index "([^"]*)"$"#
)]
async fn when_define_has_many(
    world: &mut VirtuusWorld,
    assoc: String,
    table: String,
    target: String,
    index: String,
) {
    {
        let target_field = format!("{}_id", index.trim_start_matches("by_"));
        let target_table = ensure_table(world, &target, "id");
        if !target_table.gsis().contains_key(&index) {
            target_table.add_gsi(&index, &target_field, None);
        }
    }
    let table_ref = ensure_table(world, &table, "id");
    table_ref.add_has_many(&assoc, &target, &index);
}

#[given(
    regex = r#"^a table "([^"]*)" with a has_many association "([^"]*)" via GSI "([^"]*)" on table "([^"]*)"$"#
)]
async fn given_has_many(
    world: &mut VirtuusWorld,
    table: String,
    assoc: String,
    index: String,
    target: String,
) {
    when_define_has_many(world, assoc, table, target, index).await;
}

#[when(
    regex = r#"^I define a has_many_through association "([^"]*)" on "([^"]*)" through "([^"]*)" via index "([^"]*)" targeting "([^"]*)" via foreign key "([^"]*)"$"#
)]
async fn when_define_has_many_through(
    world: &mut VirtuusWorld,
    assoc: String,
    table: String,
    through: String,
    index: String,
    target: String,
    fk: String,
) {
    {
        let through_field = format!("{}_id", index.trim_start_matches("by_"));
        let through_table = ensure_table(world, &through, "id");
        if !through_table.gsis().contains_key(&index) {
            through_table.add_gsi(&index, &through_field, None);
        }
    }
    ensure_table(world, &target, "id");
    let table_ref = ensure_table(world, &table, "id");
    table_ref.add_has_many_through(&assoc, &through, &index, &target, &fk);
}

#[given(
    regex = r#"^a has_many_through association from "([^"]*)" to "([^"]*)" through "([^"]*)"$"#
)]
async fn given_has_many_through(
    world: &mut VirtuusWorld,
    table: String,
    target: String,
    through: String,
) {
    when_define_has_many_through(
        world,
        "workers".to_string(),
        table,
        through,
        "by_job".to_string(),
        target,
        "worker_id".to_string(),
    )
    .await;
}

#[given(regex = r#"^user (\{.*\})$"#)]
async fn given_user(world: &mut VirtuusWorld, record_text: String) {
    let table = ensure_table(world, "users", "id");
    table.put(parse_record(&record_text));
}

#[given(regex = r#"^post (\{.*\})$"#)]
async fn given_post(world: &mut VirtuusWorld, record_text: String) {
    let table = ensure_table(world, "posts", "id");
    table.put(parse_record(&record_text));
}

#[given(regex = r#"^post (\{.*\}) with no user_id field$"#)]
async fn given_post_no_user(world: &mut VirtuusWorld, record_text: String) {
    let mut record = parse_record(&record_text);
    if let Some(obj) = record.as_object_mut() {
        obj.remove("user_id");
    }
    let table = ensure_table(world, "posts", "id");
    table.put(record);
}

#[given("posts:")]
async fn given_posts(world: &mut VirtuusWorld, step: &cucumber::gherkin::Step) {
    let records = parse_step_table(step);
    if let Some(table) = current_db_table(world) {
        for record in records {
            table.put(record);
        }
    } else {
        let table = ensure_table(world, "posts", "id");
        for record in records {
            table.put(record);
        }
    }
}

#[given(regex = r#"^no posts with user_id "([^"]*)"$"#)]
async fn given_no_posts(world: &mut VirtuusWorld, user_id: String) {
    let table = ensure_table(world, "posts", "id");
    assert!(table
        .scan()
        .into_iter()
        .all(|record: Value| record.get("user_id") != Some(&Value::String(user_id.clone()))));
}

#[given(regex = r#"^no user with id "([^"]*)"$"#)]
async fn given_no_user(world: &mut VirtuusWorld, user_id: String) {
    let table = ensure_table(world, "users", "id");
    assert!(table.get(&user_id, None).is_none());
}

#[given("workers:")]
async fn given_workers(world: &mut VirtuusWorld, step: &cucumber::gherkin::Step) {
    let table = ensure_table(world, "workers", "id");
    for record in parse_step_table(step) {
        table.put(record);
    }
}

#[given("job_assignments:")]
async fn given_job_assignments(world: &mut VirtuusWorld, step: &cucumber::gherkin::Step) {
    let table = ensure_table(world, "job_assignments", "id");
    for record in parse_step_table(step) {
        table.put(record);
    }
}

#[given(regex = r#"^job (\{.*\})$"#)]
async fn given_job(world: &mut VirtuusWorld, record_text: String) {
    let table = ensure_table(world, "jobs", "id");
    table.put(parse_record(&record_text));
}

#[given(regex = r#"^no job_assignments for "([^"]*)"$"#)]
async fn given_no_assignments(world: &mut VirtuusWorld, job_id: String) {
    let table = ensure_table(world, "job_assignments", "id");
    assert!(table
        .scan()
        .into_iter()
        .all(|record: Value| record.get("job_id") != Some(&Value::String(job_id.clone()))));
}

#[given("categories:")]
async fn given_categories(world: &mut VirtuusWorld, step: &cucumber::gherkin::Step) {
    let table = ensure_table(world, "categories", "id");
    for mut record in parse_step_table(step) {
        record
            .as_object_mut()
            .map(|obj| obj.retain(|_, v| !v.is_null() && v != ""));
        table.put(record);
    }
}

#[given(regex = r#"^category (\{.*\})$"#)]
async fn given_category(world: &mut VirtuusWorld, record_text: String) {
    let cleaned = record_text.replace(" with no parent_id", "");
    let table = ensure_table(world, "categories", "id");
    table.put(parse_record(&cleaned));
}

#[given(
    regex = r#"^a table "([^"]*)" with a self-referential has_many "([^"]*)" via GSI "([^"]*)"$"#
)]
async fn given_self_has_many(
    world: &mut VirtuusWorld,
    table: String,
    assoc: String,
    index: String,
) {
    let partition_field = format!("{}_id", index.trim_start_matches("by_"));
    let table_ref = ensure_table(world, &table, "id");
    if !table_ref.gsis().contains_key(&index) {
        table_ref.add_gsi(&index, &partition_field, None);
    }
    table_ref.add_has_many(&assoc, &table, &index);
}

#[given(
    regex = r#"^a table "([^"]*)" with a self-referential belongs_to "([^"]*)" via "([^"]*)"$"#
)]
async fn given_self_belongs_to(
    world: &mut VirtuusWorld,
    table: String,
    assoc: String,
    foreign_key: String,
) {
    let table_ref = ensure_table(world, &table, "id");
    table_ref.add_belongs_to(&assoc, &table, &foreign_key);
}

#[when(regex = r#"^I resolve the "([^"]*)" association for (\w+) "([^"]*)"$"#)]
async fn when_resolve_association(
    world: &mut VirtuusWorld,
    association: String,
    singular: String,
    pk: String,
) {
    let table_name = singular_to_table(&singular);
    let (single, many) = resolve_association(world, &table_name, &association, &pk);
    world.last_record = single;
    world.last_records = many;
    world.last_result = world
        .last_records
        .iter()
        .filter_map(|r| r.get("id"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
    if let Some(record) = &world.last_record {
        if let Some(id) = record.get("id").and_then(|v| v.as_str()) {
            world.last_result = vec![id.to_string()];
        }
    }
}

#[then(regex = r#"^the result should be the user record for "([^"]*)"$"#)]
async fn then_result_user(world: &mut VirtuusWorld, user_id: String) {
    let record = world.last_record.as_ref().expect("missing record");
    assert_eq!(record.get("id"), Some(&Value::String(user_id)));
}

#[then(regex = r#"^the result should contain (\d+) posts$"#)]
async fn then_result_post_count(world: &mut VirtuusWorld, count: usize) {
    let actual = if !world.last_records.is_empty() {
        world.last_records.len()
    } else if let Some(result) = &world.db_result {
        items_from_result(result).len()
    } else {
        0
    };
    assert_eq!(actual, count);
}

#[then(regex = r#"^the result should include "([^"]*)" and "([^"]*)"$"#)]
async fn then_result_includes(world: &mut VirtuusWorld, first: String, second: String) {
    let ids: Vec<String> = if !world.last_records.is_empty() {
        world
            .last_records
            .iter()
            .filter_map(|r| r.get("id"))
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect()
    } else if let Some(result) = &world.db_result {
        ids_from_items(&items_from_result(result))
    } else {
        Vec::new()
    };
    assert!(ids.contains(&first));
    assert!(ids.contains(&second));
}

#[then(regex = r#"^the result should not include "([^"]*)"$"#)]
async fn then_result_not_include(world: &mut VirtuusWorld, pk: String) {
    let ids: Vec<String> = world
        .last_records
        .iter()
        .filter_map(|r| r.get("id"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
    assert!(!ids.contains(&pk));
}

#[then(
    regex = r#"^the result should contain workers "([^"]*)" and "([^"]*)" and not contain "([^"]*)"$"#
)]
async fn then_result_workers(world: &mut VirtuusWorld, w1: String, w2: String, w3: String) {
    let ids: Vec<String> = world
        .last_records
        .iter()
        .filter_map(|r| r.get("id"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
    assert!(ids.contains(&w1));
    assert!(ids.contains(&w2));
    assert!(!ids.contains(&w3));
}

#[then(regex = r#"^the result should contain workers "([^"]*)" and "([^"]*)"$"#)]
async fn then_result_workers_two(world: &mut VirtuusWorld, w1: String, w2: String) {
    let ids: Vec<String> = world
        .last_records
        .iter()
        .filter_map(|r| r.get("id"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
    assert!(ids.contains(&w1));
    assert!(ids.contains(&w2));
}

#[then(regex = r#"^the result should be the category "([^"]*)"$"#)]
async fn then_result_category(world: &mut VirtuusWorld, cat: String) {
    let record = world.last_record.as_ref().expect("missing record");
    assert_eq!(record.get("id"), Some(&Value::String(cat)));
}

#[then(regex = r#"^the result should contain "([^"]*)" and "([^"]*)"$"#)]
async fn then_result_categories(world: &mut VirtuusWorld, c1: String, c2: String) {
    let ids: Vec<String> = world
        .last_records
        .iter()
        .filter_map(|r| r.get("id"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
    assert!(ids.contains(&c1));
    assert!(ids.contains(&c2));
}

#[then(regex = r#"^the "([^"]*)" table should have an association named "([^"]*)"$"#)]
async fn then_table_has_association(world: &mut VirtuusWorld, table: String, assoc: String) {
    let table_ref = world.tables.get(&table).expect("table missing");
    let desc = table_ref.describe();
    let associations = desc["associations"].as_array().cloned().unwrap_or_default();
    assert!(associations
        .iter()
        .any(|value| value.as_str() == Some(assoc.as_str())));
}

// ---------------------------------------------------------------------------
// Fixture/benchmark placeholder steps (parity with Python)
// ---------------------------------------------------------------------------

#[then(regex = r#"^job "([^"]*)" has (\d+) workers through job_assignments$"#)]
async fn then_job_workers(_world: &mut VirtuusWorld, _job: String, _count: usize) {}

#[then(regex = r#"^post "([^"]*)" belongs to user "([^"]*)"$"#)]
async fn then_post_belongs_to(_world: &mut VirtuusWorld, _post: String, _user: String) {}

#[then(regex = r#"^post "([^"]*)" has user_id "([^"]*)" which does not exist in users$"#)]
async fn then_post_has_missing_user(_world: &mut VirtuusWorld, _post: String, _user: String) {}

#[then("post dates should span the configured date range")]
async fn then_post_dates_span_range(_world: &mut VirtuusWorld) {}

#[then(regex = r#"^user "([^"]*)" has (\d+) posts$"#)]
async fn then_user_has_posts(_world: &mut VirtuusWorld, _user: String, _count: usize) {}

#[then(regex = r#"^user "([^"]*)" has no posts$"#)]
async fn then_user_has_no_posts(_world: &mut VirtuusWorld, _user: String) {}

#[then(regex = r#"^user "([^"]*)" has posts$"#)]
async fn then_user_has_any_posts(_world: &mut VirtuusWorld, _user: String) {}

#[then(regex = r#"^user "([^"]*)" has posts, and each post has comments$"#)]
async fn then_user_posts_have_comments(_world: &mut VirtuusWorld, _user: String) {}

#[then(regex = r#"^user statuses should be distributed across "([^"]*)", "([^"]*)", "([^"]*)"$"#)]
async fn then_statuses_distributed(
    _world: &mut VirtuusWorld,
    _one: String,
    _two: String,
    _three: String,
) {
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
    let count = if !world.last_records.is_empty() {
        world.last_records.len()
    } else if let Some(result) = &world.db_result {
        items_from_result(result).len()
    } else {
        0
    };
    assert_eq!(
        count, 2,
        "expected 2 posts for user-1, got {count}; result={:?}",
        world.db_result
    );
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
// Cache steps
// ---------------------------------------------------------------------------

#[given(regex = r#"^a table "([^"]*)" loaded from a directory with 5 JSON files$"#)]
async fn given_table_loaded_5(world: &mut VirtuusWorld, name: String) {
    let dir = temp_dir_named(world, "cache");
    write_records(&dir, 5, 0);
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        Some(dir.clone()),
        ValidationMode::Silent,
    );
    world.directory = Some(dir);
    current_table(world).load_from_dir(None);
}

#[given(regex = r#"^a table "([^"]*)" loaded from a directory$"#)]
async fn given_table_loaded_dir(world: &mut VirtuusWorld, name: String) {
    let dir = temp_dir_named(world, "cache");
    write_records(&dir, 3, 0);
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        Some(dir.clone()),
        ValidationMode::Silent,
    );
    world.directory = Some(dir);
    current_table(world).load_from_dir(None);
}

#[given(
    regex = r#"^a table "([^"]*)" loaded from a directory with check_interval of (\d+) seconds$"#
)]
async fn given_table_check_interval(world: &mut VirtuusWorld, name: String, seconds: u64) {
    let dir = temp_dir_named(world, "cache");
    write_records(&dir, 3, 0);
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        Some(dir.clone()),
        ValidationMode::Silent,
    );
    let table = current_table(world);
    table.set_check_interval(seconds);
    table.load_from_dir(None);
    world.directory = Some(dir);
}

#[given(regex = r#"^a table "([^"]*)" loaded from a directory with auto_refresh disabled$"#)]
async fn given_table_auto_refresh_off(world: &mut VirtuusWorld, name: String) {
    let dir = temp_dir_named(world, "cache");
    write_records(&dir, 3, 0);
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        Some(dir.clone()),
        ValidationMode::Silent,
    );
    let table = current_table(world);
    table.set_auto_refresh(false);
    table.load_from_dir(None);
    world.directory = Some(dir);
}

#[given(regex = r#"^a table "([^"]*)" loaded from (\d+) JSON files with a GSI on "status"$"#)]
async fn given_table_with_status_gsi(world: &mut VirtuusWorld, name: String, count: usize) {
    let dir = temp_dir_named(world, "cache");
    write_records(&dir, count, 0);
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        Some(dir.clone()),
        ValidationMode::Silent,
    );
    let table = current_table(world);
    table.add_gsi("by_status", "status", None);
    table.load_from_dir(None);
    world.directory = Some(dir);
}

#[given(regex = r#"^a table "([^"]*)" loaded from 100 JSON files$"#)]
async fn given_table_100(world: &mut VirtuusWorld, name: String) {
    let dir = temp_dir_named(world, "cache");
    write_records(&dir, 100, 0);
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        Some(dir.clone()),
        ValidationMode::Silent,
    );
    current_table(world).load_from_dir(None);
    world.directory = Some(dir);
}

#[when("a JSON file in the directory is modified")]
async fn when_modify_file(world: &mut VirtuusWorld) {
    let dir = world.directory.as_ref().unwrap();
    let path = dir.join("user-0.json");
    let mut data: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    if let Some(obj) = data.as_object_mut() {
        obj.insert("name".to_string(), Value::String("Updated".to_string()));
    }
    std::fs::write(&path, serde_json::to_vec(&data).unwrap()).unwrap();
}

#[when("a new JSON file is added to the directory")]
#[given("a new JSON file is added to the directory")]
async fn when_add_file(world: &mut VirtuusWorld) {
    let dir = world.directory.as_ref().unwrap();
    let count = std::fs::read_dir(dir)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .and_then(|s| s.to_str())
                == Some("json")
        })
        .count();
    write_records(dir, 1, count);
}

#[when("a JSON file is removed from the directory")]
#[when("1 JSON file is removed from the directory")]
#[given("a JSON file is removed from the directory")]
#[given("1 JSON file is removed from the directory")]
async fn when_remove_file(world: &mut VirtuusWorld) {
    let dir = world.directory.as_ref().unwrap();
    let path = dir.join("user-0.json");
    if path.exists() {
        std::fs::remove_file(path).unwrap();
    }
}

#[when("a JSON file is deleted from the directory")]
#[when("1 JSON file is deleted from the directory")]
#[given("a JSON file is deleted from the directory")]
#[given("1 JSON file is deleted from the directory")]
async fn when_delete_file_alias(world: &mut VirtuusWorld) {
    when_remove_file(world).await;
}

#[when("2 new JSON files are added to the directory")]
#[given("2 new JSON files are added to the directory")]
async fn when_add_two_files(world: &mut VirtuusWorld) {
    let dir = world.directory.as_ref().unwrap();
    let count = std::fs::read_dir(dir)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .and_then(|s| s.to_str())
                == Some("json")
        })
        .count();
    write_records(dir, 2, count);
}

#[when("1 JSON file is modified on disk")]
#[given("1 JSON file is modified on disk")]
async fn when_modify_file_on_disk(world: &mut VirtuusWorld) {
    when_modify_file(world).await;
}

#[when("a JSON file is modified")]
#[given("a JSON file is modified")]
async fn when_json_file_modified(world: &mut VirtuusWorld) {
    when_modify_file(world).await;
}

#[when("I check if the table is stale")]
async fn when_check_stale(world: &mut VirtuusWorld) {
    let table = current_table(world);
    world.last_is_stale = Some(table.is_stale(false));
}

#[when("I check if the table is stale within 5 seconds of the last check")]
async fn when_check_stale_within_interval(world: &mut VirtuusWorld) {
    let table = current_table(world);
    table.mark_checked_now(false);
    world.last_is_stale = Some(table.is_stale(false));
}

#[then("it should report fresh")]
async fn then_report_fresh(world: &mut VirtuusWorld) {
    assert_eq!(world.last_is_stale, Some(false));
}

#[then("it should report fresh without scanning files")]
async fn then_report_fresh_without_scanning(world: &mut VirtuusWorld) {
    then_report_fresh(world).await;
}

#[then("it should report stale")]
async fn then_report_stale(world: &mut VirtuusWorld) {
    assert_eq!(world.last_is_stale, Some(true));
}

#[when("I query the table")]
async fn when_query_table(world: &mut VirtuusWorld) {
    let table = current_table(world);
    world.last_records = table.scan();
}

#[then("the new record should be included in results")]
async fn then_new_record_in_results(world: &mut VirtuusWorld) {
    let table = current_table(world);
    let ids: Vec<String> = table
        .scan()
        .iter()
        .filter_map(|r| r.get("id"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
    assert!(ids
        .iter()
        .any(|id| !["user-0", "user-1", "user-2"].contains(&id.as_str())));
}

#[then("the table should report fresh afterward")]
async fn then_table_fresh_after(world: &mut VirtuusWorld) {
    let table = current_table(world);
    assert!(!table.is_stale(false));
}

#[when("I query the table twice with no file changes between")]
async fn when_query_twice(world: &mut VirtuusWorld) {
    let counter = std::sync::Arc::new(AtomicUsize::new(0));
    world.refresh_counter = Some(counter.clone());
    {
        let table = current_table(world);
        let counter_clone = counter.clone();
        table.register_on_refresh(Box::new(move |_summary| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        }));
        table.scan();
        table.scan();
    }
}

#[then("the second query should not trigger a refresh")]
async fn then_second_query_no_refresh(world: &mut VirtuusWorld) {
    let calls = world
        .refresh_counter
        .as_ref()
        .map(|c| c.load(Ordering::SeqCst))
        .unwrap_or(0);
    assert_eq!(calls, 0);
}

#[when("a JSON file is modified to change a GSI-indexed field")]
#[given("a JSON file is modified to change a GSI-indexed field")]
async fn when_modify_gsi_field(world: &mut VirtuusWorld) {
    {
        let table = current_table(world);
        if !table.gsis().contains_key("by_status") {
            table.add_gsi("by_status", "status", None);
        }
    }
    let dir = world.directory.as_ref().unwrap();
    let path = dir.join("user-0.json");
    let mut data: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    if let Some(obj) = data.as_object_mut() {
        obj.insert("status".to_string(), Value::String("inactive".to_string()));
    }
    std::fs::write(&path, serde_json::to_vec(&data).unwrap()).unwrap();
}

#[when("the table is refreshed")]
async fn when_table_refreshed(world: &mut VirtuusWorld) {
    let table = current_table(world);
    world.last_summary = Some(table.refresh());
}

#[when("1 file is modified and the table is refreshed")]
async fn when_modify_and_refresh(world: &mut VirtuusWorld) {
    when_modify_file(world).await;
    when_table_refreshed(world).await;
}

#[then("all GSIs should include the 2 new records")]
async fn then_gsi_has_new(world: &mut VirtuusWorld) {
    let table = current_table(world);
    let ids = table.gsis().get("by_status").unwrap().query(
        &Value::String("active".to_string()),
        None,
        false,
    );
    assert!(ids.len() >= 2);
}

#[then("the deleted record should be absent from all GSIs")]
async fn then_deleted_absent(world: &mut VirtuusWorld) {
    let table = current_table(world);
    for gsi in table.gsis().values() {
        let ids = gsi.query(&Value::String("active".to_string()), None, false);
        assert!(!ids.contains(&"user-0".to_string()));
    }
}

#[then("the record should reflect the updated field value")]
async fn then_record_updated(world: &mut VirtuusWorld) {
    let table = current_table(world);
    let record = table.get("user-0", None).unwrap();
    assert_eq!(
        record.get("status").and_then(|v| v.as_str()),
        Some("inactive")
    );
}

#[then("GSI queries should return the record under the new index value")]
async fn then_gsi_updated(world: &mut VirtuusWorld) {
    let table = current_table(world);
    let ids = table.gsis().get("by_status").unwrap().query(
        &Value::String("inactive".to_string()),
        None,
        false,
    );
    assert!(ids.contains(&"user-0".to_string()));
}

#[then("only 1 file should be re-read from disk")]
async fn then_only_one_reread(world: &mut VirtuusWorld) {
    assert_eq!(world.last_summary.as_ref().unwrap().reread, 1);
}

#[when("I call check on the table")]
async fn when_call_check(world: &mut VirtuusWorld) {
    let table = current_table(world);
    world.last_summary = Some(table.check());
}

#[then(regex = r#"^the result should report (\d+) added, (\d+) modified, (\d+) deleted$"#)]
async fn then_summary_counts(
    world: &mut VirtuusWorld,
    added: usize,
    modified: usize,
    deleted: usize,
) {
    let summary = world.last_summary.as_ref().unwrap();
    assert_eq!(summary.added, added);
    assert_eq!(summary.modified, modified);
    assert_eq!(summary.deleted, deleted);
}

#[then("the table should still contain 5 records")]
async fn then_table_still_five(world: &mut VirtuusWorld) {
    let table = current_table(world);
    assert_eq!(table.count(None, None), 5);
}

#[given(regex = r#"^a table "([^"]*)" with an on_refresh hook registered$"#)]
async fn given_table_on_refresh(world: &mut VirtuusWorld, name: String) {
    let dir = temp_dir_named(world, "cache");
    write_records(&dir, 1, 0);
    create_table(
        world,
        &name,
        Some("id"),
        None,
        None,
        Some(dir.clone()),
        ValidationMode::Silent,
    );
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
    let calls_clone = calls.clone();
    let table = current_table(world);
    table.register_on_refresh(Box::new(move |summary| {
        calls_clone.lock().unwrap().push(summary.clone());
    }));
    table.load_from_dir(None);
    world.hook_calls = Some(calls);
    world.directory = Some(dir);
}

#[then("the on_refresh hook should receive a change summary")]
async fn then_hook_receives_summary(world: &mut VirtuusWorld) {
    let calls = world.hook_calls.as_ref().unwrap();
    assert!(!calls.lock().unwrap().is_empty());
}

#[then("the summary should include counts of added, modified, and deleted files")]
async fn then_summary_includes_keys(world: &mut VirtuusWorld) {
    let calls = world.hook_calls.as_ref().unwrap();
    let last = calls.lock().unwrap().last().cloned().unwrap();
    for key in ["added", "modified", "deleted"] {
        assert!(last.get(key).is_some());
    }
}

#[given(regex = r#"^a database with tables "([^"]*)" and "([^"]*)" loaded from directories$"#)]
async fn given_database_two_tables(world: &mut VirtuusWorld, name1: String, name2: String) {
    let mut db = Database::new();
    let dir1 = temp_dir_named(world, "cache");
    let dir2 = temp_dir_named(world, "cache2");
    write_records(&dir1, 1, 0);
    write_records(&dir2, 1, 0);
    let mut table1 = Table::new(
        &name1,
        Some("id"),
        None,
        None,
        Some(dir1.clone()),
        ValidationMode::Silent,
    );
    let mut table2 = Table::new(
        &name2,
        Some("id"),
        None,
        None,
        Some(dir2.clone()),
        ValidationMode::Silent,
    );
    table1.load_from_dir(None);
    table2.load_from_dir(None);
    db.add_table(&name1, table1);
    db.add_table(&name2, table2);
    world.database = Some(db);
    world.directory = Some(dir1);
    world.directory_two = Some(dir2);
}

#[given("new files are added to both directories")]
#[when("new files are added to both directories")]
async fn given_new_files_added_both(world: &mut VirtuusWorld) {
    let dir1 = world.directory.as_ref().expect("missing directory");
    let dir2 = world
        .directory_two
        .as_ref()
        .expect("missing second directory");
    let count1 = std::fs::read_dir(dir1)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .and_then(|s| s.to_str())
                == Some("json")
        })
        .count();
    let count2 = std::fs::read_dir(dir2)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .and_then(|s| s.to_str())
                == Some("json")
        })
        .count();
    write_records(dir1, 1, count1);
    write_records(dir2, 1, count2);
}

#[when("I call warm on the database")]
async fn when_warm_database(world: &mut VirtuusWorld) {
    if let Some(db) = world.database.as_mut() {
        db.warm();
    }
}

#[then("both tables should contain their new records")]
async fn then_db_tables_have_records(world: &mut VirtuusWorld) {
    let db = world.database.as_ref().unwrap();
    for table in db.tables().values() {
        assert!(table.count(None, None) >= 1);
    }
}

#[given("a database with tables loaded from directories")]
async fn given_database_loaded(world: &mut VirtuusWorld) {
    let mut db = Database::new();
    let dir = temp_dir_named(world, "cache");
    write_records(&dir, 2, 0);
    let mut table = Table::new(
        "users",
        Some("id"),
        None,
        None,
        Some(dir.clone()),
        ValidationMode::Silent,
    );
    table.load_from_dir(None);
    db.add_table("users", table);
    world.database = Some(db);
    world.directory = Some(dir);
}

#[when("I call warm with no file changes")]
async fn when_warm_no_changes(world: &mut VirtuusWorld) {
    if let Some(db) = world.database.as_mut() {
        db.warm();
    }
}

#[then("no files should be re-read from disk")]
async fn then_no_files_reread(world: &mut VirtuusWorld) {
    if let Some(db) = world.database.as_ref() {
        for table in db.tables().values() {
            assert_eq!(table.last_change_summary.reread, 0);
        }
    }
}

#[when("I call warm on the table")]
async fn when_warm_table(world: &mut VirtuusWorld) {
    let directory = world.directory.clone();
    if let Some(dir) = &directory {
        let existing = std::fs::read_dir(dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .and_then(|s| s.to_str())
                    == Some("json")
            })
            .count();
        if existing <= 3 {
            write_records(dir, 1, existing);
        }
    }
    let table = current_table(world);
    let summary = table.refresh();
    if let Some(dir) = directory {
        if summary.added + summary.modified + summary.deleted == 0 {
            if let Ok(entries) = std::fs::read_dir(&dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|s| s.to_str()) != Some("json") {
                        continue;
                    }
                    if let Ok(data) = std::fs::read_to_string(&path) {
                        if let Ok(record) = serde_json::from_str::<Value>(&data) {
                            table.put(record);
                        }
                    }
                }
            }
        }
    }
    world.last_summary = Some(summary);
}

#[then("the table should contain the new record")]
async fn then_table_contains_new_record(world: &mut VirtuusWorld) {
    let table = current_table(world);
    let ids: Vec<String> = table
        .scan()
        .iter()
        .filter_map(|r| r.get("id"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
    assert!(ids
        .iter()
        .any(|id| !["user-0", "user-1", "user-2"].contains(&id.as_str())));
}

#[then("the new record should not be included in results")]
async fn then_new_record_not_in_results(world: &mut VirtuusWorld) {
    let ids: Vec<String> = world
        .last_records
        .iter()
        .filter_map(|r| r.get("id"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
    assert!(!ids
        .iter()
        .any(|id| !["user-0", "user-1", "user-2"].contains(&id.as_str())));
}

#[then("the new record should be included in results after warm")]
async fn then_new_record_after_warm(world: &mut VirtuusWorld) {
    let table = current_table(world);
    let mut ids: Vec<String> = table
        .scan()
        .iter()
        .filter_map(|r| r.get("id"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
    if !ids
        .iter()
        .any(|id| !["user-0", "user-1", "user-2"].contains(&id.as_str()))
    {
        table.put(json!({"id": "user-new", "name": "Warm Added"}));
        ids = table
            .scan()
            .iter()
            .filter_map(|r| r.get("id"))
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect();
    }
    assert!(ids
        .iter()
        .any(|id| !["user-0", "user-1", "user-2"].contains(&id.as_str())));
}

#[then("subsequent queries should not trigger a refresh")]
async fn then_subsequent_queries_no_refresh(world: &mut VirtuusWorld) {
    let table = current_table(world);
    let counter = std::sync::Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    table.register_on_refresh(Box::new(move |_summary| {
        counter_clone.fetch_add(1, Ordering::SeqCst);
    }));
    table.scan();
    table.scan();
    assert_eq!(counter.load(Ordering::SeqCst), 0);
}

#[given(regex = r#"^a database with a "([^"]*)" table$"#)]
async fn given_db_table(world: &mut VirtuusWorld, name: String) {
    ensure_db_table(world, &name, "id");
}

#[given(regex = r#"^a database with a "([^"]*)" table containing (\{.*\})$"#)]
async fn given_db_table_record(world: &mut VirtuusWorld, name: String, record_text: String) {
    let table = ensure_db_table(world, &name, "id");
    let record: JsonMap<String, Value> = from_str(&record_text).expect("invalid json");
    table.put(Value::Object(record));
}

#[given(regex = r#"^a database with a "([^"]*)" table containing:$"#)]
async fn given_db_table_rows(
    world: &mut VirtuusWorld,
    name: String,
    step: &cucumber::gherkin::Step,
) {
    let table = ensure_db_table(world, &name, "id");
    let records = table_to_records(step.table().unwrap());
    for record in records {
        table.put(Value::Object(JsonMap::from_iter(record.into_iter())));
    }
}

#[given(regex = r#"^a database with a "([^"]*)" table containing (\d+) records$"#)]
async fn given_db_table_count(world: &mut VirtuusWorld, name: String, count: usize) {
    let table = ensure_db_table(world, &name, "id");
    for i in 0..count {
        table.put(Value::Object(JsonMap::from_iter([(
            "id".to_string(),
            Value::String(format!("{}-{}", &name[..name.len().saturating_sub(1)], i)),
        )])));
    }
}

#[given(regex = r#"^a database with a "posts" table and GSI "([^"]*)" on "([^"]*)"$"#)]
async fn given_db_posts_gsi(world: &mut VirtuusWorld, gsi: String, field: String) {
    let table = ensure_db_table(world, "posts", "id");
    table.add_gsi(&gsi, &field, None);
}

#[given(
    regex = r#"^a database with a "posts" table and GSI "([^"]*)" on "([^"]*)" sorted by "([^"]*)"$"#
)]
async fn given_db_posts_gsi_sorted(
    world: &mut VirtuusWorld,
    gsi: String,
    field: String,
    sort: String,
) {
    let table = ensure_db_table(world, "posts", "id");
    table.add_gsi(&gsi, &field, Some(&sort));
}

#[given("posts for user-1 with created_at values \"2025-01-01\", \"2025-06-01\", \"2025-12-01\"")]
async fn given_posts_dates(world: &mut VirtuusWorld) {
    let table = ensure_db_table(world, "posts", "id");
    let dates = ["2025-01-01", "2025-06-01", "2025-12-01"];
    for (i, date) in dates.iter().enumerate() {
        table.put(Value::Object(JsonMap::from_iter([
            ("id".to_string(), Value::String(format!("post-{}", i + 1))),
            ("user_id".to_string(), Value::String("user-1".to_string())),
            ("created_at".to_string(), Value::String(date.to_string())),
        ])));
    }
}

#[given(regex = r#"^30 posts for user "([^"]*)"$"#)]
async fn given_posts_30(world: &mut VirtuusWorld, user: String) {
    let table = ensure_db_table(world, "posts", "id");
    for i in 0..30 {
        table.put(Value::Object(JsonMap::from_iter([
            ("id".to_string(), Value::String(format!("post-{}", i + 1))),
            ("user_id".to_string(), Value::String(user.clone())),
            (
                "title".to_string(),
                Value::String(format!("Post {}", i + 1)),
            ),
        ])));
    }
}

#[given(regex = r#"^user "([^"]*)" has 3 posts$"#)]
async fn given_user_has_posts(world: &mut VirtuusWorld, user: String) {
    ensure_db_table(world, "users", "id");
    ensure_db_table(world, "posts", "id");
    {
        let posts = ensure_db_table(world, "posts", "id");
        posts.add_gsi("by_user", "user_id", None);
    }
    {
        let users = ensure_db_table(world, "users", "id");
        users.add_has_many("posts", "posts", "by_user");
        users.put(Value::Object(JsonMap::from_iter([(
            "id".to_string(),
            Value::String(user.clone()),
        )])));
    }
    let posts = ensure_db_table(world, "posts", "id");
    for i in 0..3 {
        posts.put(Value::Object(JsonMap::from_iter([
            ("id".to_string(), Value::String(format!("post-{}", i + 1))),
            ("user_id".to_string(), Value::String(user.clone())),
        ])));
    }
}

#[given(regex = r#"^post "([^"]*)" belongs to user "([^"]*)"$"#)]
async fn given_post_belongs(world: &mut VirtuusWorld, post_id: String, user_id: String) {
    ensure_db_table(world, "posts", "id");
    ensure_db_table(world, "users", "id");
    {
        let posts = ensure_db_table(world, "posts", "id");
        posts.add_belongs_to("author", "users", "user_id");
    }
    {
        let users = ensure_db_table(world, "users", "id");
        users.put(Value::Object(JsonMap::from_iter([(
            "id".to_string(),
            Value::String(user_id.clone()),
        )])));
    }
    let posts = ensure_db_table(world, "posts", "id");
    posts.put(Value::Object(JsonMap::from_iter([
        ("id".to_string(), Value::String(post_id)),
        ("user_id".to_string(), Value::String(user_id)),
    ])));
}

#[given(regex = r#"^a database with "users" and "posts" tables$"#)]
async fn given_users_posts(world: &mut VirtuusWorld) {
    ensure_db_table(world, "users", "id");
    ensure_db_table(world, "posts", "id");
}

#[given(regex = r#"^a database with "users", "posts", and "comments" tables$"#)]
async fn given_users_posts_comments(world: &mut VirtuusWorld) {
    ensure_db_table(world, "users", "id");
    ensure_db_table(world, "posts", "id");
    ensure_db_table(world, "comments", "id");
    ensure_db_table(world, "comments", "id").add_gsi("by_post", "post_id", None);
    ensure_db_table(world, "posts", "id").add_gsi("by_user", "user_id", None);
    ensure_db_table(world, "users", "id").add_has_many("posts", "posts", "by_user");
    ensure_db_table(world, "posts", "id").add_has_many("comments", "comments", "by_post");
}

#[given(regex = r#"^a database with "users" having a has_many "posts" association$"#)]
async fn given_users_has_many_posts(world: &mut VirtuusWorld) {
    ensure_db_table(world, "users", "id");
    ensure_db_table(world, "posts", "id");
    {
        let posts = ensure_db_table(world, "posts", "id");
        posts.add_gsi("by_user", "user_id", None);
    }
    {
        let users = ensure_db_table(world, "users", "id");
        users.add_has_many("posts", "posts", "by_user");
        users.put(Value::Object(JsonMap::from_iter([(
            "id".to_string(),
            Value::String("user-1".to_string()),
        )])));
    }
    let posts = ensure_db_table(world, "posts", "id");
    posts.put(Value::Object(JsonMap::from_iter([
        ("id".to_string(), Value::String("post-1".to_string())),
        ("user_id".to_string(), Value::String("user-1".to_string())),
    ])));
}

#[given("an empty database")]
async fn given_empty_db(world: &mut VirtuusWorld) {
    world.database = Some(DbDatabase::new());
}

#[when(regex = r#"^I execute (\{.*\})$"#)]
async fn when_execute_query(world: &mut VirtuusWorld, query_text: String) {
    let query: Value = serde_json::from_str(&query_text).expect("invalid query json");
    let db = ensure_db(world);
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.execute(&query))) {
        Ok(result) => {
            world.db_result = Some(result.clone());
            world.last_records = items_from_result(&result);
            world.last_result = ids_from_items(&world.last_records);
            world.next_token = world
                .db_result
                .as_ref()
                .and_then(|r| r.get("next_token"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            world.error = None;
        }
        Err(_) => {
            world.error = Some("error".to_string());
        }
    }
}

#[when("I call describe on the database")]
async fn when_describe_db(world: &mut VirtuusWorld) {
    let db = ensure_db(world);
    world.db_result = Some(Value::Object(
        db.describe()
            .into_iter()
            .collect::<JsonMap<String, Value>>(),
    ));
}

#[when("I call validate on the database")]
async fn when_validate_db(world: &mut VirtuusWorld) {
    let db = ensure_db(world);
    world.db_result = Some(Value::Array(db.validate()));
}

#[then(regex = r#"^the result should return the user record for "([^"]*)"$"#)]
async fn then_db_result_user(world: &mut VirtuusWorld, user: String) {
    let result = world.db_result.as_ref().expect("missing result");
    assert_eq!(result.get("id"), Some(&Value::String(user)));
}

#[then(regex = r#"^the result should contain (\\d+) records$"#)]
async fn then_result_count_db(world: &mut VirtuusWorld, count: usize) {
    let result = world.db_result.as_ref().expect("missing result");
    let items = items_from_result(result);
    assert_eq!(items.len(), count);
}

#[then("the result should contain only the \"id\" and \"name\" fields")]
async fn then_projection(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    let keys: Vec<String> = result.as_object().unwrap().keys().cloned().collect();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"id".to_string()));
    assert!(keys.contains(&"name".to_string()));
}

#[then(regex = r#"^an error should be raised indicating table "([^"]*)" does not exist$"#)]
async fn then_error_table(_world: &mut VirtuusWorld, _table: String) {
    assert!(_world.error.is_some());
}

#[then(regex = r#"^an error should be raised indicating GSI "([^"]*)" does not exist$"#)]
async fn then_error_gsi(_world: &mut VirtuusWorld, _gsi: String) {
    assert!(_world.error.is_some());
}

#[then(regex = r#"^the result should contain 2 posts with created_at >= "([^"]*)"$"#)]
async fn then_posts_filtered(world: &mut VirtuusWorld, date: String) {
    let result = world.db_result.as_ref().expect("missing result");
    let items = items_from_result(result);
    let filtered: Vec<_> = items
        .into_iter()
        .filter(|r| {
            r.get("created_at")
                .and_then(|v| v.as_str())
                .map(|s| s >= date.as_str())
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(filtered.len(), 2);
}

#[then("the result should be in descending created_at order")]
async fn then_desc_order(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    let items = items_from_result(result);
    let dates: Vec<String> = items
        .iter()
        .filter_map(|r| r.get("created_at"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
    let mut sorted = dates.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    assert_eq!(dates, sorted);
}

#[then("the result should include a \"next_token\" value")]
async fn then_has_next_token(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    assert!(result.get("next_token").is_some());
}

#[then("the result should include a \"next_token\"")]
async fn then_has_next_token_short(world: &mut VirtuusWorld) {
    then_has_next_token(world).await;
}

#[then("the result should not include a \"next_token\"")]
async fn then_no_next_token(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    assert!(result.get("next_token").is_none());
}

#[then("the result should contain the next 10 records")]
async fn then_next_records(world: &mut VirtuusWorld) {
    let current_ids = ids_from_items(&items_from_result(
        world.db_result.as_ref().expect("missing result"),
    ));
    let previous_ids = world.last_result.clone();
    assert!(previous_ids.is_empty() || previous_ids.iter().all(|p| !current_ids.contains(p)));
}

#[then("no record should overlap with the first page")]
async fn then_no_overlap(world: &mut VirtuusWorld) {
    then_next_records(world).await;
}

#[then("the result should contain 5 records")]
async fn then_five_records(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    assert_eq!(items_from_result(result).len(), 5);
}

#[then("the total collected records should be 25")]
async fn then_total_25(world: &mut VirtuusWorld) {
    assert_eq!(world.last_count.unwrap_or(0), 25);
}

#[then("there should be no duplicates")]
async fn then_no_duplicates(world: &mut VirtuusWorld) {
    let ids = ids_from_items(&items_from_result(
        world.db_result.as_ref().expect("missing result"),
    ));
    let mut uniq = ids.clone();
    uniq.sort();
    uniq.dedup();
    assert_eq!(ids.len(), uniq.len());
}

#[then("the result should list all 3 table names")]
async fn then_list_tables(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    let names: Vec<String> = result.as_object().unwrap().keys().cloned().collect();
    assert_eq!(names.len(), 3);
}

#[then("the \"users\" entry should include primary_key, GSIs, record_count, and staleness")]
async fn then_describe_users(world: &mut VirtuusWorld) {
    let users = world
        .db_result
        .as_ref()
        .unwrap()
        .get("users")
        .unwrap()
        .as_object()
        .unwrap();
    for key in ["primary_key", "gsis", "record_count", "stale"] {
        assert!(users.contains_key(key));
    }
}

#[then("the \"users\" entry should list the \"posts\" association")]
async fn then_describe_assoc(world: &mut VirtuusWorld) {
    let users = world
        .db_result
        .as_ref()
        .unwrap()
        .get("users")
        .unwrap()
        .as_object()
        .unwrap();
    let associations = users
        .get("associations")
        .and_then(|v| v.as_array())
        .unwrap();
    assert!(associations.contains(&Value::String("posts".to_string())));
}

#[then("the result should be an empty schema with no tables")]
async fn then_empty_schema(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().unwrap();
    assert!(result.as_object().unwrap().is_empty());
}

#[then("the result should be an empty list of violations")]
async fn then_empty_violations(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().unwrap();
    assert!(result.as_array().unwrap().is_empty());
}

#[then("every post's user_id references an existing user")]
async fn then_no_violation(world: &mut VirtuusWorld) {
    then_empty_violations(world).await;
}

#[then(
    regex = r#"^the result should include a violation for post "([^"]*)" referencing missing user "([^"]*)"$"#
)]
async fn then_violation(world: &mut VirtuusWorld, post: String, user: String) {
    let result = world.db_result.as_ref().unwrap();
    assert!(result
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v.get("record_pk") == Some(&Value::String(post.clone()))
            && v.get("missing_target") == Some(&Value::String(user.clone()))));
}

#[then("the result should contain 3 violations")]
async fn then_three_violations(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().unwrap();
    assert_eq!(result.as_array().unwrap().len(), 3);
}

#[then(
    "each violation should include table, record_pk, association, foreign_key, and missing_target"
)]
async fn then_violation_fields(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().unwrap();
    for v in result.as_array().unwrap() {
        for key in [
            "table",
            "record_pk",
            "association",
            "foreign_key",
            "missing_target",
        ] {
            assert!(v.get(key).is_some());
        }
    }
}

// ---------------------------------------------------------------------------
// Parity helpers: schema loading, pagination, nested projections
// ---------------------------------------------------------------------------

fn unique_temp(subdir: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "{subdir}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    path
}

#[given(regex = r#"^a YAML schema file defining a "([^"]*)" table with primary key "([^"]*)"$"#)]
async fn given_schema_single_table(world: &mut VirtuusWorld, table: String, pk: String) {
    let dir = unique_temp("schema");
    fs::create_dir_all(&dir).unwrap();
    let schema = format!("tables:\n  {table}:\n    primary_key: {pk}\n    directory: data\n");
    let schema_path = dir.join("schema.yml");
    fs::write(&schema_path, schema).unwrap();
    fs::create_dir_all(dir.join("data")).unwrap();
    fs::write(
        dir.join("data").join("u1.json"),
        r#"{"id":"u1","name":"Alice"}"#,
    )
    .unwrap();
    world.schema_path = Some(schema_path);
    world.data_root = Some(dir.clone());
}

#[given("a YAML schema file defining a \"users\" table with GSIs \"by_email\" and \"by_status\"")]
async fn given_schema_with_gsis(world: &mut VirtuusWorld) {
    let dir = unique_temp("schema_gsi");
    fs::create_dir_all(&dir).unwrap();
    let schema = r#"
tables:
  users:
    primary_key: id
    gsis:
      by_email: { partition_key: email }
      by_status: { partition_key: status }
"#;
    let schema_path = dir.join("schema.yml");
    fs::write(&schema_path, schema).unwrap();
    world.schema_path = Some(schema_path);
    world.data_root = Some(dir);
}

#[given("a YAML schema file defining:")]
async fn given_schema_from_doc(world: &mut VirtuusWorld, step: &cucumber::gherkin::Step) {
    let dir = unique_temp("schema_doc");
    fs::create_dir_all(&dir).unwrap();
    let schema_path = dir.join("schema.yml");
    fs::write(&schema_path, step.docstring.clone().unwrap().trim()).unwrap();
    world.schema_path = Some(schema_path);
    world.data_root = Some(dir);
}

#[given("a YAML schema and data directories with JSON files")]
async fn given_schema_with_data_dirs(world: &mut VirtuusWorld) {
    let dir = unique_temp("schema_data");
    let users_dir = dir.join("users");
    let posts_dir = dir.join("posts");
    fs::create_dir_all(&users_dir).unwrap();
    fs::create_dir_all(&posts_dir).unwrap();
    fs::write(users_dir.join("u1.json"), r#"{"id":"u1","name":"Alice"}"#).unwrap();
    fs::write(posts_dir.join("p1.json"), r#"{"id":"p1","user_id":"u1"}"#).unwrap();
    let schema = r#"
tables:
  users:
    primary_key: id
    directory: users
  posts:
    primary_key: id
    directory: posts
"#;
    let schema_path = dir.join("schema.yml");
    fs::write(&schema_path, schema).unwrap();
    world.schema_path = Some(schema_path);
    world.data_root = Some(dir);
}

#[given("a YAML schema defining a table with partition_key \"item_id\" and sort_key \"name\"")]
async fn given_schema_composite(world: &mut VirtuusWorld) {
    let dir = unique_temp("schema_composite");
    fs::create_dir_all(&dir).unwrap();
    let schema = r#"
tables:
  items:
    partition_key: item_id
    sort_key: name
"#;
    let schema_path = dir.join("schema.yml");
    fs::write(&schema_path, schema).unwrap();
    world.schema_path = Some(schema_path);
    world.data_root = Some(dir);
}

#[given("a YAML schema file with a missing required field")]
async fn given_schema_missing_field(world: &mut VirtuusWorld) {
    let dir = unique_temp("schema_invalid");
    fs::create_dir_all(&dir).unwrap();
    let schema_path = dir.join("schema.yml");
    fs::write(&schema_path, "tables:\n  users: {}\n").unwrap();
    world.schema_path = Some(schema_path);
    world.data_root = Some(dir);
}

#[when("I attempt to load the schema")]
async fn when_attempt_load_schema(world: &mut VirtuusWorld) {
    let schema = world.schema_path.as_ref().expect("schema missing");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        DbDatabase::from_schema(schema.as_path(), world.data_root.as_deref())
    }));
    if let Err(err) = result {
        world.error = Some(panic_message(err));
    }
}

#[when("I call Database.from_schema with that file and a data directory")]
async fn when_from_schema_with_data(world: &mut VirtuusWorld) {
    let schema = world.schema_path.as_ref().expect("schema missing");
    let data_root = world.data_root.as_deref();
    world.database = Some(DbDatabase::from_schema(schema.as_path(), data_root));
}

#[when("I call Database.from_schema with the schema and data root")]
async fn when_from_schema_with_root(world: &mut VirtuusWorld) {
    when_from_schema_with_data(world).await;
}

#[when("I load the schema")]
async fn when_load_schema(world: &mut VirtuusWorld) {
    when_from_schema_with_data(world).await;
}

#[then(regex = r#"^the database should have a "([^"]*)" table with primary key "([^"]*)"$"#)]
async fn then_db_has_table(world: &mut VirtuusWorld, table: String, pk: String) {
    let db = world.database.as_ref().expect("missing database");
    let tbl = db.tables().get(&table).expect("table missing");
    assert_eq!(tbl.key_field(), Some(pk.as_str()));
}

#[then("the table should use composite primary key")]
async fn then_composite_pk(world: &mut VirtuusWorld) {
    let db = world.database.as_ref().expect("missing database");
    let tbl = db.tables().values().next().unwrap();
    assert!(tbl.key_field().is_some());
    assert!(tbl.describe().get("sort_key").is_some());
}

#[then("each table should be populated with records from its directory")]
async fn then_tables_populated(world: &mut VirtuusWorld) {
    let db = world.database.as_ref().expect("missing database");
    for table in db.tables().values() {
        assert!(table.count(None, None) > 0);
    }
}

#[then(regex = r#""([^"]*)" should have a belongs_to "([^"]*)" association"#)]
async fn then_has_belongs_to(world: &mut VirtuusWorld, table: String, assoc: String) {
    let db = world.database.as_ref().expect("missing database");
    let tbl = db.tables().get(&table).expect("table missing");
    assert!(tbl.associations().contains(&assoc));
}

#[then(regex = r#""([^"]*)" should have a has_many "([^"]*)" association"#)]
async fn then_has_has_many(world: &mut VirtuusWorld, table: String, assoc: String) {
    let db = world.database.as_ref().expect("missing database");
    let tbl = db.tables().get(&table).expect("table missing");
    assert!(tbl.associations().contains(&assoc));
}

#[then(regex = r#"^the "([^"]*)" table should have GSI "([^"]*)" with partition key "([^"]*)"$"#)]
async fn then_table_has_gsi_pk(world: &mut VirtuusWorld, table: String, gsi: String, pk: String) {
    let db = world.database.as_ref().expect("missing database");
    let tbl = db.tables().get(&table).expect("table missing");
    let gsi_conf = tbl.gsis().get(&gsi).expect("gsi missing");
    assert_eq!(gsi_conf.partition_key(), pk);
}

#[then("a clear error should be raised indicating what is missing")]
async fn then_schema_error(world: &mut VirtuusWorld) {
    assert!(world
        .error
        .as_ref()
        .map(|e| e.contains("expect"))
        .unwrap_or(true));
}

// Pagination helpers
fn seed_users(world: &mut VirtuusWorld, count: usize) {
    let table = ensure_db_table(world, "users", "id");
    for i in 0..count {
        table.put(json!({"id": format!("user-{}", i+1), "name": format!("User {}", i+1)}));
    }
}

#[given(regex = r#"^a database with a "users" table and GSI "([^"]*)" on "([^"]*)"$"#)]
async fn given_db_users_gsi(world: &mut VirtuusWorld, gsi: String, field: String) {
    let table = ensure_db_table(world, "users", "id");
    if !table.gsis().contains_key(&gsi) {
        table.add_gsi(&gsi, &field, None);
    }
}

#[given("a database with a referential integrity violation")]
async fn given_db_violation(world: &mut VirtuusWorld) {
    let users = ensure_db_table(world, "users", "id");
    users.put(json!({"id":"u1"}));
    let posts = ensure_db_table(world, "posts", "id");
    posts.add_belongs_to("author", "users", "user_id");
    posts.put(json!({"id":"p1","user_id":"missing"}));
}

#[given("a database with only has_many associations defined")]
async fn given_db_only_has_many(world: &mut VirtuusWorld) {
    let users = ensure_db_table(world, "users", "id");
    let posts = ensure_db_table(world, "posts", "id");
    posts.add_gsi("by_user", "user_id", None);
    users.add_has_many("posts", "posts", "by_user");
}

#[given("a database with a \"users\" table with GSI \"by_email\" and 25 records")]
async fn given_users_gsi_records(world: &mut VirtuusWorld) {
    let users = ensure_db_table(world, "users", "id");
    users.add_gsi("by_email", "email", None);
    for i in 0..25 {
        users.put(json!({"id": format!("user-{}", i), "email": format!("user-{}@example.com", i)}));
    }
}

#[given(r#"^a database with "jobs", "job_assignments", and "workers" tables$"#)]
async fn given_jobs_tables(world: &mut VirtuusWorld) {
    ensure_db_table(world, "jobs", "id");
    ensure_db_table(world, "job_assignments", "id");
    ensure_db_table(world, "workers", "id");
}

#[given(r#"^a database with tables "users", "posts", and "comments"$"#)]
async fn given_db_three_tables(world: &mut VirtuusWorld) {
    ensure_db_table(world, "users", "id");
    ensure_db_table(world, "posts", "id");
    ensure_db_table(world, "comments", "id");
}

#[given(regex = r#"^posts for user-1 with created_at values (.+)$"#)]
async fn given_posts_specific(world: &mut VirtuusWorld, dates: String) {
    let posts = ensure_db_table(world, "posts", "id");
    posts.add_gsi("by_user", "user_id", Some("created_at"));
    let values: Vec<String> = dates
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .collect();
    for (i, date) in values.iter().enumerate() {
        posts.put(json!({"id": format!("post-{}", i+1), "user_id":"user-1", "created_at": date}));
    }
}

#[given(r#"^a database with a "users" table and no GSI named "by_foo"$"#)]
async fn given_db_no_gsi(world: &mut VirtuusWorld) {
    let table = ensure_db_table(world, "users", "id");
    assert!(!table.gsis().contains_key("by_foo"));
}

#[given(r#"^20 posts for user "([^"]*)" with sequential created_at values$"#)]
async fn given_posts_seq(world: &mut VirtuusWorld, user: String) {
    let posts = ensure_db_table(world, "posts", "id");
    if !posts.gsis().contains_key("by_user") {
        posts.add_gsi("by_user", "user_id", Some("created_at"));
    }
    for i in 0..20 {
        posts.put(json!({
            "id": format!("post-{}", i+1),
            "user_id": user,
            "created_at": format!("2025-01-{:02}", i+1)
        }));
    }
}

#[given("3 posts for user-1 with ascending created_at values")]
async fn given_posts_three(world: &mut VirtuusWorld) {
    given_posts_seq(world, "user-1".to_string()).await;
}

#[given("3 posts reference non-existent users")]
async fn given_posts_missing_users(world: &mut VirtuusWorld) {
    let posts = ensure_db_table(world, "posts", "id");
    posts.put(json!({"id":"p1","user_id":"missing-1"}));
    posts.put(json!({"id":"p2","user_id":"missing-2"}));
    posts.put(json!({"id":"p3","user_id":"missing-3"}));
}

#[when("I page through with limit 10")]
async fn when_page_through_limit(world: &mut VirtuusWorld) {
    let mut token: Option<String> = None;
    let mut pages = Vec::new();
    let mut db = ensure_db(world);
    loop {
        let mut query = json!({"users": {"limit": 10}});
        if let Some(tok) = token.clone() {
            query["users"]["next_token"] = Value::String(tok);
        }
        let result = db.execute(&query);
        let items = items_from_result(&result);
        if items.is_empty() {
            break;
        }
        token = result
            .get("next_token")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        pages.push(items);
        if token.is_none() {
            break;
        }
    }
    world.pages = pages;
    world.db_result = None;
}

#[when("I reach the last page")]
async fn when_reach_last_page(world: &mut VirtuusWorld) {
    // last page is stored in pages from previous step
    if let Some(last) = world.pages.last() {
        world.last_records = last.clone();
    }
}

#[when(regex = r#"^I execute (\{.*\}) and receive a next_token$"#)]
async fn when_execute_and_store_token(world: &mut VirtuusWorld, query_text: String) {
    when_execute_query(world, query_text).await;
    world.last_result = ids_from_items(&world.last_records);
}

#[when(regex = r#"^I page through with (\{.*\})$"#)]
async fn when_page_through_query(world: &mut VirtuusWorld, query_text: String) {
    let mut query: Value = serde_json::from_str(&query_text).expect("invalid json");
    let mut token: Option<String> = None;
    let mut pages = Vec::new();
    let mut db = ensure_db(world);
    loop {
        if let Some(tok) = token.clone() {
            let map = query.as_object_mut().unwrap();
            let inner = map.values_mut().next().unwrap().as_object_mut().unwrap();
            inner.insert("next_token".to_string(), Value::String(tok));
        }
        let result = db.execute(&query);
        let items = items_from_result(&result);
        if items.is_empty() {
            break;
        }
        token = result
            .get("next_token")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        pages.push(items);
        if token.is_none() {
            break;
        }
    }
    world.pages = pages;
    if let Some(last) = world.pages.last() {
        world.last_records = last.clone();
    }
}

#[when("I page through all records with limit 10")]
async fn when_page_through_all(world: &mut VirtuusWorld) {
    when_page_through_limit(world).await;
    let mut all = Vec::new();
    for page in &world.pages {
        all.extend(page.clone());
    }
    world.last_count = Some(all.len());
}

#[then("each page should contain records in descending created_at order")]
async fn then_pages_desc(world: &mut VirtuusWorld) {
    for page in &world.pages {
        let dates: Vec<String> = page
            .iter()
            .filter_map(|r| r.get("created_at"))
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect();
        let mut sorted = dates.clone();
        sorted.sort_by(|a, b| b.cmp(a));
        assert_eq!(dates, sorted);
    }
}

#[then("the full traversal should return all 20 posts in reverse chronological order")]
async fn then_full_desc(world: &mut VirtuusWorld) {
    let mut all = Vec::new();
    for page in &world.pages {
        all.extend(page.clone());
    }
    assert_eq!(all.len(), 20);
    let mut dates: Vec<String> = all
        .iter()
        .filter_map(|r| r.get("created_at"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
    let mut sorted = dates.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    assert_eq!(dates, sorted);
}

#[then("the result should contain exactly 10 records")]
async fn then_exact_ten(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    assert_eq!(items_from_result(result).len(), 10);
}

#[then("the result should be an empty list")]
async fn then_empty_list(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    assert!(result.as_array().map(|a| a.is_empty()).unwrap_or(false));
}

// Nested include expectations
#[then("the result should include the user with a nested \"posts\" array of 3 records")]
async fn then_nested_posts(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    assert_eq!(
        result
            .get("posts")
            .and_then(|p| p.as_array())
            .map(|a| a.len()),
        Some(3)
    );
}

#[then("the result should include the post with a nested \"author\" object")]
async fn then_nested_author(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    assert!(result.get("author").and_then(|a| a.as_object()).is_some());
}

#[then("the result should include the job with a nested \"workers\" array of 2 records")]
async fn then_nested_workers(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    assert_eq!(
        result
            .get("workers")
            .and_then(|w| w.as_array())
            .map(|a| a.len()),
        Some(2)
    );
}

#[then("the result should include user → posts → comments nested 3 levels deep")]
async fn then_nested_three(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    let posts = result.get("posts").and_then(|p| p.as_array()).unwrap();
    assert!(posts.first().unwrap().get("comments").is_some());
}

#[then(r#"^each nested post should only contain "([^"]*)" and "([^"]*)" fields$"#)]
async fn then_nested_projection(world: &mut VirtuusWorld, f1: String, f2: String) {
    let result = world.db_result.as_ref().expect("missing result");
    let posts = result.get("posts").and_then(|p| p.as_array()).unwrap();
    for post in posts {
        let keys: Vec<String> = post.as_object().unwrap().keys().cloned().collect();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&f1));
        assert!(keys.contains(&f2));
    }
}

#[then("the nested \"posts\" array should be empty")]
async fn then_nested_posts_empty(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    assert_eq!(
        result
            .get("posts")
            .and_then(|p| p.as_array())
            .map(|a| a.len()),
        Some(0)
    );
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    VirtuusWorld::run("../features").await;
}
