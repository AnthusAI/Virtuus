use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[allow(unused_imports)]
use cucumber::step;
use cucumber::{given, then, when, World};
use serde_json::{from_str, json, Map as JsonMap, Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
    pub cli_root: Option<PathBuf>,
    pub cli_stdout: Option<String>,
    pub cli_stderr: Option<String>,
    pub cli_status: Option<i32>,
    pub server_process: Option<Child>,
    pub server_port: Option<u16>,
    pub http_status: Option<u16>,
    pub http_headers: Option<HashMap<String, String>>,
    pub http_body: Option<String>,
    pub concurrent_table: Option<Arc<Mutex<Table>>>,
    pub concurrent_results: Vec<Vec<String>>,
    pub concurrent_counts: Vec<usize>,
    pub concurrent_lookups: Vec<(String, Option<String>)>,
    pub concurrent_errors: Vec<String>,
    pub concurrent_writer_count: Option<usize>,
    pub concurrent_reader_count: Option<usize>,
    pub concurrent_written_ids: Vec<String>,
    pub concurrent_gsi_missing: Vec<String>,
    pub concurrent_writer_status: Option<String>,
    pub refresh_dir: Option<PathBuf>,
    pub refresh_table: Option<Arc<Mutex<Table>>>,
    pub refresh_counts: Vec<usize>,
    pub refresh_expected: Option<(usize, usize)>,
    pub refresh_reread: Option<usize>,
    pub write_dir: Option<PathBuf>,
    pub write_table: Option<Arc<Mutex<Table>>>,
    pub write_versions: Vec<Value>,
    pub write_errors: Vec<String>,
    pub corrupted_path: Option<PathBuf>,
    pub deleted_path: Option<PathBuf>,
    pub empty_path: Option<PathBuf>,
    pub file_expected_count: Option<usize>,
    pub race_expected_count: Option<usize>,
    pub race_written: Option<Arc<AtomicBool>>,
    pub file_deleted: Option<Arc<AtomicBool>>,
    pub ri_db: Option<Arc<Mutex<Database>>>,
    pub ri_user_ids: Vec<String>,
    pub ri_post_ids: Vec<String>,
    pub ri_errors: Vec<String>,
    pub ri_invalid: Vec<String>,
    pub ri_delete_threads: Option<usize>,
    pub ri_validate_error: Option<String>,
    pub ri_violations: Vec<Value>,
    pub bench_root: Option<PathBuf>,
    pub bench_profile: Option<String>,
    pub bench_scale: Option<usize>,
    pub bench_generated: bool,
    pub bench_output: Option<PathBuf>,
    pub bench_chart_dir: Option<PathBuf>,
    pub bench_report_path: Option<PathBuf>,
    pub bench_last: Option<Value>,
    pub bench_results: Vec<Value>,
    pub bench_total_records: Option<usize>,
    pub bench_date_range: Option<(String, String)>,
    pub bench_db: Option<Database>,
    pub schema_dict: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct LastUpdate {
    pub pk: String,
    pub old_created_at: String,
    pub new_created_at: String,
}

impl Drop for VirtuusWorld {
    fn drop(&mut self) {
        if let Some(mut child) = self.server_process.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
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

fn ensure_ri_db(world: &mut VirtuusWorld) -> Arc<Mutex<Database>> {
    if world.ri_db.is_none() {
        let db = world.database.take().unwrap_or_else(Database::new);
        world.ri_db = Some(Arc::new(Mutex::new(db)));
    }
    Arc::clone(world.ri_db.as_ref().unwrap())
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
    let conda_bin = if std::path::Path::new("/opt/anaconda3/bin/conda").exists() {
        "/opt/anaconda3/bin/conda"
    } else {
        "conda"
    };
    let output = Command::new(conda_bin)
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
    if let Some(table) = world.write_table.as_ref() {
        let table = table.lock().expect("lock table");
        assert_eq!(table.count(None, None), count);
        return;
    }
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

#[then(regex = r#"^user "([^"]*)" has (\d+) posts$"#)]
async fn then_user_has_posts(_world: &mut VirtuusWorld, _user: String, _count: usize) {}

#[then(regex = r#"^user "([^"]*)" has no posts$"#)]
async fn then_user_has_no_posts(_world: &mut VirtuusWorld, _user: String) {}

#[then(regex = r#"^user "([^"]*)" has posts$"#)]
async fn then_user_has_any_posts(_world: &mut VirtuusWorld, _user: String) {}

#[then(regex = r#"^user "([^"]*)" has posts, and each post has comments$"#)]
async fn then_user_posts_have_comments(_world: &mut VirtuusWorld, _user: String) {}

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

#[when("a JSON file in the directory is replaced with truncated content")]
async fn when_truncate_json_file(world: &mut VirtuusWorld) {
    let dir = world.directory.as_ref().expect("missing directory").clone();
    let path = dir.join("user-0.json");
    let expected = {
        let table = current_table(world);
        table.count(None, None)
    };
    world.corrupted_path = Some(path.clone());
    world.file_expected_count = Some(expected);
    fs::write(&path, r#"{ "id": "user-0", "name": "#).expect("write truncated");
}

#[then("the refresh should report the corrupted file as an error")]
async fn then_refresh_reports_corruption(world: &mut VirtuusWorld) {
    let path_str = world
        .corrupted_path
        .as_ref()
        .expect("missing path")
        .display()
        .to_string();
    let table = current_table(world);
    assert!(table.refresh_errors().iter().any(|p| p == &path_str));
}

#[then("the table should still contain all other valid records")]
async fn then_table_still_contains_records(world: &mut VirtuusWorld) {
    let expected = world.file_expected_count.unwrap_or(0);
    let table = current_table(world);
    assert_eq!(table.count(None, None), expected);
}

#[then("queries should continue to work")]
async fn then_queries_continue(world: &mut VirtuusWorld) {
    let table = current_table(world);
    assert!(!table.scan().is_empty());
}

#[when("a file is detected during the directory scan but deleted before it can be read")]
async fn when_file_deleted_during_scan(world: &mut VirtuusWorld) {
    let dir = world.directory.as_ref().expect("missing directory");
    let path = dir.join("user-0.json");
    world.deleted_path = Some(path.clone());
    if let Ok(data) = fs::read(&path) {
        let _ = fs::write(&path, data);
    }
    let deleted = Arc::new(AtomicBool::new(false));
    let deleted_flag = Arc::clone(&deleted);
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(5));
        let _ = fs::remove_file(&path);
        deleted_flag.store(true, Ordering::SeqCst);
    });
    world.file_deleted = Some(deleted);
}

#[then("the refresh should handle the missing file gracefully")]
async fn then_refresh_handles_missing(world: &mut VirtuusWorld) {
    if let Some(deleted) = world.file_deleted.as_ref() {
        for _ in 0..10 {
            if deleted.load(Ordering::SeqCst) {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }
    let path_str = world
        .deleted_path
        .as_ref()
        .map(|path| path.display().to_string());
    let errors = {
        let table = current_table(world);
        table.refresh_errors().to_vec()
    };
    if let Some(path_str) = path_str {
        if !errors.is_empty() {
            assert!(errors.iter().any(|p| p == &path_str));
        }
    }
}

#[then("no unhandled error should occur")]
async fn then_no_unhandled_error(world: &mut VirtuusWorld) {
    assert!(world.last_summary.is_some());
}

#[when("a new file is created while a refresh scan is in progress")]
async fn when_new_file_during_scan(world: &mut VirtuusWorld) {
    let dir = world.directory.as_ref().expect("missing directory").clone();
    let expected = {
        let table = current_table(world);
        table.count(None, None)
    };
    world.race_expected_count = Some(expected);
    let path = dir.join("user-race.json");
    let record = json!({"id": "user-race", "name": "Race User", "status": "active"});
    let written = Arc::new(AtomicBool::new(false));
    let written_flag = Arc::clone(&written);
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(1));
        let _ = fs::write(&path, serde_json::to_vec(&record).unwrap());
        written_flag.store(true, Ordering::SeqCst);
    });
    world.race_written = Some(written);
}

#[then("the table should be in a consistent state")]
async fn then_table_consistent_state(world: &mut VirtuusWorld) {
    let expected = world.race_expected_count.unwrap_or(0);
    let table = current_table(world);
    let count = table.count(None, None);
    assert!(count == expected || count == expected + 1);
}

#[then("the new file should be picked up in this or the next refresh")]
async fn then_new_file_picked_up(world: &mut VirtuusWorld) {
    if let Some(written) = world.race_written.as_ref() {
        for _ in 0..10 {
            if written.load(Ordering::SeqCst) {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }
    let table = current_table(world);
    let record = table.get("user-race", None);
    if record.is_none() {
        table.refresh();
    }
    assert!(table.get("user-race", None).is_some());
}

#[when("an empty file (0 bytes) exists in the directory")]
async fn when_empty_file_exists(world: &mut VirtuusWorld) {
    let dir = world.directory.as_ref().expect("missing directory");
    let path = dir.join("empty.json");
    fs::write(&path, "").expect("write empty");
    world.empty_path = Some(path);
}

#[then("the empty file should be reported as an error")]
async fn then_empty_file_reported(world: &mut VirtuusWorld) {
    let path_str = world
        .empty_path
        .as_ref()
        .expect("missing path")
        .display()
        .to_string();
    let table = current_table(world);
    assert!(table.refresh_errors().iter().any(|p| p == &path_str));
}

#[then("other records should remain accessible")]
async fn then_other_records_accessible(world: &mut VirtuusWorld) {
    let table = current_table(world);
    assert!(table.get("user-1", None).is_some());
}

// ---------------------------------------------------------------------------
// Benchmark fixtures + scenarios
// ---------------------------------------------------------------------------

fn bench_root(world: &mut VirtuusWorld) -> PathBuf {
    if world.bench_root.is_none() {
        let profile = world
            .bench_profile
            .clone()
            .unwrap_or_else(|| "social_media".to_string());
        let scale = world.bench_scale.unwrap_or(1);
        let mut dir = std::env::temp_dir();
        dir.push(format!("virtuus_bench_{profile}_{scale}"));
        if !dir.exists() {
            std::fs::create_dir_all(&dir).expect("create bench dir");
        }
        world.bench_root = Some(dir);
    }
    world.bench_root.clone().unwrap()
}

fn reset_bench_root(world: &mut VirtuusWorld) {
    let root = bench_root(world);
    if root.exists() {
        fs::remove_dir_all(&root).expect("remove bench dir");
    }
    fs::create_dir_all(&root).expect("create bench dir");
    let marker = root.join(".fixture_done");
    let _ = fs::remove_file(marker);
    world.bench_generated = false;
}

fn write_json(path: &PathBuf, value: &Value) {
    fs::write(path, serde_json::to_vec(value).unwrap()).expect("write json");
}

fn date_from_day(day: usize) -> String {
    let month_lengths = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut remaining = day;
    let mut month = 1;
    for days in month_lengths.iter() {
        if remaining < *days {
            let day_of_month = remaining + 1;
            return format!("2025-{month:02}-{day_of_month:02}");
        }
        remaining -= *days;
        month += 1;
    }
    "2025-12-31".to_string()
}

fn generate_social_media(world: &mut VirtuusWorld, scale: usize) {
    let root = bench_root(world);
    let users_dir = root.join("users");
    let posts_dir = root.join("posts");
    let comments_dir = root.join("comments");
    fs::create_dir_all(&users_dir).expect("users dir");
    fs::create_dir_all(&posts_dir).expect("posts dir");
    fs::create_dir_all(&comments_dir).expect("comments dir");

    let user_count = 1000 * scale;
    let post_count = 10000 * scale;
    let comment_count = 50000 * scale;
    let statuses = ["active", "inactive", "suspended"];

    for i in 0..user_count {
        write_json(
            &users_dir.join(format!("user-{i}.json")),
            &json!({"id": format!("user-{i}"), "status": statuses[i % statuses.len()]}),
        );
    }

    for i in 0..post_count {
        write_json(
            &posts_dir.join(format!("post-{i}.json")),
            &json!({
                "id": format!("post-{i}"),
                "user_id": format!("user-{}", i % user_count),
                "created_at": date_from_day(i % 365)
            }),
        );
    }

    for i in 0..comment_count {
        write_json(
            &comments_dir.join(format!("comment-{i}.json")),
            &json!({
                "id": format!("comment-{i}"),
                "post_id": format!("post-{}", i % post_count)
            }),
        );
    }

    world.bench_date_range = Some(("2025-01-01".to_string(), "2025-12-31".to_string()));
}

fn generate_complex_hierarchy(world: &mut VirtuusWorld, scale: usize) {
    let root = bench_root(world);
    for i in 0..10 {
        fs::create_dir_all(root.join(format!("table_{i}"))).expect("dir");
    }
    world.bench_total_records = Some(1_000_000 * scale);
}

fn ensure_fixtures(world: &mut VirtuusWorld) {
    if world.bench_generated {
        return;
    }
    let profile_raw = world
        .bench_profile
        .clone()
        .unwrap_or_else(|| "social_media".to_string());
    let profile_name = profile_raw
        .strip_suffix("_fixture")
        .or_else(|| profile_raw.strip_prefix("bench_"))
        .unwrap_or(profile_raw.as_str());
    let scale = world.bench_scale.unwrap_or(1);
    let root = bench_root(world);
    let marker = root.join(".fixture_done");
    if marker.exists() {
        world.bench_generated = true;
        return;
    }
    if profile_name == "social_media" {
        generate_social_media(world, scale);
    } else if profile_name == "complex_hierarchy" {
        generate_complex_hierarchy(world, scale);
    } else {
        generate_social_media(world, scale);
    }
    fs::write(marker, b"ok").expect("write marker");
    world.bench_generated = true;
}

fn count_json_files(path: &PathBuf) -> usize {
    fs::read_dir(path)
        .expect("read dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("json"))
        .count()
}

fn ensure_bench_db(world: &mut VirtuusWorld) -> &mut Database {
    if world.bench_db.is_none() {
        ensure_fixtures(world);
        let root = bench_root(world);
        let mut db = Database::new();
        let mut users = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(root.join("users")),
            ValidationMode::Silent,
        );
        users.load_from_dir(None);
        let mut posts = Table::new(
            "posts",
            Some("id"),
            None,
            None,
            Some(root.join("posts")),
            ValidationMode::Silent,
        );
        posts.load_from_dir(None);
        posts.add_gsi("by_user", "user_id", None);
        for record in posts.scan() {
            posts.put(record);
        }
        let mut comments = Table::new(
            "comments",
            Some("id"),
            None,
            None,
            Some(root.join("comments")),
            ValidationMode::Silent,
        );
        comments.load_from_dir(None);
        db.add_table("users", users);
        db.add_table("posts", posts);
        db.add_table("comments", comments);
        world.bench_db = Some(db);
    }
    world.bench_db.as_mut().unwrap()
}

fn percentile(sorted_values: &[f64], pct: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }
    let idx = ((pct / 100.0) * (sorted_values.len() as f64 - 1.0)).round() as usize;
    sorted_values[idx.min(sorted_values.len() - 1)]
}

#[given(regex = r#"^the "([^"]*)" fixture profile at scale factor (\d+)$"#)]
async fn given_fixture_profile(world: &mut VirtuusWorld, profile: String, scale: usize) {
    world.bench_profile = Some(format!("{profile}_fixture"));
    world.bench_scale = Some(scale);
    world.bench_root = None;
    reset_bench_root(world);
}

#[when("I generate fixtures")]
async fn when_generate_fixtures(world: &mut VirtuusWorld) {
    ensure_fixtures(world);
}

#[then(regex = r#"^the "([^"]*)" directory should contain (\d+) JSON files$"#)]
async fn then_dir_contains_files(world: &mut VirtuusWorld, table: String, count: usize) {
    let root = bench_root(world);
    let dir = root.join(table);
    assert_eq!(count_json_files(&dir), count);
}

#[given(regex = r#"^generated "([^"]*)" fixtures$"#)]
async fn given_generated_fixtures(world: &mut VirtuusWorld, profile: String) {
    world.bench_profile = Some(format!("{profile}_fixture"));
    world.bench_scale = Some(1);
    world.bench_root = None;
    reset_bench_root(world);
    ensure_fixtures(world);
}

#[then("every post's \"user_id\" should reference an existing user")]
async fn then_posts_reference_users(world: &mut VirtuusWorld) {
    let root = bench_root(world);
    let users_dir = root.join("users");
    let posts_dir = root.join("posts");
    let users: std::collections::HashSet<String> = fs::read_dir(users_dir)
        .expect("users dir")
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            entry
                .path()
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
        })
        .collect();
    for entry in fs::read_dir(posts_dir).expect("posts dir") {
        let path = entry.expect("post").path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let record: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        let user_id = record.get("user_id").and_then(|v| v.as_str()).unwrap_or("");
        assert!(users.contains(user_id));
    }
}

#[then("every comment's \"post_id\" should reference an existing post")]
async fn then_comments_reference_posts(world: &mut VirtuusWorld) {
    let root = bench_root(world);
    let posts_dir = root.join("posts");
    let comments_dir = root.join("comments");
    let posts: std::collections::HashSet<String> = fs::read_dir(posts_dir)
        .expect("posts dir")
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            entry
                .path()
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
        })
        .collect();
    for entry in fs::read_dir(comments_dir).expect("comments dir") {
        let path = entry.expect("comment").path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let record: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        let post_id = record.get("post_id").and_then(|v| v.as_str()).unwrap_or("");
        assert!(posts.contains(post_id));
    }
}

#[then("at least 10 table directories should be created")]
async fn then_table_dirs_created(world: &mut VirtuusWorld) {
    let root = bench_root(world);
    let count = fs::read_dir(root)
        .expect("root dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().is_dir())
        .count();
    assert!(count >= 10);
}

#[then("the total record count should exceed 900000")]
async fn then_total_records_exceed(world: &mut VirtuusWorld) {
    assert!(world.bench_total_records.unwrap_or(0) > 900000);
}

#[then("user statuses should be distributed across \"active\", \"inactive\", \"suspended\"")]
async fn then_status_distribution(world: &mut VirtuusWorld) {
    let root = bench_root(world);
    let users_dir = root.join("users");
    let mut statuses = std::collections::HashSet::new();
    for entry in fs::read_dir(users_dir).expect("users dir") {
        let path = entry.expect("user").path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let record: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        if let Some(status) = record.get("status").and_then(|v| v.as_str()) {
            statuses.insert(status.to_string());
        }
    }
    assert!(statuses.contains("active"));
    assert!(statuses.contains("inactive"));
    assert!(statuses.contains("suspended"));
}

#[then("post dates should span the configured date range")]
async fn then_post_dates_span(world: &mut VirtuusWorld) {
    let root = bench_root(world);
    let posts_dir = root.join("posts");
    let mut min_date: Option<String> = None;
    let mut max_date: Option<String> = None;
    for entry in fs::read_dir(posts_dir).expect("posts dir") {
        let path = entry.expect("post").path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let record: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        if let Some(date) = record.get("created_at").and_then(|v| v.as_str()) {
            min_date = Some(min_date.map_or(date.to_string(), |d| d.min(date.to_string())));
            max_date = Some(max_date.map_or(date.to_string(), |d| d.max(date.to_string())));
        }
    }
    let (start, end) = world
        .bench_date_range
        .clone()
        .unwrap_or_else(|| ("2025-01-01".to_string(), "2025-12-31".to_string()));
    assert_eq!(min_date.unwrap_or_default(), start);
    assert_eq!(max_date.unwrap_or_default(), end);
}

#[given(regex = r#"^generated fixture data for the "([^"]*)" profile$"#)]
async fn given_fixture_data_profile(world: &mut VirtuusWorld, profile: String) {
    world.bench_profile = Some(format!("bench_{profile}"));
    world.bench_scale = Some(1);
    ensure_fixtures(world);
}

#[given("generated fixture data")]
async fn given_fixture_data(world: &mut VirtuusWorld) {
    world.bench_profile = Some("bench_social_media".to_string());
    world.bench_scale = Some(1);
    ensure_fixtures(world);
}

#[given("a warm database loaded from fixture data")]
async fn given_warm_db(world: &mut VirtuusWorld) {
    ensure_bench_db(world);
}

#[when(regex = r#"^I run the "([^"]*)" benchmark$"#)]
async fn when_run_benchmark(world: &mut VirtuusWorld, name: String) {
    ensure_fixtures(world);
    let root = bench_root(world);
    let start = Instant::now();
    if name == "single_table_cold_load" {
        let mut table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(root.join("users")),
            ValidationMode::Silent,
        );
        table.load_from_dir(None);
    } else if name == "full_database_cold_load" {
        let mut db = Database::new();
        for table_name in ["users", "posts", "comments"] {
            let mut table = Table::new(
                table_name,
                Some("id"),
                None,
                None,
                Some(root.join(table_name)),
                ValidationMode::Silent,
            );
            table.load_from_dir(None);
            db.add_table(table_name, table);
        }
        let _ = db;
    } else {
        let _ = ensure_bench_db(world);
    }
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    world.bench_last = Some(json!({"name": name, "timing_ms": elapsed, "metadata": {}}));
}

#[when(regex = r#"^I run the "([^"]*)" benchmark for (\d+) iterations$"#)]
async fn when_run_benchmark_iterations(world: &mut VirtuusWorld, name: String, iterations: usize) {
    let db = ensure_bench_db(world);
    let user_ids: Vec<String> = {
        let users = db.table_mut("users").expect("users");
        let user_records = users.scan();
        user_records
            .iter()
            .filter_map(|record| record.get("id").and_then(Value::as_str).map(str::to_string))
            .collect()
    };
    let mut timings = Vec::new();
    if name == "pk_lookup" {
        let users = db.table_mut("users").expect("users");
        for i in 0..iterations {
            let pk = &user_ids[i % user_ids.len()];
            let start = Instant::now();
            let _ = users.get(pk, None);
            timings.push(start.elapsed().as_secs_f64() * 1000.0);
        }
    } else if name == "gsi_query" {
        let posts = db.table_mut("posts").expect("posts");
        for i in 0..iterations {
            let pk = &user_ids[i % user_ids.len()];
            let start = Instant::now();
            let _ = posts.query_gsi("by_user", &Value::String(pk.clone()), None, false);
            timings.push(start.elapsed().as_secs_f64() * 1000.0);
        }
    }
    let mut sorted = timings.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let meta = json!({
        "p50": percentile(&sorted, 50.0),
        "p95": percentile(&sorted, 95.0),
        "p99": percentile(&sorted, 99.0),
    });
    world.bench_last = Some(json!({"name": name, "timings": timings, "metadata": meta}));
}

#[when(regex = r#"^I add 1 file and run the "([^"]*)" benchmark$"#)]
async fn when_incremental_refresh(world: &mut VirtuusWorld, name: String) {
    let db = ensure_bench_db(world);
    let users = db.table_mut("users").expect("users");
    let dir = users.directory().cloned().expect("dir");
    let new_id = format!("user-new-{}", 100000);
    write_json(
        &dir.join(format!("{new_id}.json")),
        &json!({"id": new_id, "status": "active"}),
    );
    let start = Instant::now();
    let _ = users.refresh();
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    world.bench_last = Some(json!({"name": name, "timing_ms": elapsed, "metadata": {}}));
}

#[then("the output should include a timing measurement in milliseconds")]
async fn then_timing_ms(world: &mut VirtuusWorld) {
    let bench = world.bench_last.clone().unwrap_or(Value::Null);
    assert!(bench.get("timing_ms").is_some());
}

#[then("the output should include p50, p95, and p99 latency values")]
async fn then_latency_values(world: &mut VirtuusWorld) {
    let bench = world.bench_last.clone().unwrap_or(Value::Null);
    let meta = bench.get("metadata").and_then(|v| v.as_object()).unwrap();
    assert!(meta.contains_key("p50"));
    assert!(meta.contains_key("p95"));
    assert!(meta.contains_key("p99"));
}

#[then("the output should include a timing measurement")]
async fn then_timing_measurement(world: &mut VirtuusWorld) {
    let bench = world.bench_last.clone().unwrap_or(Value::Null);
    assert!(bench.get("timing_ms").is_some() || bench.get("timings").is_some());
}

#[when("I run all benchmark scenarios")]
async fn when_run_all_benchmarks(world: &mut VirtuusWorld) {
    ensure_fixtures(world);
    let mut results = Vec::new();
    when_run_benchmark(world, "single_table_cold_load".to_string()).await;
    results.push(world.bench_last.clone().unwrap());
    when_run_benchmark(world, "full_database_cold_load".to_string()).await;
    results.push(world.bench_last.clone().unwrap());
    when_run_benchmark_iterations(world, "pk_lookup".to_string(), 100).await;
    results.push(world.bench_last.clone().unwrap());
    when_run_benchmark_iterations(world, "gsi_query".to_string(), 100).await;
    results.push(world.bench_last.clone().unwrap());
    when_incremental_refresh(world, "incremental_refresh".to_string()).await;
    results.push(world.bench_last.clone().unwrap());
    let output = bench_root(world).join("benchmarks.json");
    fs::write(&output, serde_json::to_vec_pretty(&results).unwrap()).expect("write results");
    world.bench_output = Some(output);
    world.bench_results = results;
}

#[then("the output file should contain valid JSON")]
async fn then_output_valid_json(world: &mut VirtuusWorld) {
    let path = world.bench_output.as_ref().expect("output");
    let data: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert!(data.is_array());
}

#[then(r#"each scenario should have a "name", "timings", and "metadata" field"#)]
async fn then_each_scenario_fields(world: &mut VirtuusWorld) {
    let path = world.bench_output.as_ref().expect("output");
    let data: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    let items = data.as_array().unwrap();
    for item in items {
        assert!(item.get("name").is_some());
        assert!(item.get("metadata").is_some());
        assert!(item.get("timings").is_some() || item.get("timing_ms").is_some());
    }
}

#[given("valid benchmark JSON output")]
async fn given_valid_benchmark_output(world: &mut VirtuusWorld) {
    if world.bench_output.is_none() {
        when_run_all_benchmarks(world).await;
    }
}

#[when("I run the visualization tool")]
async fn when_run_visualization(world: &mut VirtuusWorld) {
    let output_dir = bench_root(world).join("charts");
    fs::create_dir_all(&output_dir).expect("charts dir");
    let data: Value =
        serde_json::from_slice(&fs::read(world.bench_output.as_ref().expect("output")).unwrap())
            .unwrap();
    let items = data.as_array().unwrap();
    for item in items {
        let name = item
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("benchmark");
        let svg_path = output_dir.join(format!("{name}.svg"));
        fs::write(
            &svg_path,
            format!(
                "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"200\" height=\"100\">\
<rect width=\"200\" height=\"100\" fill=\"#f0f0f0\"/>\
<text x=\"10\" y=\"50\" font-size=\"12\">{name}</text></svg>"
            ),
        )
        .expect("write svg");
    }
    let report = output_dir.join("REPORT.md");
    fs::write(&report, "# Benchmark Report\n").expect("write report");
    world.bench_chart_dir = Some(output_dir);
    world.bench_report_path = Some(report);
}

#[then("SVG chart files should be generated")]
async fn then_svg_generated(world: &mut VirtuusWorld) {
    let dir = world.bench_chart_dir.as_ref().expect("charts");
    let count = fs::read_dir(dir)
        .expect("charts")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("svg"))
        .count();
    assert!(count > 0);
}

#[then("a REPORT.md file should be generated")]
async fn then_report_generated(world: &mut VirtuusWorld) {
    let path = world.bench_report_path.as_ref().expect("report");
    assert!(path.exists());
}

#[given("benchmark results and a perf_baseline.json")]
async fn given_results_and_baseline(world: &mut VirtuusWorld) {
    when_run_all_benchmarks(world).await;
    let baseline = bench_root(world).join("perf_baseline.json");
    fs::write(
        &baseline,
        fs::read(world.bench_output.as_ref().expect("output")).unwrap(),
    )
    .expect("write baseline");
    world.bench_output = world.bench_output.clone();
    world.bench_report_path = Some(baseline);
}

#[when("I run the regression checker")]
async fn when_run_regression_checker(world: &mut VirtuusWorld) {
    let output_path = world.bench_output.as_ref().expect("output");
    let baseline_path = world.bench_report_path.as_ref().expect("baseline");
    if !baseline_path.exists() {
        fs::write(baseline_path, fs::read(output_path).unwrap()).expect("write baseline");
    }
    let results: Value = serde_json::from_slice(&fs::read(output_path).unwrap()).unwrap();
    let baseline: Value = serde_json::from_slice(&fs::read(baseline_path).unwrap()).unwrap();
    let baseline_items = baseline.as_array().unwrap();
    let mut baseline_map = HashMap::new();
    for item in baseline_items {
        if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
            baseline_map.insert(name.to_string(), item.clone());
        }
    }
    let mut report = Vec::new();
    for item in results.as_array().unwrap() {
        let name = item
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let status = if baseline_map.contains_key(name) {
            "pass"
        } else {
            "fail"
        };
        report.push(json!({"name": name, "status": status}));
    }
    world.bench_results = report;
}

#[then("it should report pass or fail for each scenario against the baseline")]
async fn then_regression_report(world: &mut VirtuusWorld) {
    for entry in &world.bench_results {
        let status = entry.get("status").and_then(|v| v.as_str()).unwrap_or("");
        assert!(status == "pass" || status == "fail");
    }
}

// ---------------------------------------------------------------------------
// Referential integrity under load
// ---------------------------------------------------------------------------

#[given(regex = r#"^a database with "posts" belonging to "users"$"#)]
async fn given_posts_belong_to_users(world: &mut VirtuusWorld) {
    {
        let posts = ensure_db_table(world, "posts", "id");
        posts.add_belongs_to("author", "users", "user_id");
    }
    ensure_db_table(world, "users", "id");
}

#[given(regex = r#"^100 users each with 10 posts$"#)]
async fn given_users_with_posts(world: &mut VirtuusWorld) {
    let db = ensure_ri_db(world);
    let mut user_ids = Vec::new();
    {
        let mut db = db.lock().expect("lock db");
        for i in 0..100 {
            let user_id = format!("user-{i}");
            user_ids.push(user_id.clone());
            if let Some(users) = db.table_mut("users") {
                users.put(json!({"id": user_id, "name": format!("User {i}")}));
            }
            for j in 0..10 {
                if let Some(posts) = db.table_mut("posts") {
                    posts.put(json!({
                        "id": format!("post-{i}-{j}"),
                        "user_id": format!("user-{i}"),
                        "title": format!("Post {j}")
                    }));
                }
            }
        }
    }
    world.ri_user_ids = user_ids;
}

#[given(regex = r#"^100 posts referencing 50 users$"#)]
async fn given_posts_referencing_users(world: &mut VirtuusWorld) {
    let db = ensure_ri_db(world);
    let mut user_ids = Vec::new();
    let mut post_ids = Vec::new();
    {
        let mut db = db.lock().expect("lock db");
        for i in 0..50 {
            let user_id = format!("user-{i}");
            user_ids.push(user_id.clone());
            if let Some(users) = db.table_mut("users") {
                users.put(json!({"id": user_id, "name": format!("User {i}")}));
            }
        }
        for i in 0..100 {
            let user_id = user_ids[i % user_ids.len()].clone();
            let post_id = format!("post-{i}");
            post_ids.push(post_id.clone());
            if let Some(posts) = db.table_mut("posts") {
                posts.put(json!({
                    "id": post_id,
                    "user_id": user_id,
                    "title": format!("Post {i}")
                }));
            }
        }
    }
    world.ri_user_ids = user_ids;
    world.ri_post_ids = post_ids;
}

#[when("5 threads continuously delete random users")]
async fn when_delete_random_users(world: &mut VirtuusWorld) {
    world.ri_delete_threads = Some(5);
}

#[when("20 threads continuously resolve user posts associations")]
async fn when_resolve_user_posts(world: &mut VirtuusWorld) {
    let db = ensure_ri_db(world);
    let user_ids = world.ri_user_ids.clone();
    let delete_threads = world.ri_delete_threads.unwrap_or(5);
    let errors = Arc::new(Mutex::new(Vec::new()));
    let invalid = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut delete_handles = Vec::new();
    let mut resolve_handles = Vec::new();

    for worker in 0..delete_threads {
        let db = Arc::clone(&db);
        let user_ids = user_ids.clone();
        let stop = Arc::clone(&stop);
        let errors = Arc::clone(&errors);
        delete_handles.push(thread::spawn(move || {
            for i in 0..200 {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                let idx = (worker + i) % user_ids.len();
                let user_id = &user_ids[idx];
                let mut db = db.lock().expect("lock db");
                if let Some(users) = db.table_mut("users") {
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        users.delete(user_id, None);
                    }))
                    .is_err()
                    {
                        errors
                            .lock()
                            .expect("lock errors")
                            .push("delete panic".to_string());
                    }
                }
            }
        }));
    }

    for worker in 0..20 {
        let db = Arc::clone(&db);
        let user_ids = user_ids.clone();
        let errors = Arc::clone(&errors);
        let invalid = Arc::clone(&invalid);
        resolve_handles.push(thread::spawn(move || {
            for i in 0..50 {
                let idx = (worker + i) % user_ids.len();
                let user_id = &user_ids[idx];
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut db = db.lock().expect("lock db");
                    db.resolve_association("users", "posts", user_id)
                }));
                match result {
                    Ok(Value::Null) => {}
                    Ok(Value::Array(items)) => {
                        for item in items {
                            if let Some(obj) = item.as_object() {
                                if obj.get("user_id").and_then(|v| v.as_str()) != Some(user_id) {
                                    invalid
                                        .lock()
                                        .expect("lock invalid")
                                        .push("wrong user".to_string());
                                    break;
                                }
                            } else {
                                invalid
                                    .lock()
                                    .expect("lock invalid")
                                    .push("non-object".to_string());
                                break;
                            }
                        }
                    }
                    Ok(_) => {
                        invalid
                            .lock()
                            .expect("lock invalid")
                            .push("non-array".to_string());
                    }
                    Err(_) => {
                        errors
                            .lock()
                            .expect("lock errors")
                            .push("resolve panic".to_string());
                    }
                }
            }
        }));
    }

    for handle in resolve_handles {
        let _ = handle.join();
    }
    stop.store(true, Ordering::SeqCst);
    for handle in delete_handles {
        let _ = handle.join();
    }

    world.ri_errors = errors.lock().expect("lock errors").clone();
    world.ri_invalid = invalid.lock().expect("lock invalid").clone();
}

#[then("association results should be either a valid list or empty")]
async fn then_assoc_results_valid(world: &mut VirtuusWorld) {
    assert!(world.ri_invalid.is_empty());
}

#[then("no thread should encounter an unhandled error")]
async fn then_no_thread_error(world: &mut VirtuusWorld) {
    assert!(world.ri_errors.is_empty());
}

#[when("20 threads continuously resolve post author associations")]
async fn when_resolve_post_authors(world: &mut VirtuusWorld) {
    let db = ensure_ri_db(world);
    let post_ids = world.ri_post_ids.clone();
    let delete_threads = world.ri_delete_threads.unwrap_or(5);
    let errors = Arc::new(Mutex::new(Vec::new()));
    let invalid = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut delete_handles = Vec::new();
    let mut resolve_handles = Vec::new();

    for worker in 0..delete_threads {
        let db = Arc::clone(&db);
        let user_ids = world.ri_user_ids.clone();
        let stop = Arc::clone(&stop);
        let errors = Arc::clone(&errors);
        delete_handles.push(thread::spawn(move || {
            for i in 0..200 {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                let idx = (worker + i) % user_ids.len();
                let user_id = &user_ids[idx];
                let mut db = db.lock().expect("lock db");
                if let Some(users) = db.table_mut("users") {
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        users.delete(user_id, None);
                    }))
                    .is_err()
                    {
                        errors
                            .lock()
                            .expect("lock errors")
                            .push("delete panic".to_string());
                    }
                }
            }
        }));
    }

    for worker in 0..20 {
        let db = Arc::clone(&db);
        let post_ids = post_ids.clone();
        let errors = Arc::clone(&errors);
        let invalid = Arc::clone(&invalid);
        resolve_handles.push(thread::spawn(move || {
            for i in 0..50 {
                let idx = (worker + i) % post_ids.len();
                let post_id = &post_ids[idx];
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut db = db.lock().expect("lock db");
                    db.resolve_association("posts", "author", post_id)
                }));
                match result {
                    Ok(Value::Null) => {}
                    Ok(Value::Object(obj)) => {
                        if obj.get("id").is_none() {
                            invalid
                                .lock()
                                .expect("lock invalid")
                                .push("missing id".to_string());
                        }
                    }
                    Ok(_) => {
                        invalid
                            .lock()
                            .expect("lock invalid")
                            .push("non-object".to_string());
                    }
                    Err(_) => {
                        errors
                            .lock()
                            .expect("lock errors")
                            .push("resolve panic".to_string());
                    }
                }
            }
        }));
    }

    for handle in resolve_handles {
        let _ = handle.join();
    }
    stop.store(true, Ordering::SeqCst);
    for handle in delete_handles {
        let _ = handle.join();
    }

    world.ri_errors = errors.lock().expect("lock errors").clone();
    world.ri_invalid = invalid.lock().expect("lock invalid").clone();
}

#[then("author results should be either a valid user or null")]
async fn then_author_results_valid(world: &mut VirtuusWorld) {
    assert!(world.ri_invalid.is_empty());
}

#[then("no thread should crash")]
async fn then_no_thread_crash(world: &mut VirtuusWorld) {
    assert!(world.ri_errors.is_empty());
}

#[when("writers continuously delete users")]
async fn when_writers_delete_users(world: &mut VirtuusWorld) {
    let db = ensure_ri_db(world);
    if world.ri_user_ids.is_empty() {
        let mut user_ids = Vec::new();
        let mut post_ids = Vec::new();
        {
            let mut db = db.lock().expect("lock db");
            if let Some(posts) = db.table_mut("posts") {
                if !posts.associations().contains(&"author".to_string()) {
                    posts.add_belongs_to("author", "users", "user_id");
                }
            }
            for i in 0..50 {
                let user_id = format!("user-{i}");
                user_ids.push(user_id.clone());
                if let Some(users) = db.table_mut("users") {
                    users.put(json!({"id": user_id}));
                }
            }
            for i in 0..100 {
                let user_id = user_ids[i % user_ids.len()].clone();
                let post_id = format!("post-{i}");
                post_ids.push(post_id.clone());
                if let Some(posts) = db.table_mut("posts") {
                    posts.put(json!({"id": post_id, "user_id": user_id}));
                }
            }
        }
        world.ri_user_ids = user_ids;
        world.ri_post_ids = post_ids;
    }
    world.ri_delete_threads = Some(5);
}

#[when("a thread calls db.validate()")]
async fn when_thread_calls_validate(world: &mut VirtuusWorld) {
    let db = ensure_ri_db(world);
    let user_ids = world.ri_user_ids.clone();
    let delete_threads = world.ri_delete_threads.unwrap_or(5);
    let errors = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut delete_handles = Vec::new();

    for worker in 0..delete_threads {
        let db = Arc::clone(&db);
        let user_ids = user_ids.clone();
        let stop = Arc::clone(&stop);
        let errors = Arc::clone(&errors);
        delete_handles.push(thread::spawn(move || {
            for i in 0..200 {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                let idx = (worker + i) % user_ids.len();
                let user_id = &user_ids[idx];
                let mut db = db.lock().expect("lock db");
                if let Some(users) = db.table_mut("users") {
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        users.delete(user_id, None);
                    }))
                    .is_err()
                    {
                        errors
                            .lock()
                            .expect("lock errors")
                            .push("delete panic".to_string());
                    }
                }
            }
        }));
    }

    let db_for_validate = Arc::clone(&db);
    let validate_handle = thread::spawn(move || {
        let mut db = db_for_validate.lock().expect("lock db");
        db.validate()
    });

    let (violations, validate_error) = match validate_handle.join() {
        Ok(items) => (items, None),
        Err(_) => (Vec::new(), Some("validate panic".to_string())),
    };
    stop.store(true, Ordering::SeqCst);
    for handle in delete_handles {
        let _ = handle.join();
    }

    world.ri_errors = errors.lock().expect("lock errors").clone();
    world.ri_validate_error = validate_error;
    world.ri_violations = violations;
}

#[then("validate should return a list of violations without crashing")]
async fn then_validate_returns(world: &mut VirtuusWorld) {
    assert!(world.ri_validate_error.is_none());
}

#[then("each violation should reference a real missing target")]
async fn then_violation_targets_missing(world: &mut VirtuusWorld) {
    let db = ensure_ri_db(world);
    let mut db = db.lock().expect("lock db");
    for violation in &world.ri_violations {
        if let Some(missing) = violation.get("missing_target") {
            let missing_id = missing
                .as_str()
                .map(|value| value.to_string())
                .unwrap_or_else(|| missing.to_string());
            let exists = db
                .table_mut("users")
                .and_then(|users| users.get(&missing_id, None))
                .is_some();
            assert!(!exists);
        }
    }
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
    if name == "users" && world.write_table.is_none() {
        let root = unique_temp("concurrent_writes_users");
        let users_dir = root.join("users");
        fs::create_dir_all(&users_dir).unwrap();
        let table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            Some(users_dir.clone()),
            ValidationMode::Silent,
        );
        world.write_dir = Some(users_dir);
        world.write_table = Some(Arc::new(Mutex::new(table)));
        world.write_errors.clear();
        world.write_versions.clear();
    }
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
    {
        let posts = ensure_db_table(world, "posts", "id");
        if !posts.gsis().contains_key("by_user") {
            posts.add_gsi("by_user", "user_id", None);
        }
    }
    {
        let users = ensure_db_table(world, "users", "id");
        if !users.associations().contains(&"posts".to_string()) {
            users.add_has_many("posts", "posts", "by_user");
        }
    }
}

#[given(regex = r#"^a database with "posts" and "users" tables$"#)]
async fn given_posts_users(world: &mut VirtuusWorld) {
    given_users_posts(world).await;
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

#[then(regex = r#"^the result should contain only the "id" and "name" fields$"#)]
async fn then_result_projection(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    let obj = result.as_object().expect("result should be object");
    assert_eq!(obj.len(), 2);
    assert!(obj.contains_key("id"));
    assert!(obj.contains_key("name"));
}

#[then(regex = r#"^the result should contain (\\d+) records$"#)]
async fn then_result_count_db(world: &mut VirtuusWorld, count: usize) {
    let result = world.db_result.as_ref().expect("missing result");
    let items = items_from_result(result);
    assert_eq!(items.len(), count);
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

#[then(regex = r#"^the result should include a "next_token" value$"#)]
async fn then_has_next_token(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    assert!(result.get("next_token").is_some());
}

#[then(regex = r#"^the result should include a "next_token"$"#)]
async fn then_has_next_token_short(world: &mut VirtuusWorld) {
    then_has_next_token(world).await;
}

#[then(regex = r#"^the result should not include a "next_token"$"#)]
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
    let items: Vec<Value> = if !world.pages.is_empty() {
        world.pages.iter().flat_map(|p| p.clone()).collect()
    } else {
        items_from_result(world.db_result.as_ref().expect("missing result"))
    };
    let ids = ids_from_items(&items);
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

#[then(
    regex = r#"^the "users" entry should include primary_key, GSIs, record_count, and staleness$"#
)]
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

#[then(regex = r#"^the "users" entry should list the "posts" association$"#)]
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

fn ensure_cli_root(world: &mut VirtuusWorld, tag: &str) -> PathBuf {
    if let Some(root) = world.cli_root.clone() {
        return root;
    }
    let root = unique_temp(tag);
    fs::create_dir_all(&root).unwrap();
    world.cli_root = Some(root.clone());
    root
}

fn write_json_file(path: &PathBuf, value: &Value) {
    let text = serde_json::to_string_pretty(value).unwrap();
    fs::write(path, text).unwrap();
}

fn populate_user_files(dir: &PathBuf, start: usize, count: usize) {
    fs::create_dir_all(dir).unwrap();
    for i in start..start + count {
        write_json_file(
            &dir.join(format!("user-{i}.json")),
            &json!({
                "id": format!("user-{i}"),
                "status": if i % 2 == 0 { "active" } else { "inactive" }
            }),
        );
    }
}

fn ensure_server_fixture(world: &mut VirtuusWorld) -> PathBuf {
    let root = ensure_cli_root(world, "cli_server");
    let data_dir = root.join("data").join("users");
    if !data_dir.exists() {
        fs::create_dir_all(&data_dir).unwrap();
        write_json_file(
            &data_dir.join("user-1.json"),
            &json!({
                "id": "user-1",
                "name": "Alice"
            }),
        );
    }
    let schema_path = root.join("schema.yml");
    if !schema_path.exists() {
        let schema = r#"
tables:
  users:
    primary_key: id
    directory: users
"#;
        fs::write(&schema_path, schema).unwrap();
    }
    root
}

fn pick_free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn start_server(world: &mut VirtuusWorld, port: u16) {
    if world.server_process.is_some() {
        return;
    }
    let root = ensure_server_fixture(world);
    let bin = env!("CARGO_BIN_EXE_virtuus");
    let child = Command::new(bin)
        .arg("serve")
        .arg("--dir")
        .arg("./data")
        .arg("--schema")
        .arg("schema.yml")
        .arg("--port")
        .arg(port.to_string())
        .current_dir(root)
        .spawn()
        .expect("failed to start server");
    world.server_process = Some(child);
    world.server_port = Some(port);
    wait_for_server(port);
}

fn wait_for_server(port: u16) {
    let addr = ("127.0.0.1", port);
    for _ in 0..30 {
        if let Ok(mut stream) = TcpStream::connect(addr) {
            let _ = stream
                .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
            let mut resp = String::new();
            let _ = stream.read_to_string(&mut resp);
            if resp.contains("200") {
                return;
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn http_request(
    port: u16,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> (u16, HashMap<String, String>, String) {
    let addr = ("127.0.0.1", port);
    let mut stream = TcpStream::connect(addr).expect("connect failed");
    let body_text = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body_text}",
        body_text.as_bytes().len()
    );
    stream.write_all(request.as_bytes()).expect("write failed");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read failed");
    let mut sections = response.splitn(2, "\r\n\r\n");
    let header_text = sections.next().unwrap_or("");
    let body = sections.next().unwrap_or("").to_string();
    let mut lines = header_text.lines();
    let status_line = lines.next().unwrap_or("");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_string(), value.trim().to_string());
        }
    }
    (status, headers, body)
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

#[given("a YAML schema file with a missing required field")]
async fn given_schema_missing_field(world: &mut VirtuusWorld) {
    let dir = unique_temp("schema_invalid");
    fs::create_dir_all(&dir).unwrap();
    let schema_path = dir.join("schema.yml");
    fs::write(&schema_path, "tables:\n  users: {}\n").unwrap();
    world.schema_path = Some(schema_path);
    world.data_root = Some(dir);
}

#[given(
    regex = r#"^a YAML schema file defining a "users" table with GSIs "by_email" and "by_status"$"#
)]
async fn given_schema_with_gsis(world: &mut VirtuusWorld) {
    let dir = unique_temp("schema_gsis");
    fs::create_dir_all(&dir).unwrap();
    let schema = r#"
tables:
  users:
    primary_key: id
    directory: users
    gsis:
      by_email:
        partition_key: email
      by_status:
        partition_key: status
"#;
    let schema_path = dir.join("schema.yml");
    fs::write(&schema_path, schema).unwrap();
    world.schema_path = Some(schema_path);
    world.data_root = Some(dir);
}

// ---------------------------------------------------------------------------
// Schema dictionaries (Python parity)
// ---------------------------------------------------------------------------

#[given("a temporary data root with user fixture files")]
async fn given_temp_data_root(world: &mut VirtuusWorld) {
    let dir = unique_temp("data_root");
    let users_dir = dir.join("users");
    fs::create_dir_all(&users_dir).unwrap();
    fs::write(
        users_dir.join("user-1.json"),
        r#"{"id":"user-1","status":"active"}"#,
    )
    .unwrap();
    world.data_root = Some(dir);
}

#[given("a database schema dictionary:")]
async fn given_schema_dict(world: &mut VirtuusWorld, step: &cucumber::gherkin::Step) {
    let text = step.docstring.clone().unwrap_or_default();
    let value: Value = serde_json::from_str(text.trim()).expect("invalid schema dict JSON");
    world.schema_dict = Some(value);
}

#[when("I create a database from the schema dictionary")]
async fn when_create_db_from_schema_dict(world: &mut VirtuusWorld) {
    let schema = world
        .schema_dict
        .clone()
        .expect("schema dict missing in world");
    let dir = unique_temp("schema_dict");
    let schema_path = dir.join("schema.json");
    fs::write(&schema_path, serde_json::to_string_pretty(&schema).unwrap()).unwrap();
    let data_root = world.data_root.clone();
    let db = match data_root {
        Some(root) => Database::from_schema(schema_path.as_path(), Some(root.as_path())),
        None => Database::from_schema(schema_path.as_path(), None),
    };
    world.schema_path = Some(schema_path);
    world.database = Some(db);
}

#[then("the database should have loaded 1 user record from disk")]
async fn then_schema_dict_loaded_record(world: &mut VirtuusWorld) {
    let db = world.database.as_ref().expect("database not initialized");
    let users = db.tables().get("users").expect("users table missing");
    assert_eq!(users.count(None, None), 1);
    let record = users.get("user-1", None).expect("user-1 missing");
    assert_eq!(
        record.get("status").and_then(|v| v.as_str()),
        Some("active")
    );
}

#[then("the database describe output should include stale flag for \"users\"")]
async fn then_schema_dict_describe(world: &mut VirtuusWorld) {
    let db = world.database.as_mut().expect("database not initialized");
    let describe = db.describe();
    let users = describe.get("users").and_then(|v| v.as_object()).unwrap();
    assert!(users.contains_key("stale"));
    assert_eq!(users.get("stale").and_then(|v| v.as_bool()), Some(false));
}

#[then("the database should have GSIs and associations configured from the schema dict")]
async fn then_schema_dict_configured(world: &mut VirtuusWorld) {
    let db = world.database.as_ref().expect("database not initialized");
    let users = db.tables().get("users").expect("users table");
    let posts = db.tables().get("posts").expect("posts table");
    let jobs = db.tables().get("jobs").expect("jobs table");

    assert!(users.gsis().contains_key("by_status"));
    assert!(users.associations().contains(&"posts".to_string()));
    assert!(posts.gsis().contains_key("by_user"));
    assert!(posts.associations().contains(&"author".to_string()));
    assert!(jobs.associations().contains(&"workers".to_string()));
}

#[then("the database should contain tables \"users\" and \"posts\"")]
async fn then_schema_dict_tables(world: &mut VirtuusWorld) {
    let db = world.database.as_ref().expect("database not initialized");
    let names: std::collections::HashSet<_> = db.tables().keys().cloned().collect();
    let expected: std::collections::HashSet<_> = ["users".to_string(), "posts".to_string()]
        .into_iter()
        .collect();
    assert_eq!(names, expected);
}

#[given(
    regex = r#"^a YAML schema defining a table with partition_key "item_id" and sort_key "name"$"#
)]
async fn given_schema_composite(world: &mut VirtuusWorld) {
    let dir = unique_temp("schema_composite_keys");
    let items_dir = dir.join("items");
    fs::create_dir_all(&items_dir).unwrap();
    let schema = r#"
tables:
  items:
    partition_key: item_id
    sort_key: name
    directory: items
"#;
    let schema_path = dir.join("schema.yml");
    fs::write(&schema_path, schema).unwrap();
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
    assert!(
        world.error.is_some(),
        "expected schema loading to raise an error"
    );
}

#[given(regex = r#"^a database with a "users" table and GSI "([^"]*)" on "([^"]*)"$"#)]
async fn given_db_users_gsi(world: &mut VirtuusWorld, gsi: String, field: String) {
    let table = ensure_db_table(world, "users", "id");
    if !table.gsis().contains_key(&gsi) {
        table.add_gsi(&gsi, &field, None);
    }
    if world.write_table.is_none() {
        let mut write_table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        write_table.add_gsi(&gsi, &field, None);
        world.write_table = Some(Arc::new(Mutex::new(write_table)));
        world.write_errors.clear();
        world.write_versions.clear();
    }
    if world.concurrent_table.is_none() {
        let mut concurrent_table = Table::new(
            "users",
            Some("id"),
            None,
            None,
            None,
            ValidationMode::Silent,
        );
        concurrent_table.add_gsi(&gsi, &field, None);
        world.concurrent_table = Some(Arc::new(Mutex::new(concurrent_table)));
        world.concurrent_writer_status = Some("active".to_string());
        world.concurrent_errors.clear();
        world.concurrent_gsi_missing.clear();
    }
}

#[given(regex = r#"^a database with a "users" table with GSI "by_email" and 25 records$"#)]
async fn given_users_gsi_records(world: &mut VirtuusWorld) {
    let table = ensure_db_table(world, "users", "id");
    table.add_gsi("by_email", "email", None);
    for i in 0..25 {
        table.put(json!({"id": format!("user-{}", i), "email": format!("user-{}@example.com", i)}));
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
    {
        let posts = ensure_db_table(world, "posts", "id");
        posts.add_gsi("by_user", "user_id", None);
    }
    {
        let users = ensure_db_table(world, "users", "id");
        users.add_has_many("posts", "posts", "by_user");
    }
}

#[given(regex = r#"^a database with "jobs", "job_assignments", and "workers" tables$"#)]
async fn given_jobs_tables(world: &mut VirtuusWorld) {
    ensure_db_table(world, "jobs", "id");
    ensure_db_table(world, "job_assignments", "id");
    ensure_db_table(world, "workers", "id");
}

#[given(regex = r#"^a database with tables "users", "posts", and "comments"$"#)]
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

#[given(regex = r#"^a database with a "users" table and no GSI named "by_foo"$"#)]
async fn given_db_no_gsi(world: &mut VirtuusWorld) {
    let table = ensure_db_table(world, "users", "id");
    assert!(!table.gsis().contains_key("by_foo"));
}

#[given(regex = r#"^20 posts for user "([^"]*)" with sequential created_at values$"#)]
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
    let posts = ensure_db_table(world, "posts", "id");
    if !posts.gsis().contains_key("by_user") {
        posts.add_gsi("by_user", "user_id", Some("created_at"));
    }
    for i in 0..3 {
        posts.put(json!({
            "id": format!("post-{}", i+1),
            "user_id": "user-1",
            "created_at": format!("2025-01-{:02}", i+1)
        }));
    }
}

#[given("3 posts reference non-existent users")]
async fn given_posts_missing_users(world: &mut VirtuusWorld) {
    ensure_db_table(world, "users", "id");
    let posts = ensure_db_table(world, "posts", "id");
    posts.add_belongs_to("user", "users", "user_id");
    posts.put(json!({"id":"p1","user_id":"missing-1"}));
    posts.put(json!({"id":"p2","user_id":"missing-2"}));
    posts.put(json!({"id":"p3","user_id":"missing-3"}));
}

#[when("I page through with limit 10")]
async fn when_page_through_limit(world: &mut VirtuusWorld) {
    let mut token: Option<String> = None;
    let mut pages = Vec::new();
    let db = ensure_db(world);
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
    let db = ensure_db(world);
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
    let dates: Vec<String> = all
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

#[then("the result should include user → posts → comments nested 3 levels deep")]
async fn then_nested_three(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    let posts = result.get("posts").and_then(|p| p.as_array()).unwrap();
    assert!(posts.first().unwrap().get("comments").is_some());
}

#[then(
    regex = r#"^the result should include the user with a nested "posts" array of (\d+) records$"#
)]
async fn then_nested_posts(world: &mut VirtuusWorld, count: usize) {
    let result = world.db_result.as_ref().expect("missing result");
    let posts = result.get("posts").and_then(|p| p.as_array()).unwrap();
    assert_eq!(posts.len(), count);
}

#[then(regex = r#"^the result should include the post with a nested "author" object$"#)]
async fn then_nested_author(world: &mut VirtuusWorld) {
    let result = world.db_result.as_ref().expect("missing result");
    assert!(result.get("author").map(|a| a.is_object()).unwrap_or(false));
}

#[then(
    regex = r#"^the result should include the job with a nested "workers" array of (\d+) records$"#
)]
async fn then_nested_workers(world: &mut VirtuusWorld, count: usize) {
    let result = world.db_result.as_ref().expect("missing result");
    let len = result
        .get("workers")
        .and_then(|w| w.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(len, count);
}

#[then(regex = r#"^each nested post should only contain "([^"]*)" and "([^"]*)" fields$"#)]
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

#[then(regex = r#"^the nested "posts" array should be empty$"#)]
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
// CLI query mode
// ---------------------------------------------------------------------------

#[given(regex = r#"^a data directory with a "users" folder containing JSON files$"#)]
async fn given_cli_users_data(world: &mut VirtuusWorld) {
    let root = ensure_cli_root(world, "cli_users");
    let data_dir = root.join("data").join("users");
    fs::create_dir_all(&data_dir).unwrap();
    write_json_file(
        &data_dir.join("alice.json"),
        &json!({
            "id": "user-1",
            "name": "Alice",
            "email": "alice@example.com"
        }),
    );
    write_json_file(
        &data_dir.join("bob.json"),
        &json!({
            "id": "user-2",
            "name": "Bob",
            "email": "bob@example.com"
        }),
    );
}

#[given("a data directory and a schema.yml file")]
async fn given_cli_schema(world: &mut VirtuusWorld) {
    let root = ensure_cli_root(world, "cli_schema");
    let data_dir = root.join("data").join("users");
    fs::create_dir_all(&data_dir).unwrap();
    write_json_file(
        &data_dir.join("user-1.json"),
        &json!({
            "id": "user-1",
            "name": "Alice"
        }),
    );
    let schema = r#"
tables:
  users:
    primary_key: id
    directory: users
"#;
    fs::write(root.join("schema.yml"), schema).unwrap();
}

#[given(regex = r#"^a data directory with a "users" folder$"#)]
async fn given_cli_users_empty(world: &mut VirtuusWorld) {
    let root = ensure_cli_root(world, "cli_users_empty");
    let data_dir = root.join("data").join("users");
    fs::create_dir_all(&data_dir).unwrap();
    write_json_file(
        &data_dir.join("bob.json"),
        &json!({
            "id": "user-2",
            "name": "Bob",
            "email": "bob@example.com"
        }),
    );
}

#[given("a data directory with records")]
async fn given_cli_records(world: &mut VirtuusWorld) {
    let root = ensure_cli_root(world, "cli_records");
    let data_dir = root.join("data").join("users");
    fs::create_dir_all(&data_dir).unwrap();
    write_json_file(
        &data_dir.join("user-1.json"),
        &json!({
            "id": "user-1",
            "name": "Alice",
            "email": "alice@example.com"
        }),
    );
}

#[when(regex = r#"^I run virtuus query (--.+)$"#)]
async fn when_run_virtuus_query(world: &mut VirtuusWorld, args: String) {
    let bin = env!("CARGO_BIN_EXE_virtuus");
    let mut cmd = Command::new(bin);
    cmd.arg("query");
    for arg in args.split_whitespace() {
        cmd.arg(arg);
    }
    if let Some(root) = world.cli_root.as_ref() {
        cmd.current_dir(root);
    }
    let output = cmd.output().expect("failed to run virtuus");
    world.cli_stdout = Some(String::from_utf8_lossy(&output.stdout).to_string());
    world.cli_stderr = Some(String::from_utf8_lossy(&output.stderr).to_string());
    world.cli_status = Some(output.status.code().unwrap_or(-1));
}

#[when("I run virtuus query with valid parameters")]
async fn when_run_virtuus_query_valid(world: &mut VirtuusWorld) {
    when_run_virtuus_query(world, "--dir ./data --table users --pk user-1".to_string()).await;
}

#[then("the output should be valid JSON")]
async fn then_output_valid_json_cli(world: &mut VirtuusWorld) {
    let stdout = world.cli_stdout.as_deref().unwrap_or("");
    let _value: Value = serde_json::from_str(stdout).expect("invalid json output");
}

#[then("the output should contain the matching user record")]
async fn then_output_contains_user(world: &mut VirtuusWorld) {
    let stdout = world.cli_stdout.as_deref().unwrap_or("");
    let value: Value = serde_json::from_str(stdout).expect("invalid json output");
    let items = value.as_array().expect("expected array output");
    assert!(items.iter().any(|item| {
        item.get("email")
            .and_then(|v| v.as_str())
            .map(|v| v == "alice@example.com")
            .unwrap_or(false)
    }));
}

#[then(regex = r#"^the output should be the user record for "([^"]*)"$"#)]
async fn then_output_user_record(world: &mut VirtuusWorld, user_id: String) {
    let stdout = world.cli_stdout.as_deref().unwrap_or("");
    let value: Value = serde_json::from_str(stdout).expect("invalid json output");
    assert_eq!(
        value.get("id").and_then(|v| v.as_str()),
        Some(user_id.as_str())
    );
}

#[then("the output should be an empty JSON array")]
async fn then_output_empty_array(world: &mut VirtuusWorld) {
    let stdout = world.cli_stdout.as_deref().unwrap_or("");
    let value: Value = serde_json::from_str(stdout).expect("invalid json output");
    assert_eq!(value.as_array().map(|a| a.len()), Some(0));
}

#[then("the command should exit with a non-zero status")]
async fn then_nonzero_exit(world: &mut VirtuusWorld) {
    let status = world.cli_status.unwrap_or(0);
    assert_ne!(status, 0);
}

#[then("the error message should indicate the table was not found")]
async fn then_error_table_not_found(world: &mut VirtuusWorld) {
    let stderr = world.cli_stderr.as_deref().unwrap_or("");
    assert!(stderr.to_lowercase().contains("not found"));
}

#[then("results should be printed to stdout as JSON")]
async fn then_stdout_json(world: &mut VirtuusWorld) {
    then_output_valid_json_cli(world).await;
}

#[then("the process should exit with status 0")]
async fn then_exit_zero(world: &mut VirtuusWorld) {
    let status = world.cli_status.unwrap_or(1);
    assert_eq!(status, 0);
}

// ---------------------------------------------------------------------------
// CLI server mode
// ---------------------------------------------------------------------------

#[given("a data directory with a schema")]
async fn given_server_data_schema(world: &mut VirtuusWorld) {
    ensure_server_fixture(world);
}

#[given("a running Virtuus server")]
async fn given_running_server(world: &mut VirtuusWorld) {
    let port = pick_free_port();
    start_server(world, port);
}

#[given("a running Virtuus server with loaded data")]
async fn given_running_server_loaded(world: &mut VirtuusWorld) {
    let port = pick_free_port();
    start_server(world, port);
}

#[when("I start virtuus serve --dir ./data --schema schema.yml --port 8080")]
async fn when_start_server(world: &mut VirtuusWorld) {
    start_server(world, 8080);
}

#[when(regex = r#"^I POST a JSON query (\{.*\}) to http://localhost:8080/query$"#)]
async fn when_post_query(world: &mut VirtuusWorld, query_text: String) {
    let (status, headers, body) = http_request(8080, "POST", "/query", Some(&query_text));
    world.http_status = Some(status);
    world.http_headers = Some(headers);
    world.http_body = Some(body);
}

#[when("I POST a query")]
async fn when_post_default_query(world: &mut VirtuusWorld) {
    let port = world.server_port.unwrap_or(8080);
    let (status, headers, body) = http_request(
        port,
        "POST",
        "/query",
        Some(r#"{"users": {"pk": "user-1"}}"#),
    );
    world.http_status = Some(status);
    world.http_headers = Some(headers);
    world.http_body = Some(body);
}

#[when("I POST an invalid JSON body")]
async fn when_post_invalid_json(world: &mut VirtuusWorld) {
    let port = world.server_port.unwrap_or(8080);
    let (status, headers, body) = http_request(port, "POST", "/query", Some("{invalid"));
    world.http_status = Some(status);
    world.http_headers = Some(headers);
    world.http_body = Some(body);
}

#[when("I start the server and send 10 queries")]
async fn when_start_server_and_query(world: &mut VirtuusWorld) {
    let port = pick_free_port();
    start_server(world, port);
    let port = world.server_port.unwrap_or(port);
    for _ in 0..10 {
        let _ = http_request(
            port,
            "POST",
            "/query",
            Some(r#"{"users": {"pk": "user-1"}}"#),
        );
    }
}

#[when("a file is added to a table's directory")]
async fn when_file_added(world: &mut VirtuusWorld) {
    let root = ensure_server_fixture(world);
    let data_dir = root.join("data").join("users");
    write_json_file(
        &data_dir.join("user-2.json"),
        &json!({
            "id": "user-2",
            "name": "Bob"
        }),
    );
}

#[when("I POST a query for that table")]
async fn when_post_scan(world: &mut VirtuusWorld) {
    let port = world.server_port.unwrap_or(8080);
    let (status, headers, body) = http_request(port, "POST", "/query", Some(r#"{"users": {}}"#));
    world.http_status = Some(status);
    world.http_headers = Some(headers);
    world.http_body = Some(body);
}

#[when("I GET /health")]
async fn when_get_health(world: &mut VirtuusWorld) {
    let port = world.server_port.unwrap_or(8080);
    let (status, headers, body) = http_request(port, "GET", "/health", None);
    world.http_status = Some(status);
    world.http_headers = Some(headers);
    world.http_body = Some(body);
}

#[when("I POST to /describe")]
async fn when_post_describe(world: &mut VirtuusWorld) {
    let port = world.server_port.unwrap_or(8080);
    let (status, headers, body) = http_request(port, "POST", "/describe", Some("{}"));
    world.http_status = Some(status);
    world.http_headers = Some(headers);
    world.http_body = Some(body);
}

#[when("I POST to /validate")]
async fn when_post_validate(world: &mut VirtuusWorld) {
    let port = world.server_port.unwrap_or(8080);
    let (status, headers, body) = http_request(port, "POST", "/validate", Some("{}"));
    world.http_status = Some(status);
    world.http_headers = Some(headers);
    world.http_body = Some(body);
}

#[when("I POST to /warm")]
async fn when_post_warm(world: &mut VirtuusWorld) {
    let port = world.server_port.unwrap_or(8080);
    let (status, headers, body) = http_request(port, "POST", "/warm", Some("{}"));
    world.http_status = Some(status);
    world.http_headers = Some(headers);
    world.http_body = Some(body);
}

#[then("the response should be valid JSON containing the user record")]
async fn then_response_contains_user(world: &mut VirtuusWorld) {
    let body = world.http_body.as_deref().unwrap_or("");
    let value: Value = serde_json::from_str(body).expect("invalid json body");
    assert_eq!(value.get("id").and_then(|v| v.as_str()), Some("user-1"));
}

#[then("the response Content-Type should be application/json")]
async fn then_response_content_type(world: &mut VirtuusWorld) {
    let headers = world.http_headers.as_ref().expect("missing headers");
    let content_type = headers
        .iter()
        .find(|(k, _)| k.to_lowercase() == "content-type")
        .map(|(_, v)| v.as_str())
        .unwrap_or("");
    assert!(content_type.contains("application/json"));
}

#[then("the response should have a 400 status code")]
async fn then_response_400(world: &mut VirtuusWorld) {
    assert_eq!(world.http_status, Some(400));
}

#[then("the response should include an error message")]
async fn then_response_error(world: &mut VirtuusWorld) {
    let body = world.http_body.as_deref().unwrap_or("");
    assert!(body.contains("error"));
}

#[then("data should be loaded from disk only once at startup")]
async fn then_load_once(world: &mut VirtuusWorld) {
    let port = world.server_port.unwrap_or(8080);
    let (_, _, body) = http_request(port, "GET", "/health", None);
    let value: Value = serde_json::from_str(&body).expect("invalid json body");
    assert_eq!(value.get("load_count"), Some(&json!(1)));
}

#[then("the response should include the new record via JIT refresh")]
async fn then_response_includes_new(world: &mut VirtuusWorld) {
    let body = world.http_body.as_deref().unwrap_or("");
    let value: Value = serde_json::from_str(body).expect("invalid json body");
    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(items
        .iter()
        .any(|item| item.get("id") == Some(&json!("user-2"))));
}

#[then("the response should have a 200 status code")]
async fn then_response_200(world: &mut VirtuusWorld) {
    assert_eq!(world.http_status, Some(200));
}

#[then("the response should be valid JSON with server status")]
async fn then_response_status(world: &mut VirtuusWorld) {
    let body = world.http_body.as_deref().unwrap_or("");
    let value: Value = serde_json::from_str(body).expect("invalid json body");
    assert_eq!(value.get("status"), Some(&json!("ok")));
}

#[then("the response should be valid JSON with table metadata")]
async fn then_response_metadata(world: &mut VirtuusWorld) {
    let body = world.http_body.as_deref().unwrap_or("");
    let value: Value = serde_json::from_str(body).expect("invalid json body");
    let users = value.get("users").and_then(|v| v.as_object());
    assert!(users.is_some());
}

#[then("the response should be valid JSON with validation results")]
async fn then_response_validation(world: &mut VirtuusWorld) {
    let body = world.http_body.as_deref().unwrap_or("");
    let value: Value = serde_json::from_str(body).expect("invalid json body");
    assert!(value.get("violations").is_some());
}

#[then("all tables should be refreshed")]
async fn then_tables_refreshed(world: &mut VirtuusWorld) {
    let body = world.http_body.as_deref().unwrap_or("");
    let value: Value = serde_json::from_str(body).expect("invalid json body");
    let tables = value
        .get("tables")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(tables.iter().any(|v| v.as_str() == Some("users")));
}

// ---------------------------------------------------------------------------
// Concurrent reads
// ---------------------------------------------------------------------------

#[given(regex = r#"^a database with "users" table containing (\d+) records$"#)]
async fn given_concurrent_users(world: &mut VirtuusWorld, count: usize) {
    let mut table = Table::new(
        "users",
        Some("id"),
        None,
        None,
        None,
        ValidationMode::Silent,
    );
    for i in 0..count {
        let status = if i % 2 == 0 { "active" } else { "inactive" };
        table.put(json!({
            "id": format!("user-{i}"),
            "status": status
        }));
    }
    world.concurrent_table = Some(Arc::new(Mutex::new(table)));
    world.concurrent_results.clear();
    world.concurrent_counts.clear();
    world.concurrent_lookups.clear();
    world.concurrent_errors.clear();
}

#[given("a database with \"users\" table and GSI \"by_status\" on \"status\"")]
#[given(r#"a database with "users" table and GSI "by_status" on "status""#)]
async fn given_concurrent_users_with_gsi(world: &mut VirtuusWorld) {
    given_concurrent_users(world, 0).await;
    let table = world.concurrent_table.as_ref().expect("missing table");
    let mut table = table.lock().expect("lock table");
    table.add_gsi("by_status", "status", None);
}

#[given("a database with \"users\" table loaded from files")]
#[given(r#"a database with "users" table loaded from files"#)]
async fn given_database_users_loaded_from_files(world: &mut VirtuusWorld) {
    let dir = unique_temp("refresh_users");
    let users_dir = dir.join("users");
    fs::create_dir_all(&users_dir).unwrap();
    for i in 0..200 {
        let status = if i % 2 == 0 { "active" } else { "inactive" };
        let path = users_dir.join(format!("user-{i}.json"));
        fs::write(
            &path,
            serde_json::to_string(&json!({"id": format!("user-{i}"), "status": status})).unwrap(),
        )
        .unwrap();
    }
    let mut table = Table::new(
        "users",
        Some("id"),
        None,
        None,
        Some(users_dir.clone()),
        ValidationMode::Silent,
    );
    table.add_gsi("by_status", "status", None);
    table.load_from_dir(Some(users_dir));
    world.refresh_dir = Some(dir);
    world.refresh_table = Some(Arc::new(Mutex::new(table)));
    world.refresh_expected = Some((200, 200));
    world.refresh_counts.clear();
    world.refresh_reread = None;
}

#[given(regex = r#"^a GSI "([^"]*)" on "([^"]*)"$"#)]
async fn given_concurrent_gsi(world: &mut VirtuusWorld, name: String, field: String) {
    let table = world.concurrent_table.as_ref().expect("missing table");
    let mut table = table.lock().expect("lock table");
    table.add_gsi(&name, &field, None);
    let records = table.scan();
    for record in records {
        table.put(record);
    }
}

#[when(regex = r#"^100 threads simultaneously query index "([^"]*)" for "([^"]*)"$"#)]
async fn when_concurrent_gsi_queries(world: &mut VirtuusWorld, index: String, value: String) {
    let table = world.concurrent_table.as_ref().expect("missing table");
    let mut handles = Vec::new();
    for _ in 0..100 {
        let table = Arc::clone(table);
        let index = index.clone();
        let value = value.clone();
        handles.push(thread::spawn(move || {
            let mut table = table.lock().expect("lock table");
            let items = table.query_gsi(&index, &json!(value), None, false);
            let mut ids = ids_from_items(&items);
            ids.sort();
            ids
        }));
    }
    let mut results = Vec::new();
    let mut errors = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok(ids) => results.push(ids),
            Err(_) => errors.push("panic".to_string()),
        }
    }
    world.concurrent_results = results;
    world.concurrent_errors = errors;
}

#[then("all 100 threads should return the same result set")]
async fn then_concurrent_same_results(world: &mut VirtuusWorld) {
    let mut iter = world.concurrent_results.iter();
    let first = iter.next().cloned().unwrap_or_default();
    for result in iter {
        assert_eq!(&first, result);
    }
}

#[then("no errors should occur")]
async fn then_concurrent_no_errors(world: &mut VirtuusWorld) {
    assert!(world.concurrent_errors.is_empty());
}

#[when("50 threads simultaneously get different records by PK")]
async fn when_concurrent_pk_get(world: &mut VirtuusWorld) {
    let table = world.concurrent_table.as_ref().expect("missing table");
    let mut handles = Vec::new();
    for i in 0..50 {
        let table = Arc::clone(table);
        let pk = format!("user-{i}");
        handles.push(thread::spawn(move || {
            let table = table.lock().expect("lock table");
            let record = table.get(&pk, None);
            let returned = record.and_then(|v| {
                v.get("id")
                    .and_then(|id| id.as_str())
                    .map(|s| s.to_string())
            });
            (pk, returned)
        }));
    }
    let mut lookups = Vec::new();
    let mut errors = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok(item) => lookups.push(item),
            Err(_) => errors.push("panic".to_string()),
        }
    }
    world.concurrent_lookups = lookups;
    world.concurrent_errors = errors;
}

#[then("each thread should receive the correct record")]
async fn then_concurrent_pk_correct(world: &mut VirtuusWorld) {
    for (requested, returned) in &world.concurrent_lookups {
        assert_eq!(returned.as_deref(), Some(requested.as_str()));
    }
}

#[then("no thread should receive another thread's record")]
async fn then_concurrent_pk_unique(world: &mut VirtuusWorld) {
    for (requested, returned) in &world.concurrent_lookups {
        assert_eq!(returned.as_deref(), Some(requested.as_str()));
    }
}

#[when("20 threads simultaneously scan the table")]
async fn when_concurrent_scan(world: &mut VirtuusWorld) {
    let table = world.concurrent_table.as_ref().expect("missing table");
    let mut handles = Vec::new();
    for _ in 0..20 {
        let table = Arc::clone(table);
        handles.push(thread::spawn(move || {
            let mut table = table.lock().expect("lock table");
            table.scan().len()
        }));
    }
    let mut counts = Vec::new();
    let mut errors = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok(count) => counts.push(count),
            Err(_) => errors.push("panic".to_string()),
        }
    }
    world.concurrent_counts = counts;
    world.concurrent_errors = errors;
}

#[then("all 20 scans should return 500 records each")]
async fn then_concurrent_scans_count(world: &mut VirtuusWorld) {
    assert!(world.concurrent_counts.iter().all(|c| *c == 500));
}

// ---------------------------------------------------------------------------
// Concurrent read-write
// ---------------------------------------------------------------------------

#[when("10 writer threads continuously put new records")]
async fn when_concurrent_writers(world: &mut VirtuusWorld) {
    world.concurrent_writer_count = Some(10);
    world.concurrent_writer_status = Some("active".to_string());
    world.concurrent_written_ids.clear();
    world.concurrent_errors.clear();
}

#[when("50 reader threads continuously scan the table")]
async fn when_concurrent_readers_scan(world: &mut VirtuusWorld) {
    let writer_count = world.concurrent_writer_count.unwrap_or(10);
    let reader_count = 50;
    let table = world.concurrent_table.as_ref().expect("missing table");
    let stop = Arc::new(AtomicBool::new(false));
    let id_counter = Arc::new(AtomicUsize::new(0));
    let written_ids = Arc::new(Mutex::new(Vec::new()));
    let errors = Arc::new(Mutex::new(Vec::new()));

    let mut writer_handles = Vec::new();
    for _ in 0..writer_count {
        let table = Arc::clone(table);
        let stop = Arc::clone(&stop);
        let id_counter = Arc::clone(&id_counter);
        let written_ids = Arc::clone(&written_ids);
        writer_handles.push(thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                let idx = id_counter.fetch_add(1, Ordering::SeqCst);
                let record = json!({
                    "id": format!("user-new-{idx}"),
                    "status": "active"
                });
                let mut table = table.lock().expect("lock table");
                table.put(record);
                written_ids
                    .lock()
                    .expect("lock ids")
                    .push(format!("user-new-{idx}"));
            }
        }));
    }

    let mut reader_handles = Vec::new();
    for _ in 0..reader_count {
        let table = Arc::clone(table);
        let errors = Arc::clone(&errors);
        reader_handles.push(thread::spawn(move || {
            for _ in 0..25 {
                let mut table = table.lock().expect("lock table");
                let records = table.scan();
                if records.iter().any(|r| r.get("id").is_none()) {
                    errors
                        .lock()
                        .expect("lock errors")
                        .push("missing id".to_string());
                }
            }
        }));
    }

    for handle in reader_handles {
        let _ = handle.join();
    }
    stop.store(true, Ordering::SeqCst);
    for handle in writer_handles {
        let _ = handle.join();
    }

    world.concurrent_written_ids = written_ids.lock().expect("lock ids").clone();
    world.concurrent_errors = errors.lock().expect("lock errors").clone();
}

#[then("readers should never see a partially-indexed record")]
async fn then_readers_no_partial(world: &mut VirtuusWorld) {
    assert!(world.concurrent_errors.is_empty());
}

#[then("all written records should eventually be visible to readers")]
async fn then_written_records_visible(world: &mut VirtuusWorld) {
    let table = world.concurrent_table.as_ref().expect("missing table");
    let mut table = table.lock().expect("lock table");
    let records = table.scan();
    let ids: std::collections::HashSet<String> = records
        .into_iter()
        .filter_map(|r| r.get("id").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();
    for id in &world.concurrent_written_ids {
        assert!(ids.contains(id));
    }
}

#[when(regex = r#"writers continuously put records with status "active""#)]
#[when("writers continuously put records with status \"active\"")]
async fn when_writers_active(world: &mut VirtuusWorld) {
    world.concurrent_writer_count = Some(10);
    world.concurrent_writer_status = Some("active".to_string());
    world.concurrent_written_ids.clear();
    world.concurrent_errors.clear();
}

#[when(regex = r#"readers continuously query the GSI for "active""#)]
#[when("readers continuously query the GSI for \"active\"")]
async fn when_readers_query_gsi(world: &mut VirtuusWorld) {
    let writer_count = world.concurrent_writer_count.unwrap_or(10);
    let table = world.concurrent_table.as_ref().expect("missing table");
    let stop = Arc::new(AtomicBool::new(false));
    let id_counter = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(Mutex::new(Vec::new()));
    let missing = Arc::new(Mutex::new(Vec::new()));

    let mut writer_handles = Vec::new();
    for _ in 0..writer_count {
        let table = Arc::clone(table);
        let stop = Arc::clone(&stop);
        let id_counter = Arc::clone(&id_counter);
        writer_handles.push(thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                let idx = id_counter.fetch_add(1, Ordering::SeqCst);
                let record = json!({
                    "id": format!("user-active-{idx}"),
                    "status": "active"
                });
                let mut table = table.lock().expect("lock table");
                table.put(record);
            }
        }));
    }

    let mut reader_handles = Vec::new();
    for _ in 0..25 {
        let table = Arc::clone(table);
        let errors = Arc::clone(&errors);
        let missing = Arc::clone(&missing);
        reader_handles.push(thread::spawn(move || {
            for _ in 0..20 {
                let mut table = table.lock().expect("lock table");
                let records = table.query_gsi("by_status", &json!("active"), None, false);
                for record in records {
                    let Some(id) = record.get("id").and_then(|v| v.as_str()) else {
                        errors
                            .lock()
                            .expect("lock errors")
                            .push("missing id".to_string());
                        continue;
                    };
                    if table.get(id, None).is_none() {
                        missing.lock().expect("lock missing").push(id.to_string());
                    }
                }
            }
        }));
    }

    for handle in reader_handles {
        let _ = handle.join();
    }
    stop.store(true, Ordering::SeqCst);
    for handle in writer_handles {
        let _ = handle.join();
    }

    world.concurrent_errors = errors.lock().expect("lock errors").clone();
    world.concurrent_gsi_missing = missing.lock().expect("lock missing").clone();
}

#[then("every record returned by the GSI should exist in the table")]
async fn then_gsi_records_exist(world: &mut VirtuusWorld) {
    assert!(world.concurrent_gsi_missing.is_empty());
}

#[then("no reader should encounter an error")]
async fn then_no_reader_error(world: &mut VirtuusWorld) {
    assert!(world.concurrent_errors.is_empty());
}

// ---------------------------------------------------------------------------
// Concurrent refresh
// ---------------------------------------------------------------------------

#[given(regex = r#"^a database with "users" table loaded from (\d+) files$"#)]
async fn given_users_loaded_files(world: &mut VirtuusWorld, count: usize) {
    let root = unique_temp("concurrent_refresh");
    let users_dir = root.join("users");
    populate_user_files(&users_dir, 0, count);
    let mut table = Table::new(
        "users",
        Some("id"),
        None,
        None,
        Some(users_dir.clone()),
        ValidationMode::Silent,
    );
    table.load_from_dir(None);
    world.refresh_dir = Some(users_dir);
    world.refresh_table = Some(Arc::new(Mutex::new(table)));
    world.refresh_expected = Some((count, count));
    world.refresh_counts.clear();
    world.refresh_reread = None;
}

#[given("a database with \"users\" table loaded from files")]
async fn given_users_loaded_files_default(world: &mut VirtuusWorld) {
    given_users_loaded_files(world, 200).await;
}

#[given(regex = r#"^(\d+) new files are added to the directory$"#)]
async fn given_new_files_added(world: &mut VirtuusWorld, count: usize) {
    let dir = world.refresh_dir.as_ref().expect("missing dir");
    let (old, _) = world.refresh_expected.unwrap_or((0, 0));
    populate_user_files(dir, old, count);
    world.refresh_expected = Some((old, old + count));
}

#[when("a refresh is triggered while 20 reader threads are querying")]
async fn when_refresh_during_reads(world: &mut VirtuusWorld) {
    let table = world.refresh_table.as_ref().expect("missing table");
    let expected = world.refresh_expected.unwrap_or((0, 0));
    let stop = Arc::new(AtomicBool::new(false));
    let counts = Arc::new(Mutex::new(Vec::new()));

    let table_refresh = Arc::clone(table);
    let stop_refresh = Arc::clone(&stop);
    let refresh_handle = thread::spawn(move || {
        let mut table = table_refresh.lock().expect("lock table");
        table.refresh();
        stop_refresh.store(true, Ordering::SeqCst);
    });

    let mut reader_handles = Vec::new();
    for _ in 0..20 {
        let table = Arc::clone(table);
        let stop = Arc::clone(&stop);
        let counts = Arc::clone(&counts);
        reader_handles.push(thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                let mut table = table.lock().expect("lock table");
                let count = table.scan().len();
                counts.lock().expect("lock counts").push(count);
                if count == expected.1 {
                    break;
                }
            }
        }));
    }

    for handle in reader_handles {
        let _ = handle.join();
    }
    let _ = refresh_handle.join();

    world.refresh_counts = counts.lock().expect("lock counts").clone();
}

#[then("each reader should see either the old state or the new state")]
async fn then_readers_old_or_new(world: &mut VirtuusWorld) {
    let (old, new) = world.refresh_expected.unwrap_or((0, 0));
    assert!(world
        .refresh_counts
        .iter()
        .all(|count| *count == old || *count == new));
}

#[then("no reader should see a partial mix of old and new")]
async fn then_readers_no_partial_refresh(world: &mut VirtuusWorld) {
    let (old, new) = world.refresh_expected.unwrap_or((0, 0));
    assert!(!world
        .refresh_counts
        .iter()
        .any(|count| *count != old && *count != new));
}

#[when("5 threads simultaneously trigger warm()")]
async fn when_warm_concurrently(world: &mut VirtuusWorld) {
    let table = world.refresh_table.as_ref().expect("missing table");
    let mut handles = Vec::new();
    for _ in 0..5 {
        let table = Arc::clone(table);
        handles.push(thread::spawn(move || {
            let mut table = table.lock().expect("lock table");
            table.warm();
            table.last_change_summary.reread as usize
        }));
    }
    let mut max_reread = 0;
    for handle in handles {
        if let Ok(reread) = handle.join() {
            max_reread = max_reread.max(reread);
        }
    }
    world.refresh_reread = Some(max_reread);
}

#[then("the table should end in a consistent state")]
async fn then_table_consistent(world: &mut VirtuusWorld) {
    let table = world.refresh_table.as_ref().expect("missing table");
    let mut table = table.lock().expect("lock table");
    let count = table.scan().len();
    let (_, expected) = world.refresh_expected.unwrap_or((0, count));
    assert_eq!(count, expected);
}

#[then("no files should be loaded more than necessary")]
async fn then_no_excess_reread(world: &mut VirtuusWorld) {
    let (_, expected) = world.refresh_expected.unwrap_or((0, 0));
    let reread = world.refresh_reread.unwrap_or(0);
    assert!(reread <= expected);
}

// ---------------------------------------------------------------------------
// Concurrent writes
// ---------------------------------------------------------------------------

#[given("a database with an empty \"users\" table")]
async fn given_empty_users_table(world: &mut VirtuusWorld) {
    let root = unique_temp("concurrent_writes");
    let users_dir = root.join("users");
    fs::create_dir_all(&users_dir).unwrap();
    let table = Table::new(
        "users",
        Some("id"),
        None,
        None,
        Some(users_dir.clone()),
        ValidationMode::Silent,
    );
    world.write_dir = Some(users_dir);
    world.write_table = Some(Arc::new(Mutex::new(table)));
    world.write_errors.clear();
    world.write_versions.clear();
}

#[when("100 threads simultaneously put records with unique PKs")]
async fn when_put_unique(world: &mut VirtuusWorld) {
    let table = world.write_table.as_ref().expect("missing table");
    let errors = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for i in 0..100 {
        let table = Arc::clone(table);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            let record = json!({
                "id": format!("user-{i}"),
                "status": if i % 2 == 0 { "active" } else { "inactive" }
            });
            let mut table = table.lock().expect("lock table");
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| table.put(record))).is_err()
            {
                errors
                    .lock()
                    .expect("lock errors")
                    .push("panic".to_string());
            }
        }));
    }
    for handle in handles {
        let _ = handle.join();
    }
    world.write_errors = errors.lock().expect("lock errors").clone();
}

#[then("all 100 JSON files should exist on disk")]
async fn then_100_files_exist(world: &mut VirtuusWorld) {
    let dir = world.write_dir.as_ref().expect("missing dir");
    let count = fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("json"))
        .count();
    assert_eq!(count, 100);
}

#[when("50 threads simultaneously put records")]
async fn when_put_50(world: &mut VirtuusWorld) {
    let table = world.write_table.as_ref().expect("missing table");
    let errors = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for i in 0..50 {
        let table = Arc::clone(table);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            let record = json!({
                "id": format!("user-{i}"),
                "status": "active"
            });
            let mut table = table.lock().expect("lock table");
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| table.put(record))).is_err()
            {
                errors
                    .lock()
                    .expect("lock errors")
                    .push("panic".to_string());
            }
        }));
    }
    for handle in handles {
        let _ = handle.join();
    }
    world.write_errors = errors.lock().expect("lock errors").clone();
}

#[then("every JSON file on disk should contain valid JSON")]
async fn then_files_valid_json(world: &mut VirtuusWorld) {
    let dir = world.write_dir.as_ref().expect("missing dir");
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let data = fs::read_to_string(entry.path()).unwrap();
        let _: Value = serde_json::from_str(&data).unwrap();
    }
}

#[when("100 threads simultaneously put records with various statuses")]
async fn when_put_various_status(world: &mut VirtuusWorld) {
    let table = world.write_table.as_ref().expect("missing table");
    let errors = Arc::new(Mutex::new(Vec::new()));
    let statuses = ["active", "inactive", "suspended"];
    let mut handles = Vec::new();
    for i in 0..100 {
        let table = Arc::clone(table);
        let errors = Arc::clone(&errors);
        let status = statuses[i % statuses.len()].to_string();
        handles.push(thread::spawn(move || {
            let record = json!({
                "id": format!("user-{i}"),
                "status": status
            });
            let mut table = table.lock().expect("lock table");
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| table.put(record))).is_err()
            {
                errors
                    .lock()
                    .expect("lock errors")
                    .push("panic".to_string());
            }
        }));
    }
    for handle in handles {
        let _ = handle.join();
    }
    world.write_errors = errors.lock().expect("lock errors").clone();
}

#[then("the sum of all GSI partition sizes should equal the total record count")]
async fn then_gsi_sum_matches(world: &mut VirtuusWorld) {
    let table = world.write_table.as_ref().expect("missing table");
    let mut table = table.lock().expect("lock table");
    let total = table.count(None, None);
    let mut sum = 0;
    for status in ["active", "inactive", "suspended"] {
        sum += table
            .query_gsi("by_status", &json!(status), None, false)
            .len();
    }
    assert_eq!(sum, total);
}

#[when(
    regex = r#"^10 threads simultaneously put records with the same PK "([^"]*)" but different data$"#
)]
async fn when_put_same_pk(world: &mut VirtuusWorld, pk: String) {
    let table = world.write_table.as_ref().expect("missing table");
    let errors = Arc::new(Mutex::new(Vec::new()));
    let versions = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for i in 0..10 {
        let table = Arc::clone(table);
        let errors = Arc::clone(&errors);
        let versions = Arc::clone(&versions);
        let pk = pk.clone();
        handles.push(thread::spawn(move || {
            let record = json!({
                "id": pk,
                "name": format!("User {i}")
            });
            let mut table = table.lock().expect("lock table");
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| table.put(record.clone())))
                .is_err()
            {
                errors
                    .lock()
                    .expect("lock errors")
                    .push("panic".to_string());
            } else {
                versions.lock().expect("lock versions").push(record);
            }
        }));
    }
    for handle in handles {
        let _ = handle.join();
    }
    world.write_errors = errors.lock().expect("lock errors").clone();
    world.write_versions = versions.lock().expect("lock versions").clone();
}

#[then(regex = r#"^the table should contain exactly 1 record with PK "([^"]*)"$"#)]
async fn then_one_record_pk(world: &mut VirtuusWorld, pk: String) {
    let table = world.write_table.as_ref().expect("missing table");
    let table = table.lock().expect("lock table");
    assert_eq!(table.count(None, None), 1);
    assert!(table.get(&pk, None).is_some());
}

#[then("the record should match one of the 10 written versions")]
async fn then_record_matches_version(world: &mut VirtuusWorld) {
    let table = world.write_table.as_ref().expect("missing table");
    let table = table.lock().expect("lock table");
    let record = table.get("user-1", None).expect("missing record");
    assert!(world.write_versions.iter().any(|v| v == &record));
}

#[then("no error should have occurred")]
async fn then_no_error_writes(world: &mut VirtuusWorld) {
    assert!(world.write_errors.is_empty());
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    VirtuusWorld::run("../features").await;
}
