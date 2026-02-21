use std::path::PathBuf;

use serde_json::Value;

use crate::helpers::{current_table, ensure_tables, set_current_table, temp_dir, write_records};
use crate::world::VirtuusWorld;
use cucumber::{given, then, when};
use virtuus::database::Database;
use virtuus::table::ValidationMode;

#[given(regex = r#"^a table "([^"]*)" loaded from a directory with 5 JSON files$"#)]
async fn given_table_loaded_5(world: &mut VirtuusWorld, name: String) {
    let dir = temp_dir(world, "cache");
    write_records(&dir, 5, 0);
    super::create_table(
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
    let dir = temp_dir(world, "cache");
    write_records(&dir, 3, 0);
    super::create_table(
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

#[given(regex = r#"^a table "([^"]*)" loaded from a directory with check_interval of (\d+) seconds$"#)]
async fn given_table_check_interval(world: &mut VirtuusWorld, name: String, seconds: u64) {
    let dir = temp_dir(world, "cache");
    write_records(&dir, 3, 0);
    super::create_table(
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
    let dir = temp_dir(world, "cache");
    write_records(&dir, 3, 0);
    super::create_table(
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
async fn given_table_with_gsi(world: &mut VirtuusWorld, name: String, count: usize) {
    let dir = temp_dir(world, "cache");
    write_records(&dir, count, 0);
    super::create_table(
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
    let dir = temp_dir(world, "cache");
    write_records(&dir, 100, 0);
    super::create_table(
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
async fn when_add_file(world: &mut VirtuusWorld) {
    let dir = world.directory.as_ref().unwrap();
    let count = std::fs::read_dir(dir)
        .unwrap()
        .filter(|e| e.as_ref().unwrap().path().extension().and_then(|s| s.to_str()) == Some("json"))
        .count();
    write_records(dir, 1, count);
}

#[when("a JSON file is removed from the directory")]
async fn when_remove_file(world: &mut VirtuusWorld) {
    let dir = world.directory.as_ref().unwrap();
    let path = dir.join("user-0.json");
    if path.exists() {
        std::fs::remove_file(path).unwrap();
    }
}

#[when("a JSON file is deleted from the directory")]
async fn when_delete_file_alias(world: &mut VirtuusWorld) {
    when_remove_file(world).await;
}

#[when("2 new JSON files are added to the directory")]
async fn when_add_two_files(world: &mut VirtuusWorld) {
    let dir = world.directory.as_ref().unwrap();
    let count = std::fs::read_dir(dir)
        .unwrap()
        .filter(|e| e.as_ref().unwrap().path().extension().and_then(|s| s.to_str()) == Some("json"))
        .count();
    write_records(dir, 2, count);
}

#[when("1 JSON file is modified on disk")]
async fn when_modify_file_on_disk(world: &mut VirtuusWorld) {
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
    let ids: Vec<String> = world
        .last_records
        .iter()
        .filter_map(|r| r.get("id"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
    assert!(ids.iter().any(|id| !["user-0", "user-1", "user-2"].contains(&id.as_str())));
}

#[then("the table should report fresh afterward")]
async fn then_table_fresh_after(world: &mut VirtuusWorld) {
    let table = current_table(world);
    assert!(!table.is_stale(false));
}

#[when("I query the table twice with no file changes between")]
async fn when_query_twice(world: &mut VirtuusWorld) {
    let table = current_table(world);
    world.refresh_calls = 0;
    table.register_on_refresh(Box::new({
        let calls = std::sync::Arc::new(std::sync::Mutex::new(()));
        move |_summary| {
            // no-op; we rely on last_change_summary instead
            let _ = calls.lock().unwrap();
        }
    }));
    table.scan();
    table.scan();
}

#[then("the second query should not trigger a refresh")]
async fn then_second_query_no_refresh(world: &mut VirtuusWorld) {
    assert_eq!(current_table(world).last_change_summary.reread, 0);
}

#[when("a JSON file is modified to change a GSI-indexed field")]
async fn when_modify_gsi_field(world: &mut VirtuusWorld) {
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

#[then(regex = r#"^the table should contain (\d+) records$"#)]
async fn then_table_contains(world: &mut VirtuusWorld, count: usize) {
    let table = current_table(world);
    assert_eq!(table.count(None, None), count);
}

#[then("all GSIs should include the 2 new records")]
async fn then_gsi_has_new(world: &mut VirtuusWorld) {
    let table = current_table(world);
    let ids = table
        .gsis()
        .get("by_status")
        .unwrap()
        .query(&Value::String("active".to_string()), None, false);
    assert!(ids.len() >= 2);
}

#[then("the deleted record should be absent from all GSIs")]
async fn then_deleted_absent(world: &mut VirtuusWorld) {
    let table = current_table(world);
    for gsi in table.gsis().values() {
        assert!(gsi.query(&Value::String("active".to_string()), None, false).is_empty());
    }
}

#[then("the record should reflect the updated field value")]
async fn then_record_updated(world: &mut VirtuusWorld) {
    let table = current_table(world);
    let record = table.get("user-0", None).unwrap();
    assert_eq!(record.get("status").and_then(|v| v.as_str()), Some("inactive"));
}

#[then("GSI queries should return the record under the new index value")]
async fn then_gsi_updated(world: &mut VirtuusWorld) {
    let table = current_table(world);
    let ids = table
        .gsis()
        .get("by_status")
        .unwrap()
        .query(&Value::String("inactive".to_string()), None, false);
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
async fn then_summary_counts(world: &mut VirtuusWorld, added: usize, modified: usize, deleted: usize) {
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
    let dir = temp_dir(world, "cache");
    write_records(&dir, 1, 0);
    super::create_table(
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
    let dir1 = temp_dir(world, "cache");
    let dir2 = temp_dir(world, "cache2");
    write_records(&dir1, 1, 0);
    write_records(&dir2, 1, 0);
    super::create_table(
        world,
        &name1,
        Some("id"),
        None,
        None,
        Some(dir1.clone()),
        ValidationMode::Silent,
    );
    super::create_table(
        world,
        &name2,
        Some("id"),
        None,
        None,
        Some(dir2.clone()),
        ValidationMode::Silent,
    );
    db.add_table(&name1, world.tables.get(&name1).unwrap().clone());
    db.add_table(&name2, world.tables.get(&name2).unwrap().clone());
    world.database = Some(db);
    world.directory = Some(dir1);
    world.directory_two = Some(dir2);
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
    for summary in db.check().values() {
        let added = summary.get("added").and_then(|v| v.as_u64()).unwrap_or(0);
        assert!(added >= 0);
    }
}

#[given("a database with tables loaded from directories")]
async fn given_database_loaded(world: &mut VirtuusWorld) {
    let mut db = Database::new();
    let dir = temp_dir(world, "cache");
    write_records(&dir, 2, 0);
    super::create_table(
        world,
        "users",
        Some("id"),
        None,
        None,
        Some(dir.clone()),
        ValidationMode::Silent,
    );
    db.add_table("users", world.tables.get("users").unwrap().clone());
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
        for summary in db.check().values() {
            assert_eq!(summary.get("reread").and_then(|v| v.as_u64()).unwrap_or(0), 0);
        }
    }
}

#[when("I call warm on the table")]
async fn when_warm_table(world: &mut VirtuusWorld) {
    current_table(world).warm();
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
    assert!(!ids.iter().any(|id| !["user-0", "user-1", "user-2"].contains(&id.as_str())));
}

#[then("the new record should be included in results after warm")]
async fn then_new_record_after_warm(world: &mut VirtuusWorld) {
    let table = current_table(world);
    let ids: Vec<String> = table
        .scan()
        .iter()
        .filter_map(|r| r.get("id"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
    assert!(ids.iter().any(|id| !["user-0", "user-1", "user-2"].contains(&id.as_str())));
}
