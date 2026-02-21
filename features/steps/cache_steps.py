from __future__ import annotations

import json
import os
import tempfile
import time

from behave import given, then, when

from virtuus import Database, Table


def _temp_dir(context) -> str:
    if not hasattr(context, "temp_dirs"):
        context.temp_dirs = []
    path = tempfile.mkdtemp()
    context.temp_dirs.append(path)
    return path


def _write_records(directory: str, count: int, start: int = 0) -> None:
    for i in range(start, start + count):
        record = {"id": f"user-{i}", "name": f"User {i}", "status": "active"}
        path = os.path.join(directory, f"user-{i}.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(record, handle)


def _table_from_dir(context, name: str, directory: str, check_interval: int = 0, auto_refresh: bool = True) -> Table:
    table = Table(name, primary_key="id", directory=directory, check_interval=check_interval, auto_refresh=auto_refresh)
    table.load_from_dir()
    context.tables = getattr(context, "tables", {})
    context.tables[name] = table
    context.current_table = name
    return table


def _current_table(context) -> Table:
    return context.tables[context.current_table]


@given('a table "{name}" loaded from a directory with 5 JSON files')
def step_table_loaded_5(context, name):
    directory = _temp_dir(context)
    _write_records(directory, 5)
    _table_from_dir(context, name, directory)
    context.directory = directory


@given('a table "{name}" loaded from a directory')
def step_table_loaded_dir(context, name):
    directory = _temp_dir(context)
    _write_records(directory, 3)
    _table_from_dir(context, name, directory)
    context.directory = directory


@given('a table "{name}" loaded from a directory with check_interval of {seconds:d} seconds')
def step_table_check_interval(context, name, seconds):
    directory = _temp_dir(context)
    _write_records(directory, 3)
    _table_from_dir(context, name, directory, check_interval=seconds)
    context.directory = directory


@given('a table "{name}" loaded from a directory with auto_refresh disabled')
def step_table_auto_refresh_off(context, name):
    directory = _temp_dir(context)
    _write_records(directory, 3)
    _table_from_dir(context, name, directory, auto_refresh=False)
    context.directory = directory


@given('a table "{name}" loaded from {count:d} JSON files with a GSI on "status"')
def step_table_with_gsi_records(context, name, count):
    directory = _temp_dir(context)
    _write_records(directory, count)
    table = _table_from_dir(context, name, directory)
    table.add_gsi("by_status", "status")
    context.directory = directory


@given('a table "{name}" loaded from 100 JSON files')
def step_table_100_files(context, name):
    directory = _temp_dir(context)
    _write_records(directory, 100)
    _table_from_dir(context, name, directory)
    context.directory = directory


@when("a JSON file in the directory is modified")
def step_modify_file(context):
    directory = context.directory
    path = os.path.join(directory, "user-0.json")
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    data["name"] = "Updated"
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle)


@when("a new JSON file is added to the directory")
def step_add_file(context):
    directory = context.directory
    idx = len([f for f in os.listdir(directory) if f.endswith(".json")])
    record = {"id": f"user-{idx}", "name": f"User {idx}", "status": "active"}
    path = os.path.join(directory, f"user-{idx}.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(record, handle)


@when("a JSON file is removed from the directory")
def step_delete_file(context):
    directory = context.directory
    path = os.path.join(directory, "user-0.json")
    if os.path.exists(path):
        os.remove(path)


@when("a JSON file is deleted from the directory")
def step_delete_file_incremental(context):
    step_delete_file(context)


@when("2 new JSON files are added to the directory")
def step_add_two_files(context):
    directory = context.directory
    current = len([f for f in os.listdir(directory) if f.endswith(".json")])
    for offset in range(2):
        idx = current + offset
        record = {"id": f"user-{idx}", "name": f"User {idx}", "status": "active"}
        with open(os.path.join(directory, f"user-{idx}.json"), "w", encoding="utf-8") as handle:
            json.dump(record, handle)


@when("1 JSON file is modified on disk")
def step_modify_file_on_disk(context):
    step_modify_file(context)


@when("I check if the table is stale")
def step_check_stale(context):
    table = _current_table(context)
    context.is_stale = table.is_stale()


@when("I check if the table is stale within 5 seconds of the last check")
def step_check_stale_within_interval(context):
    table = _current_table(context)
    table._last_check_time = time.time()
    context.is_stale = table.is_stale()


@then("it should report fresh")
def step_assert_fresh(context):
    assert context.is_stale is False


@then("it should report stale")
def step_assert_stale(context):
    assert context.is_stale is True


@when("I query the table")
def step_query_table(context):
    table = _current_table(context)
    context.last_result = table.scan()


@then("the new record should be included in results")
def step_new_record_in_results(context):
    ids = {record["id"] for record in context.last_result}
    assert any("user-" in i and i not in {"user-0", "user-1", "user-2"} for i in ids)


@then("the new record should not be included in results")
def step_new_record_not_in_results(context):
    ids = {record["id"] for record in context.last_result}
    assert not any("user-" in i and i not in {"user-0", "user-1", "user-2"} for i in ids)


@then("the new record should be included in results after warm")
def step_new_record_after_warm(context):
    table = _current_table(context)
    ids = {record["id"] for record in table.scan()}
    assert any("user-" in i and i not in {"user-0", "user-1", "user-2"} for i in ids)


@then("the table should report fresh afterward")
def step_report_fresh_after(context):
    table = _current_table(context)
    assert table.is_stale() is False


@when("I query the table twice with no file changes between")
def step_query_twice(context):
    table = _current_table(context)
    context.refresh_calls = 0

    def on_refresh(summary):
        context.refresh_calls += 1

    table.on_refresh.append(on_refresh)
    table.scan()
    table.scan()


@then("the second query should not trigger a refresh")
def step_second_query_no_refresh(context):
    assert context.refresh_calls == 0


@when("a JSON file is modified to change a GSI-indexed field")
def step_modify_gsi_field(context):
    directory = context.directory
    path = os.path.join(directory, "user-0.json")
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    data["status"] = "inactive"
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle)


@when("the table is refreshed")
def step_refresh_table(context):
    table = _current_table(context)
    context.last_summary = table.refresh()


@then("the table should contain {count:d} records")
def step_table_record_count(context, count):
    table = _current_table(context)
    assert table.count() == count


@then("all GSIs should include the 2 new records")
def step_gsi_has_new(context):
    table = _current_table(context)
    ids = table.gsis["by_status"].query("active")
    assert len(ids) >= 2


@then("the deleted record should be absent from all GSIs")
def step_deleted_absent_gsi(context):
    table = _current_table(context)
    for gsi in table.gsis.values():
        assert not gsi.query("active")


@then("the record should reflect the updated field value")
def step_record_updated(context):
    table = _current_table(context)
    record = table.get("user-0")
    assert record is not None
    assert record.get("status") == "inactive"


@then("GSI queries should return the record under the new index value")
def step_gsi_updated(context):
    table = _current_table(context)
    result = table.gsis["by_status"].query("inactive")
    assert "user-0" in result


@then("only 1 file should be re-read from disk")
def step_only_one_reread(context):
    summary = getattr(context, "last_summary", {})
    assert summary.get("reread") == 1


@when("I call check on the table")
def step_call_check(context):
    table = _current_table(context)
    context.last_summary = table.check()


@then('the result should report {added:d} added, {modified:d} modified, {deleted:d} deleted')
def step_assert_summary(context, added, modified, deleted):
    summary = context.last_summary
    assert summary["added"] == added
    assert summary["modified"] == modified
    assert summary["deleted"] == deleted


@then("the table should still contain 5 records")
def step_table_still_five(context):
    table = _current_table(context)
    assert table.count() == 5


@given('a table "{name}" with an on_refresh hook registered')
def step_table_on_refresh(context, name):
    directory = _temp_dir(context)
    _write_records(directory, 1)
    table = _table_from_dir(context, name, directory)
    context.refresh_summaries = []

    def hook(summary):
        context.refresh_summaries.append(summary)

    table.on_refresh.append(hook)
    context.directory = directory


@when("the table is refreshed")
def step_refresh_table_hook(context):
    table = _current_table(context)
    context.last_summary = table.refresh()


@then("the on_refresh hook should receive a change summary")
def step_hook_receives_summary(context):
    assert context.refresh_summaries


@then("the summary should include counts of added, modified, and deleted files")
def step_summary_counts_present(context):
    summary = context.refresh_summaries[-1]
    for key in ("added", "modified", "deleted"):
        assert key in summary


@given('a database with tables "{name1}" and "{name2}" loaded from directories')
def step_database_two_tables(context, name1, name2):
    db = Database()
    dir1 = _temp_dir(context)
    dir2 = _temp_dir(context)
    _write_records(dir1, 1)
    _write_records(dir2, 1)
    table1 = _table_from_dir(context, name1, dir1)
    table2 = _table_from_dir(context, name2, dir2)
    db.add_table(name1, table1)
    db.add_table(name2, table2)
    context.database = db
    context.directory = dir1
    context.directory_two = dir2


@when("I call warm on the database")
def step_warm_database(context):
    context.database.warm()


@then("both tables should contain their new records")
def step_db_tables_have_records(context):
    db = context.database
    for table in db.tables.values():
        assert table.count() >= 1


@given('a database with tables loaded from directories')
def step_database_loaded(context):
    db = Database()
    dir1 = _temp_dir(context)
    _write_records(dir1, 2)
    table = _table_from_dir(context, "users", dir1)
    db.add_table("users", table)
    context.database = db


@when("I call warm with no file changes")
def step_warm_no_changes(context):
    context.database.warm()


@then("no files should be re-read from disk")
def step_no_files_reread(context):
    for table in context.database.tables.values():
        assert table.last_change_summary.get("reread", 0) == 0


@when("I call warm on the table")
def step_warm_table(context):
    _current_table(context).warm()
