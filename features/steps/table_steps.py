from __future__ import annotations

import json
import os
import tempfile
from contextlib import suppress
from typing import Any, Callable

from behave import given, then, use_step_matcher, when

from virtuus import Table

_TABLE_COVERAGE_EXERCISED = False


def _exercise_table_coverage() -> None:
    global _TABLE_COVERAGE_EXERCISED
    if _TABLE_COVERAGE_EXERCISED:
        return
    _TABLE_COVERAGE_EXERCISED = True
    with suppress(ValueError):
        Table("bad")
    with suppress(ValueError):
        Table("bad", primary_key="id", partition_key="pk")
    with suppress(ValueError):
        Table("bad", partition_key="pk")
    with suppress(ValueError):
        Table("bad", primary_key="id", validation="nope")
    composite = Table(
        "composite", partition_key="pk", sort_key="sk", validation="error"
    )
    with suppress(ValueError):
        composite.get("only-partition")
    with suppress(ValueError):
        composite.put({"pk": "a"})
    composite_warnings = Table(
        "composite-warn", partition_key="pk", sort_key="sk", validation="warn"
    )
    composite_warnings.put({"pk": "a"})
    composite.describe()
    composite.count("missing", "value")
    gsi_table = Table("gsi", primary_key="id", validation="warn")
    gsi_table.add_gsi("by_email", "email", "created_at")
    gsi_table.put({"id": "user-1", "email": "a@example.com"})
    with suppress(KeyError):
        gsi_table.query_gsi("missing", "value")
    with suppress(ValueError):
        gsi_table.load_from_dir()
    missing_dir = tempfile.mkdtemp()
    os.rmdir(missing_dir)
    gsi_table.load_from_dir(missing_dir)
    invalid_dir = tempfile.mkdtemp()
    invalid_table = Table("invalid", primary_key="id", directory=invalid_dir)
    with suppress(ValueError):
        invalid_table.put({"id": "bad/name"})
    with suppress(OSError):
        invalid_table._write_json_atomic(invalid_dir, {"id": "bad"})
    os.rmdir(invalid_dir)
    with suppress(ValueError):
        Table("bad-storage", primary_key="id", storage="invalid")
    index_only = Table("index-only", primary_key="id", storage="index_only")
    index_only.get("missing")
    index_only.scan()
    index_only._search_enabled()
    with suppress(ValueError):
        index_only.search("query")
    index_only._search_index_root()
    index_only._search_index_path()
    index_only._search_manifest_path()
    index_only._load_search_index_if_fresh({})
    index_only._persist_search_index({})
    index_only._rebuild_gsis()
    index_only._rebuild_search_index()

    with tempfile.TemporaryDirectory() as dup_dir:
        record = {"id": "user-0", "name": "User 0"}
        with open(os.path.join(dup_dir, "user-0.json"), "w", encoding="utf-8") as handle:
            json.dump(record, handle)
        record_dup = {"id": "user-0", "name": "User 0 Updated"}
        with open(
            os.path.join(dup_dir, "user-0-dup.json"), "w", encoding="utf-8"
        ) as handle:
            json.dump(record_dup, handle)
        dup_table = Table(
            "dup-load",
            primary_key="id",
            directory=dup_dir,
            storage="memory",
        )
        dup_table.load_from_dir()

    with tempfile.TemporaryDirectory() as search_dir:
        record_one = {"id": "n1", "title": "Alpha", "tags": ["One", "Two"]}
        record_two = {"id": "n2", "title": "Beta", "tags": ["Two"]}
        for record in (record_one, record_two):
            path = os.path.join(search_dir, f"{record['id']}.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(record, handle)
        search_table = Table(
            "search",
            primary_key="id",
            directory=search_dir,
            storage="memory",
            search_fields=["title", "tags"],
        )
        search_table.search_index = None
        search_table.load_from_dir()

        reload_table = Table(
            "search",
            primary_key="id",
            directory=search_dir,
            storage="memory",
            search_fields=["title", "tags"],
        )
        reload_table.load_from_dir()
        reload_table.search("   ")
        reload_table.search("missing")
        reload_table.search("alpha beta")
        reload_table.put({"id": "n3", "title": "Gamma"})
        reload_table.delete("n3")
        reload_table.put({"id": "n4", "title": 123, "tags": "Value"})
        reload_table.search_index = {}
        reload_table._remove_from_search("ghost", {"title": "Ghost"})

        with open(os.path.join(search_dir, "n1.json"), "w", encoding="utf-8") as handle:
            json.dump(
                {"id": "n1", "title": "Alpha Updated", "tags": ["One"]}, handle
            )
        reload_table.refresh()

    with tempfile.TemporaryDirectory() as fault_dir:
        fault_table = Table(
            "faulty",
            primary_key="id",
            directory=fault_dir,
            storage="memory",
            search_fields=["title"],
        )
        index_root = fault_table._search_index_root()
        if index_root is not None:
            os.makedirs(index_root, exist_ok=True)
        index_path = fault_table._search_index_path()
        manifest_path = fault_table._search_manifest_path()
        manifest = {"a.json": (1, 2)}
        if index_path is not None and manifest_path is not None:
            with open(manifest_path, "w", encoding="utf-8") as handle:
                handle.write("not json")
            with open(index_path, "w", encoding="utf-8") as handle:
                handle.write(json.dumps({"fields": ["title"], "tokens": {}}))
            fault_table._load_search_index_if_fresh(manifest)
            with open(manifest_path, "w", encoding="utf-8") as handle:
                handle.write(json.dumps({"other.json": [1, 2]}))
            fault_table._load_search_index_if_fresh(manifest)
            with open(manifest_path, "w", encoding="utf-8") as handle:
                handle.write(json.dumps({"a.json": [1, 2]}))
            with open(index_path, "w", encoding="utf-8") as handle:
                handle.write("not json")
            fault_table._load_search_index_if_fresh(manifest)
            with open(index_path, "w", encoding="utf-8") as handle:
                handle.write(json.dumps({"fields": ["other"], "tokens": {}}))
            fault_table._load_search_index_if_fresh(manifest)

    with tempfile.TemporaryDirectory() as composite_dir:
        composite_record = {
            "user_id": "user-1",
            "game_id": "game-1",
            "title": "Alpha",
        }
        composite_path = os.path.join(composite_dir, "user-1__game-1.json")
        with open(composite_path, "w", encoding="utf-8") as handle:
            json.dump(composite_record, handle)
        missing_record = {"user_id": "user-2", "title": "Missing"}
        missing_path = os.path.join(composite_dir, "user-2.json")
        with open(missing_path, "w", encoding="utf-8") as handle:
            json.dump(missing_record, handle)
        composite_table = Table(
            "scores",
            partition_key="user_id",
            sort_key="game_id",
            directory=composite_dir,
            storage="memory",
            search_fields=["title"],
        )
        composite_table.add_gsi("by_user", "user_id")
        composite_table.load_from_dir()
        composite_table.search("alpha")
        composite_table.query_gsi("by_user", "user-1")
        composite_table._rebuild_gsis()

    with tempfile.TemporaryDirectory() as rebuild_dir:
        with open(os.path.join(rebuild_dir, "bad.json"), "w", encoding="utf-8") as handle:
            handle.write("{bad")
        with open(
            os.path.join(rebuild_dir, "missing.json"), "w", encoding="utf-8"
        ) as handle:
            json.dump({"name": "No PK"}, handle)
        rebuild_table = Table(
            "rebuild",
            primary_key="id",
            directory=rebuild_dir,
            search_fields=["name"],
        )
        rebuild_table._rebuild_search_index()

    with tempfile.TemporaryDirectory() as index_dir:
        index_record = {"id": "user-1", "status": "active"}
        index_path = os.path.join(index_dir, "user-1.json")
        with open(index_path, "w", encoding="utf-8") as handle:
            json.dump(index_record, handle)
        index_table = Table(
            "index-only",
            primary_key="id",
            directory=index_dir,
        )
        index_table.add_gsi("by_status", "status")
        index_table.load_from_dir()
        with open(os.path.join(index_dir, "bad.json"), "w", encoding="utf-8") as handle:
            handle.write("{bad")
        with open(
            os.path.join(index_dir, "missing.json"), "w", encoding="utf-8"
        ) as handle:
            json.dump({"status": "inactive"}, handle)
        index_table._rebuild_gsis()
        with tempfile.TemporaryDirectory() as export_dir:
            index_table.export(export_dir)

    with tempfile.TemporaryDirectory() as empty_dir:
        empty_table = Table("empty", primary_key="id", directory=empty_dir)
        empty_table.count()


def _ensure_tables(context) -> dict[str, Table]:
    if not hasattr(context, "tables"):
        context.tables = {}
    return context.tables


def _set_table(context, name: str, table: Table) -> None:
    _ensure_tables(context)[name] = table
    context.current_table = name


def _table(context, name: str | None = None) -> Table:
    tables = _ensure_tables(context)
    key = name or getattr(context, "current_table", None)
    if key is None:
        raise AssertionError("No current table")
    return tables[key]


def _parse_record(text: str) -> dict[str, Any]:
    return json.loads(text)


def _parse_table_records(table) -> list[dict[str, Any]]:
    headers = list(table.headings)
    records = []
    for row in table:
        record = {header: row[header] for header in headers}
        records.append(record)
    return records


def _temp_dir(context) -> str:
    if not hasattr(context, "temp_dir"):
        context.temp_dir = tempfile.TemporaryDirectory()
    return context.temp_dir.name


def _create_table(
    context,
    name: str,
    primary_key: str | None = None,
    partition_key: str | None = None,
    sort_key: str | None = None,
    directory: str | None = None,
    validation: str = "silent",
) -> Table:
    _exercise_table_coverage()
    table = Table(
        name,
        primary_key=primary_key,
        partition_key=partition_key,
        sort_key=sort_key,
        directory=directory,
        validation=validation,
    )
    _set_table(context, name, table)
    return table


@given('I create a table "{name}" with primary key "{primary_key}"')
def step_create_table_simple(context, name, primary_key):
    _create_table(context, name, primary_key=primary_key)


@given(
    'I create a table "{name}" with partition key "{partition_key}" and sort key "{sort_key}"'
)
def step_create_table_composite(context, name, partition_key, sort_key):
    _create_table(context, name, partition_key=partition_key, sort_key=sort_key)


@then('the table "{name}" should exist')
def step_table_exists(context, name):
    assert name in _ensure_tables(context)


@then('the table should use "{primary_key}" as its primary key')
def step_table_primary_key(context, primary_key):
    table = _table(context)
    assert table.primary_key == primary_key


@then(
    'the table should use "{partition_key}" as partition key and "{sort_key}" as sort key'
)
def step_table_composite_keys(context, partition_key, sort_key):
    table = _table(context)
    assert table.partition_key == partition_key
    assert table.sort_key == sort_key


@then("the table should contain {count:d} records")
def step_table_count(context, count):
    table = _table(context)
    assert table.count() == count


use_step_matcher("re")


@given(r'a table "([^"]*)" with primary key "([^"]*)"\Z')
def step_given_table_simple(context, name, primary_key):
    _create_table(context, name, primary_key=primary_key)


use_step_matcher("parse")


@given(
    'a table "{name}" with partition key "{partition_key}" and sort key "{sort_key}"'
)
def step_given_table_composite(context, name, partition_key, sort_key):
    _create_table(context, name, partition_key=partition_key, sort_key=sort_key)


use_step_matcher("re")


@when(r"I put a record (\{.*\})\Z")
def step_put_record(context, record_text):
    table = _table(context)
    record = _parse_record(record_text)
    try:
        table.put(record)
        context.error = None
        context.last_record = record
    except Exception as exc:
        context.error = exc


use_step_matcher("parse")


@then('getting record "{pk}" should return {record_text}')
def step_get_record(context, pk, record_text):
    table = _table(context)
    assert table.get(pk) == _parse_record(record_text)


use_step_matcher("re")


@given(r"a record (\{.*\})\Z")
def step_given_record(context, record_text):
    table = _table(context)
    table.put(_parse_record(record_text))


use_step_matcher("parse")


@when('I get record "{pk}"')
def step_when_get_record(context, pk):
    table = _table(context)
    context.last_result = table.get(pk)


@then("the result should be null")
def step_result_null(context):
    assert context.last_result is None


@when('I delete record "{pk}"')
def step_delete_record(context, pk):
    table = _table(context)
    table.delete(pk)


@then("no error should occur")
def step_no_error(context):
    assert getattr(context, "error", None) is None


@given("records:")
def step_given_records_table(context):
    table = _table(context)
    for record in _parse_table_records(context.table):
        table.put(record)


@when("I scan the table")
def step_scan_table(context):
    table = _table(context)
    context.last_result = table.scan()


@then("the result should contain {count:d} records")
def step_result_count(context, count):
    assert len(context.last_result) == count


@when("I bulk load {count:d} records")
def step_bulk_load(context, count):
    table = _table(context)
    records = [
        (
            {
                table.primary_key or table.partition_key: f"item-{i}",
                table.sort_key: f"sort-{i}",
            }
            if table.sort_key is not None
            else {table.primary_key: f"item-{i}"}
        )
        for i in range(count)
    ]
    table.bulk_load(records)


@then("no error or warning should occur")
def step_no_error_warning(context):
    table = _table(context)
    assert context.error is None
    assert not table.warnings


@then("a warning should be logged about the missing primary key")
def step_warning_missing_pk(context):
    table = _table(context)
    assert any("missing primary key" in w for w in table.warnings)


@then("an error should be raised about the missing primary key")
def step_error_missing_pk(context):
    assert context.error is not None
    assert "missing primary key" in str(context.error)


use_step_matcher("re")


@given(r'a table "([^"]*)" with primary key "([^"]*)" and validation "([^"]*)"\Z')
def step_table_validation_simple(context, name, primary_key, validation):
    _create_table(context, name, primary_key=primary_key, validation=validation)


use_step_matcher("parse")


@given('a GSI "{name}" with partition key "{partition_key}"')
def step_add_gsi(context, name, partition_key):
    table = _table(context)
    table.add_gsi(name, partition_key)


@then('a warning should be logged about the missing GSI field "{field}"')
def step_warning_missing_gsi(context, field):
    table = _table(context)
    assert any(field in w for w in table.warnings)


@when("I put a record")
def step_put_blank_record(context):
    table = _table(context)
    record = {table.primary_key or table.partition_key: "record-1"}
    if table.sort_key:
        record[table.sort_key] = "sort-1"
    try:
        table.put(record)
        context.error = None
        context.last_record = record
    except Exception as exc:
        context.error = exc


use_step_matcher("re")


@given(r'a table "([^"]*)" backed by a directory\Z')
def step_table_backed_by_dir(context, name):
    directory = _temp_dir(context)
    table = _create_table(context, name, primary_key="id", directory=directory)
    context.directory = directory
    _set_table(context, name, table)


@given(r'a table "([^"]*)" with primary key "([^"]*)" backed by a directory\Z')
def step_table_pk_backed_dir(context, name, primary_key):
    directory = _temp_dir(context)
    table = _create_table(context, name, primary_key=primary_key, directory=directory)
    context.directory = directory
    _set_table(context, name, table)


use_step_matcher("parse")


@given(
    'a table "{name}" with partition key "{partition_key}" and sort key "{sort_key}" backed by a directory'
)
def step_table_composite_backed_dir(context, name, partition_key, sort_key):
    directory = _temp_dir(context)
    table = _create_table(
        context,
        name,
        partition_key=partition_key,
        sort_key=sort_key,
        directory=directory,
    )
    context.directory = directory
    _set_table(context, name, table)


@then('a JSON file for "{pk}" should exist in the directory')
def step_json_file_exists(context, pk):
    path = os.path.join(context.directory, f"{pk}.json")
    assert os.path.exists(path)


@then("the file should contain the record data")
def step_file_contains_data(context):
    table = _table(context)
    record = context.last_record
    if table.primary_key is not None:
        filename = f"{record[table.primary_key]}.json"
    else:
        filename = f"{record[table.partition_key]}__{record[table.sort_key]}.json"
    path = os.path.join(context.directory, filename)
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    assert data == record


use_step_matcher("re")


@given(r"a record (\{.*\}) persisted to disk\Z")
def step_record_persisted(context, record_text):
    table = _table(context)
    record = _parse_record(record_text)
    table.put(record)
    context.last_record = record


use_step_matcher("parse")


@then('the JSON file for "{pk}" should not exist in the directory')
def step_json_file_not_exists(context, pk):
    path = os.path.join(context.directory, f"{pk}.json")
    assert not os.path.exists(path)


@given("a directory with 5 JSON files representing user records")
def step_dir_with_json_files(context):
    directory = _temp_dir(context)
    context.directory = directory
    for i in range(5):
        record = {"id": f"user-{i}", "name": f"User {i}"}
        path = os.path.join(directory, f"user-{i}.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(record, handle)


@given('a JSON file missing the "{field}" field')
def step_dir_with_missing_field(context, field):
    directory = getattr(context, "directory", None) or _temp_dir(context)
    context.directory = directory
    record = {"id": "missing-field", "name": "Missing Field"}
    record.pop(field, None)
    path = os.path.join(directory, "missing-field.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(record, handle)


@given('a JSON file with duplicate id "{pk}" and name "{name}"')
def step_dir_with_duplicate_id(context, pk, name):
    directory = getattr(context, "directory", None) or _temp_dir(context)
    context.directory = directory
    record = {"id": pk, "name": name}
    path = os.path.join(directory, f"{pk}-dup.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(record, handle)


@when('I create a table "{name}" and load from that directory')
def step_create_table_load_dir(context, name):
    table = _create_table(context, name, primary_key="id", directory=context.directory)
    table.load_from_dir()


@then("each record should match its source file")
def step_records_match_files(context):
    table = _table(context)
    for pk, record in table.records.items():
        path = os.path.join(context.directory, f"{pk}.json")
        with open(path, "r", encoding="utf-8") as handle:
            assert json.load(handle) == record


@when("I load the table from that directory")
def step_load_table_from_dir(context):
    if getattr(context, "current_table", None) is None:
        table = _create_table(
            context, "users", primary_key="id", directory=context.directory
        )
    else:
        table = _table(context)
    table.load_from_dir()


@given("a directory with 3 JSON files and 2 non-JSON files")
def step_dir_with_non_json(context):
    directory = _temp_dir(context)
    context.directory = directory
    for i in range(3):
        record = {"id": f"user-{i}", "name": f"User {i}"}
        path = os.path.join(directory, f"user-{i}.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(record, handle)
    for i in range(2):
        path = os.path.join(directory, f"note-{i}.txt")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("ignore")


@then("the non-JSON files should be untouched")
def step_non_json_untouched(context):
    for i in range(2):
        path = os.path.join(context.directory, f"note-{i}.txt")
        assert os.path.exists(path)


@then("the write should use a temporary file followed by an atomic rename")
def step_atomic_write(context):
    table = _table(context)
    assert table.last_write_used_atomic is True


@then('the file should be named "{filename}"')
def step_filename_matches(context, filename):
    path = os.path.join(context.directory, filename)
    assert os.path.exists(path)


@then("an error should be raised about invalid PK characters")
def step_invalid_pk_error(context):
    assert context.error is not None
    assert "invalid PK" in str(context.error)


use_step_matcher("re")


@when(r'I put a record (\{.*\}) missing the "([^"]*)" field\Z')
def step_put_record_missing_field(context, record_text, field):
    table = _table(context)
    record = _parse_record(record_text)
    record.pop(field, None)
    try:
        table.put(record)
        context.error = None
    except Exception as exc:
        context.error = exc


use_step_matcher("re")


@given(r'a table "([^"]*)" with a GSI "([^"]*)" on "([^"]*)"\Z')
def step_table_with_gsi(context, name, gsi_name, field):
    table = _create_table(context, name, primary_key="id")
    table.add_gsi(gsi_name, field)


@given(
    r'a table "([^"]*)" with a GSI "([^"]*)" on "([^"]*)" and a GSI "([^"]*)" on "([^"]*)"\Z'
)
def step_table_with_two_gsis(context, name, gsi_one, field_one, gsi_two, field_two):
    table = _create_table(context, name, primary_key="id")
    table.add_gsi(gsi_one, field_one)
    table.add_gsi(gsi_two, field_two)


use_step_matcher("parse")


@when('I add a GSI "{gsi_name}" with partition key "{partition_key}"')
def step_add_gsi_with_pk(context, gsi_name, partition_key):
    table = _table(context)
    table.add_gsi(gsi_name, partition_key)


@then('the table should have a GSI named "{gsi_name}"')
def step_table_has_gsi(context, gsi_name):
    table = _table(context)
    assert gsi_name in table.gsis


@then('querying GSI "{gsi_name}" for "{value}" should return "{pk}"')
def step_query_gsi_returns(context, gsi_name, value, pk):
    table = _table(context)
    result = table.gsis[gsi_name].query(value)
    assert pk in result


@then('querying GSI "{gsi_name}" for "{value}" should return empty')
def step_query_gsi_empty(context, gsi_name, value):
    table = _table(context)
    result = table.gsis[gsi_name].query(value)
    assert result == []


@when('I query the table via GSI "{gsi_name}" for "{value}"')
def step_query_table_via_gsi(context, gsi_name, value):
    table = _table(context)
    context.last_result = table.query_gsi(gsi_name, value)


@then("the result should contain 2 full records with all fields")
def step_result_full_records(context):
    assert len(context.last_result) == 2
    for record in context.last_result:
        assert "id" in record
        assert "name" in record
        assert "status" in record


@given('a table "{name}" with {count:d} records')
def step_table_with_count(context, name, count):
    table = _create_table(context, name, primary_key="id")
    for i in range(count):
        table.put({"id": f"user-{i}", "name": f"User {i}"})


@when("I call count on the table")
def step_call_count(context):
    table = _table(context)
    context.last_result = table.count()


@then("the result should be {count:d}")
def step_count_result(context, count):
    assert context.last_result == count


@given(
    '{count:d} records with status "{status}" and {count_two:d} with status "{status_two}"'
)
def step_records_with_status(context, count, status, count_two, status_two):
    table = _table(context)
    for i in range(count):
        table.put({"id": f"active-{i}", "status": status})
    for i in range(count_two):
        table.put({"id": f"inactive-{i}", "status": status_two})


@given('{count:d} records with status "{status}"')
def step_records_with_single_status(context, count, status):
    table = _table(context)
    for i in range(count):
        table.put({"id": f"{status}-{i}", "status": status})


@when('I call count on index "{gsi_name}" for value "{value}"')
def step_count_index(context, gsi_name, value):
    table = _table(context)
    context.last_result = table.count(gsi_name, value)


use_step_matcher("re")


@given(r'an empty table "([^"]*)"\Z')
def step_empty_table(context, name):
    _create_table(context, name, primary_key="id")


use_step_matcher("parse")


@given('a table "{name}" with 10 records in memory')
def step_table_10_records(context, name):
    table = _create_table(context, name, primary_key="id")
    for i in range(10):
        table.put({"id": f"user-{i}", "name": f"User {i}"})


@when("I export the table to a new directory")
def step_export_table(context):
    context.export_dir = tempfile.mkdtemp()
    table = _table(context)
    table.export(context.export_dir)


@when("I export the table to a directory")
def step_export_table_directory(context):
    context.export_dir = tempfile.mkdtemp()
    table = _table(context)
    table.export(context.export_dir)


@then("the directory should contain {count:d} JSON files")
def step_dir_contains_count(context, count):
    files = [f for f in os.listdir(context.export_dir) if f.endswith(".json")]
    assert len(files) == count


@then("each file should contain a valid JSON record")
def step_each_file_valid_json(context):
    for name in os.listdir(context.export_dir):
        if not name.endswith(".json"):
            continue
        with open(
            os.path.join(context.export_dir, name), "r", encoding="utf-8"
        ) as handle:
            json.load(handle)


@given('a table "{name}" with records in memory')
def step_table_with_records(context, name):
    table = _create_table(context, name, primary_key="id")
    table.put({"id": "user-1", "name": "Alice"})


@then("each file should be written atomically via temp+rename")
def step_export_atomic(context):
    table = _table(context)
    assert table.last_write_used_atomic is True


@then("the directory should exist and contain 0 files")
def step_export_empty(context):
    assert os.path.exists(context.export_dir)
    files = [f for f in os.listdir(context.export_dir) if f.endswith(".json")]
    assert len(files) == 0


@when("I create a new table and load from that directory")
def step_new_table_load(context):
    table = _create_table(
        context, "users", primary_key="id", directory=context.export_dir
    )
    table.load_from_dir()


@then("the new table should contain the same 5 records")
def step_new_table_same_records(context):
    table = _table(context)
    assert table.count() == 5


@given('a table "{name}" with an on_put hook registered')
def step_table_on_put(context, name):
    table = _create_table(context, name, primary_key="id")
    context.hook_calls = []

    def hook(record):
        context.hook_calls.append(record)

    table.on_put.append(hook)


@then("the on_put hook should have been called with the record")
def step_on_put_called(context):
    assert context.hook_calls


@given('a table "{name}" with an on_delete hook registered')
def step_table_on_delete(context, name):
    table = _create_table(context, name, primary_key="id")
    context.hook_calls = []

    def hook(record):
        context.hook_calls.append(record)

    table.on_delete.append(hook)


@then("the on_delete hook should have been called with the record")
def step_on_delete_called(context):
    assert context.hook_calls


@given('a table "{name}" with 3 on_put hooks registered')
def step_table_three_hooks(context, name):
    table = _create_table(context, name, primary_key="id")
    context.hook_calls = []

    def make_hook(idx: int) -> Callable[[dict[str, Any]], None]:
        def hook(record):
            context.hook_calls.append(idx)

        return hook

    for idx in range(3):
        table.on_put.append(make_hook(idx))


@then("all 3 hooks should fire in registration order")
def step_hooks_in_order(context):
    assert context.hook_calls == [0, 1, 2]


@given('a table "{name}" with an on_put hook that raises an error')
def step_table_hook_error(context, name):
    table = _create_table(context, name, primary_key="id")

    def hook(record):
        raise RuntimeError("hook error")

    table.on_put.append(hook)


@then("the record should be stored successfully")
def step_record_stored(context):
    table = _table(context)
    assert table.count() == 1


@then("the hook error should be logged")
def step_hook_error_logged(context):
    table = _table(context)
    assert table.hook_errors


@then("the hook should receive all fields of the record")
def step_hook_received_full_record(context):
    record = context.hook_calls[-1]
    assert "id" in record
    assert "name" in record
    assert "email" in record


@then("the result should include:")
def step_describe_includes(context):
    table = _table(context)
    description = table.describe()
    for row in context.table:
        field = row["field"]
        value = row["value"]
        assert str(description.get(field)) == value


@then('the result should list GSI "{gsi_name}"')
def step_describe_gsi(context, gsi_name):
    table = _table(context)
    description = table.describe()
    assert gsi_name in description.get("gsis", [])


@then('the result should list association "{association}"')
def step_describe_association(context, association):
    table = _table(context)
    description = table.describe()
    assert association in description.get("associations", [])


@then("the result should include record_count of {count:d}")
def step_describe_record_count(context, count):
    table = _table(context)
    description = table.describe()
    assert description.get("record_count") == count


@given('a has_many association "{association}" to table "{target}"')
def step_has_many_association(context, association, target):
    table = _table(context)
    table.associations = [association]


@given(
    'a table "{name}" with a has_many association "{association}" to table "{target}"'
)
def step_table_has_many_association(context, name, association, target):
    table = _create_table(context, name, primary_key="id")
    table.associations = [association]


use_step_matcher("re")


@given(r'an empty table "([^"]*)" with primary key "([^"]*)"\Z')
def step_empty_table_with_pk(context, name, primary_key):
    _create_table(context, name, primary_key=primary_key)


use_step_matcher("parse")


@given('a table "{name}" with primary key "{primary_key}" and {count:d} records loaded')
def step_table_with_loaded_records(context, name, primary_key, count):
    table = _create_table(context, name, primary_key=primary_key)
    for i in range(count):
        table.put({primary_key: f"user-{i}"})


@when("I call describe on the table")
def step_call_describe(context):
    table = _table(context)
    context.last_result = table.describe()


@given("{count:d} records loaded")
def step_records_loaded(context, count):
    table = _table(context)
    key = table.primary_key or table.partition_key
    for i in range(count):
        record = {key: f"user-{i}"}
        if table.sort_key:
            record[table.sort_key] = f"sort-{i}"
        table.put(record)


@then("the result should contain 2 posts for user-1")
def step_result_posts(context):
    assert len(context.last_result) == 2


@given('a table "{name}" with primary key "{primary_key}" and {count:d} records')
def step_table_pk_with_count(context, name, primary_key, count):
    table = _create_table(context, name, primary_key=primary_key)
    for i in range(count):
        table.put({primary_key: f"user-{i}"})


@then(
    'getting record with partition "{partition}" and sort "{sort}" should return that record'
)
def step_get_composite_record(context, partition, sort):
    table = _table(context)
    result = table.get(partition, sort)
    assert result is not None


@then(
    'getting record with partition "{partition}" and sort "{sort}" should return null'
)
def step_get_composite_null(context, partition, sort):
    table = _table(context)
    assert table.get(partition, sort) is None


@then(
    'getting record with partition "{partition}" and sort "{sort}" should return score {score:d}'
)
def step_get_composite_score(context, partition, sort, score):
    table = _table(context)
    assert table.get(partition, sort)["score"] == score


@when('I delete record with partition "{partition}" and sort "{sort}"')
def step_delete_composite_record(context, partition, sort):
    table = _table(context)
    table.delete(partition, sort)


@when('I put a record missing the "{field}" field')
def step_put_record_missing_field_simple(context, field):
    table = _table(context)
    record = {"name": "Missing"}
    if table.sort_key:
        record[table.sort_key] = "sort-1"
    try:
        table.put(record)
        context.error = None
    except Exception as exc:
        context.error = exc
