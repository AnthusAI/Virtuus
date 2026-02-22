import json
import os
import tempfile
from pathlib import Path

from behave import given, when, then

from virtuus._python import Database, Table


@given("a fresh python database")
def step_fresh_db(context):
    context.db = Database()


@given('a python table "{name}" with primary key "{pk}"')
def step_add_table_pk(context, name, pk):
    table = Table(name, primary_key=pk)
    context.db.add_table(name, table)


@given('a python gsi table "{name}" with primary key "{pk}" and gsi "{gsi}" on "{field}"')
def step_add_table_gsi(context, name, pk, gsi, field):
    table = Table(name, primary_key=pk)
    table.add_gsi(gsi, field)
    context.db.add_table(name, table)


@given('a has_many association "{assoc}" from "{source}" via GSI "{gsi}" on table "{target}"')
def step_has_many(context, assoc, source, gsi, target):
    context.db.tables[source].add_has_many(assoc, target, gsi)


@given('a belongs_to association "{assoc}" on "{source}" targeting "{target}" via "{fk}"')
def step_belongs_to(context, assoc, source, target, fk):
    context.db.tables[source].add_belongs_to(assoc, target, fk)


@given("records exist in table \"{name}\":")
def step_records_exist(context, name):
    table = context.db.tables[name]
    for row in context.table:
        record = {heading: row[heading] for heading in context.table.headings}
        table.put(record)


@when("I execute the python database query:")
def step_execute_query(context):
    raw = context.text.replace("<previous_token>", str(getattr(context, "prev_token", "0")))
    query = json.loads(raw)
    context.last_result = context.db.execute(query)
    if isinstance(context.last_result, dict) and "next_token" in context.last_result:
        context.prev_token = context.last_result["next_token"]


@then('the result should include posts for user "{user_id}"')
def step_assert_include(context, user_id):
    user = context.last_result
    assert user and user.get("id") == user_id
    posts = user.get("posts") or []
    post_ids = {p["id"] for p in posts}
    assert post_ids == {"p1", "p2"}


@then("executing the python database query:")
def step_execute_second_query(context):
    query = json.loads(context.text)
    if "<previous_token>" in context.text and hasattr(context, "prev_token"):
        query = json.loads(context.text.replace("<previous_token>", context.prev_token or "0"))
    context.second_result = context.db.execute(query)


@then("the result count should be {count:d}")
def step_assert_count(context, count):
    assert len(context.second_result["items"]) == count


@then("the result should contain {count:d} item and a next_token")
@then("the result should contain {count:d} items and a next_token")
def step_assert_pagination_with_token(context, count):
    res = context.last_result
    assert len(res["items"]) == count
    assert "next_token" in res and res["next_token"]
    context.prev_token = res["next_token"]


@then("the result should contain {count:d} item and no next_token")
@then("the result should contain {count:d} items and no next_token")
def step_assert_pagination_no_token(context, count):
    res = context.last_result
    assert len(res["items"]) == count
    assert not res.get("next_token")


@when("I validate the python database")
def step_validate(context):
    context.validation = context.db.validate()


@then('the validation should report a missing parent for "{pk}"')
def step_assert_validation(context, pk):
    assert any(v["record_pk"] == pk for v in context.validation)


@given('a temporary schema YAML file with tables "users" and "posts"')
def step_temp_schema(context):
    context.tempdir = tempfile.TemporaryDirectory()
    schema = {
        "tables": {
            "users": {"primary_key": "id"},
            "posts": {"primary_key": "id"},
        }
    }
    path = Path(context.tempdir.name) / "schema.yaml"
    path.write_text(json.dumps(schema), encoding="utf-8")
    context.schema_path = str(path)


@when("I load a python database from that schema file")
def step_load_schema(context):
    context.db = Database.from_schema(context.schema_path)


@then("warming the python database should succeed")
def step_warm(context):
    context.db.warm()


@when("I execute the python database query expecting an error:")
def step_execute_query_error(context):
    query = json.loads(context.text)
    try:
        context.db.execute(query)
    except Exception as exc:  # noqa: BLE001
        context.last_error = exc
    else:
        context.last_error = None


@then("an error should have been raised")
def step_assert_error(context):
    assert context.last_error is not None
