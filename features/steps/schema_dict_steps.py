import json
import os
import tempfile

from behave import given, when, then

from virtuus._python.database import Database


@given("a temporary data root with user fixture files")
def step_temp_data_root(context):
    context.tempdir = tempfile.TemporaryDirectory()
    users_dir = os.path.join(context.tempdir.name, "users")
    os.makedirs(users_dir, exist_ok=True)
    user_path = os.path.join(users_dir, "user-1.json")
    with open(user_path, "w", encoding="utf-8") as fh:
        json.dump({"id": "user-1", "status": "active"}, fh)
    context.data_root = context.tempdir.name


@given("a database schema dictionary:")
def step_schema_dict(context):
    context.schema_dict = json.loads(context.text)


@when("I create a database from the schema dictionary")
def step_build_db(context):
    context.db = Database.from_schema_dict(context.schema_dict, data_root=context.data_root)


@then("the database should have loaded 1 user record from disk")
def step_assert_load(context):
    users = context.db.tables["users"]
    assert users.count() == 1
    record = users.get("user-1")
    assert record["status"] == "active"


@then('the database describe output should include stale flag for "users"')
def step_assert_describe(context):
    desc = context.db.describe()["users"]
    assert "stale" in desc
    # Freshly loaded table should be considered not stale
    assert desc["stale"] is False


@then("the database should have GSIs and associations configured from the schema dict")
def step_assert_schema_config(context):
    users = context.db.tables["users"]
    posts = context.db.tables["posts"]
    jobs = context.db.tables["jobs"]

    assert "by_status" in users.gsis
    assert "posts" in users.association_defs
    assert users.association_defs["posts"]["kind"] == "has_many"

    assert "by_user" in posts.gsis
    assert "author" in posts.association_defs
    assert posts.association_defs["author"]["kind"] == "belongs_to"

    assert "workers" in jobs.association_defs
    assert jobs.association_defs["workers"]["kind"] == "has_many_through"
