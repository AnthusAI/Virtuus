from __future__ import annotations

import json
import os
import tempfile
from typing import Any

import yaml
from behave import given, then, when, use_step_matcher

from virtuus import Database, Table


def _ensure_db(context) -> Database:
    if not hasattr(context, "db"):
        context.db = Database()
    return context.db


def _parse_json(text: str) -> Any:
    return json.loads(text)


def _table(context, name: str, primary_key: str = "id") -> Table:
    db = _ensure_db(context)
    if not hasattr(context, "tables"):
        context.tables = {}
    if name not in context.tables:
        table = Table(name, primary_key=primary_key)
        db.add_table(name, table)
        context.tables[name] = table
    return context.tables[name]


def _add_records(table: Table, rows: list[dict[str, Any]]) -> None:
    for record in rows:
        table.put(record)


def _table_from_step_table(step_table) -> list[dict[str, Any]]:
    headers = list(step_table.headings)
    rows = []
    for row in step_table:
        rows.append({header: row[header] for header in headers})
    return rows


def _store_result(context, result):
    context.result = result
    if isinstance(result, dict) and "next_token" in result:
        context.last_token = result["next_token"]
    else:
        context.last_token = None
    if isinstance(result, dict) and "items" in result:
        context.last_result = result["items"]
    else:
        context.last_result = result


@given('a database with a "{table}" table')
def step_db_with_table(context, table):
    _table(context, table)


use_step_matcher("re")


@given(r'a database with a "([^"]*)" table containing (\{.*\})')
def step_db_table_with_record(context, table, record_text):
    table_ref = _table(context, table)
    table_ref.put(_parse_json(record_text))


use_step_matcher("parse")


@given('a database with a "{table}" table containing:')
def step_db_table_with_rows(context, table):
    rows = _table_from_step_table(context.table)
    _add_records(_table(context, table), rows)


@given('a database with a "{table}" table containing {count:d} records')
def step_db_table_with_count(context, table, count):
    records = [{"id": f"{table[:-1]}-{i}"} for i in range(count)]
    _add_records(_table(context, table), records)


@given('a database with tables "users", "posts", and "comments"')
def step_db_three_tables(context):
    db = _ensure_db(context)
    for name in ("users", "posts", "comments"):
        db.add_table(name, Table(name, primary_key="id"))


@given(
    r'a database with a "([^"]*)" table and GSI "([^"]*)" on "([^"]*)"(?: sorted by "([^"]*)")?'
)
def step_db_table_gsi(context, table, gsi, field, sort=None):
    table_ref = _table(context, table)
    table_ref.add_gsi(gsi, field, sort)


@given('a database with a "users" table and no GSI named "by_foo"')
def step_db_no_gsi(context):
    table = _table(context, "users")
    table.gsis.pop("by_foo", None)


@given('a database with a "posts" table and GSI "by_user" on "user_id"')
def step_posts_gsi(context):
    table = _table(context, "posts")
    table.add_gsi("by_user", "user_id")


@given(
    'a database with a "posts" table and GSI "by_user" on "user_id" sorted by "created_at"'
)
def step_posts_gsi_sorted(context):
    table = _table(context, "posts")
    table.add_gsi("by_user", "user_id", "created_at")


@given('a database with a "users" table with GSI "by_email" and 25 records')
def step_db_users_gsi_records(context):
    table = _table(context, "users")
    table.add_gsi("by_email", "email")
    rows = [{"id": f"user-{i}", "email": f"user-{i}@example.com"} for i in range(25)]
    _add_records(table, rows)


@given("posts for user-1 with created_at values {dates}")
def step_posts_dates(context, dates):
    values = [d.strip().strip('"') for d in dates.split(",")]
    rows = [
        {
            "id": f"post-{i+1}",
            "user_id": "user-1",
            "title": f"Post {i+1}",
            "created_at": v,
        }
        for i, v in enumerate(values)
    ]
    _add_records(_table(context, "posts"), rows)


@given("3 posts for user-1 with ascending created_at values")
def step_posts_three(context):
    step_posts_dates(context, '"2025-01-01", "2025-06-01", "2025-12-01"')


@given('3 posts for user "user-1" with ascending created_at values')
def step_posts_three_user(context):
    step_posts_three(context)


@given('20 posts for user "user-1" with sequential created_at values')
def step_posts_twenty(context):
    rows = [
        {
            "id": f"post-{i+1}",
            "user_id": "user-1",
            "created_at": f"2025-01-{i+1:02d}",
        }
        for i in range(20)
    ]
    _add_records(_table(context, "posts"), rows)


@given('30 posts for user "{user_id}"')
def step_posts_thirty(context, user_id):
    rows = [
        {"id": f"post-{i+1}", "user_id": user_id, "title": f"Post {i+1}"}
        for i in range(30)
    ]
    _add_records(_table(context, "posts"), rows)


@given('a database with "users" and "posts" tables')
def step_users_posts(context):
    users = _table(context, "users")
    posts = _table(context, "posts")
    posts.add_gsi("by_user", "user_id")
    users.add_has_many("posts", "posts", "by_user")


@given('a database with "posts" and "users" tables')
def step_posts_users(context):
    step_users_posts(context)


@given('a database with "users" having a has_many "posts" association')
def step_users_has_many_assoc(context):
    users = _table(context, "users")
    posts = _table(context, "posts")
    posts.add_gsi("by_user", "user_id")
    users.add_has_many("posts", "posts", "by_user")
    users.put({"id": "user-1"})
    posts.put({"id": "post-1", "user_id": "user-1"})


@given('user "{user_id}" has 3 posts')
def step_user_three_posts(context, user_id):
    users = _table(context, "users")
    posts = _table(context, "posts")
    users.put({"id": user_id, "name": "User"})
    for i in range(3):
        posts.put({"id": f"post-{i+1}", "user_id": user_id, "title": f"Post {i+1}"})


@given('user "{user_id}" has posts')
def step_user_has_posts(context, user_id):
    users = _table(context, "users")
    posts = _table(context, "posts")
    users.put({"id": user_id})
    posts.add_gsi("by_user", "user_id")
    users.add_has_many("posts", "posts", "by_user")
    for i in range(2):
        posts.put({"id": f"post-{i+1}", "user_id": user_id, "title": f"Post {i+1}"})


@given('user "{user_id}" has no posts')
def step_user_no_posts(context, user_id):
    users = _table(context, "users")
    users.put({"id": user_id})
    posts = _table(context, "posts")
    posts.add_gsi("by_user", "user_id")
    users = _table(context, "users")
    users.add_has_many("posts", "posts", "by_user")


@given('post "{post_id}" belongs to user "{user_id}"')
def step_post_belongs(context, post_id, user_id):
    posts = _table(context, "posts")
    users = _table(context, "users")
    posts.add_belongs_to("author", "users", "user_id")
    users.put({"id": user_id, "name": "User"})
    posts.put({"id": post_id, "user_id": user_id, "title": "Post"})


@given('a database with "jobs", "job_assignments", and "workers" tables')
def step_jobs_workers(context):
    jobs = _table(context, "jobs")
    assignments = _table(context, "job_assignments")
    workers = _table(context, "workers")
    assignments.add_gsi("by_job", "job_id")
    jobs.add_has_many_through(
        "workers", "job_assignments", "by_job", "workers", "worker_id"
    )
    _ = workers


@given('job "{job_id}" has 2 workers through job_assignments')
def step_job_workers(context, job_id):
    jobs = _table(context, "jobs")
    jobs.put({"id": job_id})
    workers = _table(context, "workers")
    workers.put({"id": "worker-1"})
    workers.put({"id": "worker-2"})
    assignments = _table(context, "job_assignments")
    assignments.add_gsi("by_job", "job_id")
    assignments.put({"id": "assign-1", "job_id": job_id, "worker_id": "worker-1"})
    assignments.put({"id": "assign-2", "job_id": job_id, "worker_id": "worker-2"})


@given('a database with "users", "posts", and "comments" tables')
def step_users_posts_comments(context):
    users = _table(context, "users")
    posts = _table(context, "posts")
    comments = _table(context, "comments")
    posts.add_gsi("by_user", "user_id")
    comments.add_gsi("by_post", "post_id")
    users.add_has_many("posts", "posts", "by_user")
    posts.add_has_many("comments", "comments", "by_post")


@given('user "{user_id}" has posts, and each post has comments')
def step_user_posts_comments(context, user_id):
    users = _table(context, "users")
    posts = _table(context, "posts")
    comments = _table(context, "comments")
    users.put({"id": user_id})
    posts.add_gsi("by_user", "user_id")
    comments.add_gsi("by_post", "post_id")
    users.add_has_many("posts", "posts", "by_user")
    posts.add_has_many("comments", "comments", "by_post")
    for i in range(2):
        post_id = f"post-{i+1}"
        posts.put({"id": post_id, "user_id": user_id})
        for j in range(2):
            comments.put({"id": f"comment-{i+1}-{j+1}", "post_id": post_id})


@when("I execute {query_text}")
def step_execute(context, query_text):
    expects_token = query_text.endswith("and receive a next_token")
    if expects_token:
        query_text = query_text.removesuffix(" and receive a next_token").strip()
    if "<previous_token>" in query_text and hasattr(context, "previous_token"):
        query_text = query_text.replace(
            "<previous_token>", context.previous_token or ""
        )
    db = _ensure_db(context)
    try:
        result = db.execute(_parse_json(query_text))
        _store_result(context, result)
        context.error = None
    except Exception as exc:  # noqa: BLE001
        context.error = exc
        return
    if expects_token:
        assert context.result.get("next_token")
        context.saved_page = list(context.result.get("items", []))
        context.previous_token = context.result["next_token"]


@when("I call describe on the database")
def step_call_describe(context):
    context.result = _ensure_db(context).describe()


@when("I call validate on the database")
def step_call_validate(context):
    context.result = _ensure_db(context).validate()


@when("I call Database.from_schema with that file and a data directory")
def step_call_from_schema(context):
    schema_path = context.schema_path
    data_dir = context.data_dir
    context.db = Database.from_schema(schema_path, data_dir)


@when("I load the schema")
def step_load_schema(context):
    context.db = Database.from_schema(context.schema_path, context.data_dir)


@when("I call Database.from_schema with the schema and data root")
def step_call_schema_data_root(context):
    context.db = Database.from_schema(context.schema_path, context.data_dir)


@when("I attempt to load the schema")
def step_attempt_load_schema(context):
    try:
        Database.from_schema(context.schema_path, context.data_dir)
        raise AssertionError("expected schema load to fail")
    except Exception as exc:  # noqa: BLE001
        context.error = exc


@given('a YAML schema file defining a "{table}" table with primary key "{pk}"')
def step_schema_simple(context, table, pk):
    schema = {
        "tables": {
            table: {
                "primary_key": pk,
                "directory": table,
            }
        }
    }
    _write_schema(context, schema)


@given(
    'a YAML schema file defining a "users" table with GSIs "by_email" and "by_status"'
)
def step_schema_gsis(context):
    schema = {
        "tables": {
            "users": {
                "primary_key": "id",
                "directory": "users",
                "gsis": {
                    "by_email": {"partition_key": "email"},
                    "by_status": {"partition_key": "status"},
                },
            }
        }
    }
    _write_schema(context, schema)


@given("a YAML schema file defining:")
def step_schema_multiline(context):
    _write_schema_text(context, context.text)


@given("a YAML schema and data directories with JSON files")
def step_schema_with_data(context):
    schema = {
        "tables": {"users": {"primary_key": "id", "directory": "users"}},
    }
    data_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(data_dir, "users"), exist_ok=True)
    with open(
        os.path.join(data_dir, "users", "user-1.json"), "w", encoding="utf-8"
    ) as handle:
        json.dump({"id": "user-1", "name": "Alice"}, handle)
    context.data_dir = data_dir
    _write_schema(context, schema)


@given('a data directory for "{table}" with records:')
def step_data_directory_records(context, table: str) -> None:
    rows = _table_from_step_table(context.table)
    data_dir = getattr(context, "data_dir", None)
    if data_dir is None:
        data_dir = tempfile.mkdtemp()
        context.data_dir = data_dir
    table_dir = os.path.join(data_dir, table)
    os.makedirs(table_dir, exist_ok=True)
    for idx, row in enumerate(rows):
        filename = row.get("id") or row.get("pk") or f"record-{idx}"
        path = os.path.join(table_dir, f"{filename}.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(row, handle)


@given("an empty database")
def step_empty_db(context):
    context.db = Database()


@given(
    'a YAML schema defining a table with partition_key "item_id" and sort_key "name"'
)
def step_schema_composite(context):
    schema = {
        "tables": {
            "items": {
                "partition_key": "item_id",
                "sort_key": "name",
                "directory": "items",
            }
        }
    }
    _write_schema(context, schema)


@given("a YAML schema file with a missing required field")
def step_schema_invalid(context):
    schema = {"tables": {"users": {"directory": "users"}}}
    _write_schema(context, schema)


@then('the database should have a "{table}" table with primary key "{pk}"')
def step_db_has_table(context, table, pk):
    db = _ensure_db(context)
    assert table in db.tables
    assert db.tables[table].primary_key == pk


@then('the "{table}" table should have GSI "{gsi}" with partition key "{field}"')
def step_table_gsi_exists(context, table, gsi, field):
    table_ref = _ensure_db(context).tables[table]
    assert gsi in table_ref.gsis
    assert table_ref.gsis[gsi].partition_key == field


@then('the database should have a "{assoc_type}" "{assoc}" association')
def step_schema_assoc(context, assoc_type, assoc):
    db = _ensure_db(context)
    found = any(assoc in table.associations for table in db.tables.values())
    assert found


@then("each table should be populated with records from its directory")
def step_tables_populated(context):
    db = _ensure_db(context)
    assert db.tables["users"].count() == 1


@then("the table should use composite primary key")
def step_table_composite_pk(context):
    db = _ensure_db(context)
    table = next(iter(db.tables.values()))
    assert table.partition_key == "item_id"
    assert table.sort_key == "name"


@then("a clear error should be raised indicating what is missing")
def step_schema_error(context):
    try:
        Database.from_schema(context.schema_path, context.data_dir)
        raise AssertionError("expected error")
    except Exception:  # noqa: BLE001
        return


@then('the result should return the user record for "{user_id}"')
def step_result_user_record(context, user_id):
    assert context.result["id"] == user_id


@then('an error should be raised indicating table "{table}" does not exist')
def step_error_missing_table(context, table):
    assert isinstance(context.error, KeyError)
    assert table in str(context.error)


@then('an error should be raised indicating GSI "{gsi}" does not exist')
def step_error_missing_gsi(context, gsi):
    assert isinstance(context.error, KeyError)
    assert gsi in str(context.error)


@then("the result should include {first} and {second}")
def step_result_includes_ids(context, first, second):
    items = context.result.get("items", [])
    ids = {item["id"] for item in items}
    assert first.strip('"') in ids
    assert second.strip('"') in ids


@then('the result should contain only the "id" and "name" fields')
def step_result_projection(context):
    assert set(context.result.keys()) == {"id", "name"}


@then(
    'the result should include the user with a nested "posts" array of {count:d} records'
)
def step_nested_posts(context, count):
    assert len(context.result["posts"]) == count


@then('the result should include the post with a nested "author" object')
def step_nested_author(context):
    assert isinstance(context.result["author"], dict)


@then(
    'the result should include the job with a nested "workers" array of {count:d} records'
)
def step_nested_workers(context, count):
    assert len(context.result["workers"]) == count


@then("the result should include user → posts → comments nested 3 levels deep")
def step_nested_three(context):
    assert "posts" in context.result
    assert "comments" in context.result["posts"][0]


@then('each nested post should only contain "id" and "title" fields')
def step_nested_projection(context):
    for post in context.result["posts"]:
        assert set(post.keys()) == {"id", "title"}


@then('the nested "posts" array should be empty')
def step_nested_empty(context):
    assert context.result["posts"] == []


@then("the result should be in descending created_at order")
def step_desc_order(context):
    items = context.result.get("items", [])
    dates = [item["created_at"] for item in items]
    assert dates == sorted(dates, reverse=True)


@then('the result should contain 2 posts with created_at >= "2025-06-01"')
def step_posts_date_filter(context):
    result = getattr(context, "result", {}) or {}
    items = result.get("items", []) if isinstance(result, dict) else result
    filtered = [item for item in items if item["created_at"] >= "2025-06-01"]
    assert len(filtered) == 2


@then('the result should include a "next_token" value')
def step_has_next_token(context):
    assert "next_token" in context.result


@then('the result should include a "next_token"')
def step_has_next_token_short(context):
    assert "next_token" in context.result


@then('the result should not include a "next_token"')
def step_no_next_token(context):
    assert "next_token" not in context.result


@then("the result should contain exactly {count:d} records")
def step_exact_count(context, count):
    items = (
        context.result
        if isinstance(context.result, list)
        else context.result.get("items", [])
    )
    assert len(items) == count


@then("the result should contain the next 10 records")
def step_next_page(context):
    current_ids = {item["id"] for item in context.result.get("items", [])}
    previous_ids = {item["id"] for item in getattr(context, "saved_page", [])}
    assert current_ids.isdisjoint(previous_ids)


@then("no record should overlap with the first page")
def step_no_overlap(context):
    step_next_page(context)


@then("the total collected records should be {count:d}")
def step_total_collected(context, count):
    assert getattr(context, "collected_count", 0) == count


@then("there should be no duplicates")
def step_no_duplicates(context):
    ids = getattr(context, "collected_ids", [])
    assert len(ids) == len(set(ids))


@then("each page should contain records in descending created_at order")
def step_each_page_desc(context):
    for page in context.pages:
        dates = [item["created_at"] for item in page]
        assert dates == sorted(dates, reverse=True)


@then("the full traversal should return all 20 posts in reverse chronological order")
def step_full_desc(context):
    all_dates = getattr(context, "all_dates", [])
    assert all_dates == sorted(all_dates, reverse=True)


@then("the result should list all 3 table names")
def step_describe_tables(context):
    assert set(context.result.keys()) == {"users", "posts", "comments"}


@then('the "users" entry should include primary_key, GSIs, record_count, and staleness')
def step_describe_users_entry(context):
    users = context.result["users"]
    assert "primary_key" in users
    assert "gsis" in users
    assert "record_count" in users
    assert "stale" in users


@then('the "{table}" table should report storage mode "{mode}"')
def step_table_storage_mode(context, table: str, mode: str) -> None:
    table_desc = context.result.get(table, {})
    assert table_desc.get("storage") == mode


@then(
    'the "{table}" table should list searchable fields "{first}" and "{second}"'
)
def step_table_search_fields(context, table: str, first: str, second: str) -> None:
    table_desc = context.result.get(table, {})
    fields = table_desc.get("search_fields") or []
    assert first in fields
    assert second in fields


@then('the result should include record "{record_id}"')
def step_result_includes_record(context, record_id: str) -> None:
    items = context.result.get("items", [])
    ids = {item.get("id") for item in items}
    assert record_id in ids


@then('the search index should be persisted for "{table}"')
def step_search_index_persisted(context, table: str) -> None:
    data_dir = context.data_dir
    assert data_dir is not None
    index_root = os.path.join(data_dir, ".virtuus", "index", table)
    index_path = os.path.join(index_root, "search_index.json")
    manifest_path = os.path.join(index_root, "search_manifest.json")
    assert os.path.exists(index_path)
    assert os.path.exists(manifest_path)


@then('the "users" entry should list the "posts" association')
def step_describe_association(context):
    assert "posts" in context.result["users"]["associations"]


@then('"users" should have a has_many "posts" association')
def step_schema_users_has_many(context):
    users = _ensure_db(context).tables["users"]
    assert "posts" in users.associations


@then('"posts" should have a belongs_to "author" association')
def step_schema_posts_belongs(context):
    posts = _ensure_db(context).tables["posts"]
    assert "author" in posts.associations


@then("the result should be an empty schema with no tables")
def step_describe_empty(context):
    assert context.result == {}


@then("the result should be an empty list of violations")
def step_validate_empty(context):
    assert context.result == []


@then("the result should be an empty list")
def step_validate_empty_list(context):
    assert context.result == []


@then(
    'the result should include a violation for post "{post_id}" referencing missing user "{user_id}"'
)
def step_validate_violation(context, post_id, user_id):
    assert any(
        v["record_pk"] == post_id and v["missing_target"] == user_id
        for v in context.result
    )


@then("the result should contain 3 violations")
def step_validate_three(context):
    assert len(context.result) == 3


@then(
    "each violation should include table, record_pk, association, foreign_key, and missing_target"
)
def step_validate_fields(context):
    for violation in context.result:
        for field in (
            "table",
            "record_pk",
            "association",
            "foreign_key",
            "missing_target",
        ):
            assert field in violation


@then("the result should return the next page until completion")
def step_page_through(context):
    pass


@when("I page through with limit 10")
def step_page_through_limit(context):
    db = _ensure_db(context)
    collected = []
    token = None
    last_page = []
    while True:
        query = {"users": {"limit": 10}}
        if token is not None:
            query["users"]["next_token"] = token
        result = db.execute(query)
        collected.extend(result.get("items", []))
        token = result.get("next_token")
        last_page = result.get("items", [])
        if token is None:
            break
    context.result = result
    context.last_result = last_page
    context.collected_ids = [item["id"] for item in collected]
    context.collected_count = len(collected)


@when("I reach the last page")
def step_reach_last_page(context):
    step_page_through_limit(context)


@when("I page through all records with limit 10")
def step_page_through_all(context):
    step_page_through_limit(context)


# Switch to regex matcher for literal JSON patterns
use_step_matcher("re")


@given(r'I execute \{"users": \{"limit": 10\}\} and receive a next_token')
def step_given_exec_with_token(context):
    step_execute(context, '{"users": {"limit": 10}} and receive a next_token')


# Restore default parse matcher
use_step_matcher("parse")


@given("I page through with limit 10")
def step_given_page_limit(context):
    step_page_through_limit(context)


@when("I page through with {query_text}")
def step_page_custom(context, query_text):
    db = _ensure_db(context)
    directive = _parse_json(query_text)
    pages = []
    token = None
    all_dates = []
    while True:
        q = directive.copy()
        inner = next(iter(q.values()))
        if token is not None:
            inner["next_token"] = token
        result = db.execute(q)
        items = result.get("items", [])
        pages.append(items)
        all_dates.extend([item["created_at"] for item in items])
        token = result.get("next_token")
        if token is None:
            break
    context.pages = pages
    context.all_dates = all_dates
    context.result = {"items": pages[-1]} if pages else {"items": []}


@given("every post's user_id references an existing user")
def step_posts_valid(context):
    users = _table(context, "users")
    posts = _table(context, "posts")
    posts.add_belongs_to("user", "users", "user_id")
    for user_id in ("user-1", "user-2"):
        users.put({"id": user_id})
        posts.put({"id": f"post-{user_id}", "user_id": user_id})


@given('post "{post_id}" has user_id "{user_id}" which does not exist in users')
def step_post_missing_user(context, post_id, user_id):
    posts = _table(context, "posts")
    posts.add_belongs_to("user", "users", "user_id")
    posts.put({"id": post_id, "user_id": user_id})


@given("3 posts reference non-existent users")
def step_three_missing(context):
    posts = _table(context, "posts")
    posts.add_belongs_to("user", "users", "user_id")
    for i in range(3):
        posts.put({"id": f"post-{i+1}", "user_id": f"missing-{i}"})


@given("a database with a referential integrity violation")
def step_violation(context):
    _table(context, "users")
    posts = _table(context, "posts")
    posts.add_belongs_to("user", "users", "user_id")
    posts.put({"id": "post-1", "user_id": "missing"})


@given("a database with only has_many associations defined")
def step_only_has_many(context):
    users = _table(context, "users")
    posts = _table(context, "posts")
    posts.add_gsi("by_user", "user_id")
    users.add_has_many("posts", "posts", "by_user")
    users.put({"id": "user-1"})
    posts.put({"id": "post-1", "user_id": "user-1"})


def _write_schema(context, schema: dict[str, Any]) -> None:
    fd, path = tempfile.mkstemp(suffix=".yml")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        yaml_text = json.dumps(schema)
        handle.write(yaml.safe_dump(json.loads(yaml_text)))
    context.schema_path = path
    if not hasattr(context, "data_dir") or context.data_dir is None:
        context.data_dir = tempfile.mkdtemp()
    for table_name, conf in schema.get("tables", {}).items():
        directory = conf.get("directory")
        if directory:
            os.makedirs(os.path.join(context.data_dir, directory), exist_ok=True)


def _write_schema_text(context, text: str) -> None:
    fd, path = tempfile.mkstemp(suffix=".yml")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)
    context.schema_path = path
    context.data_dir = tempfile.mkdtemp()
