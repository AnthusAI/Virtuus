from __future__ import annotations

import json
from typing import Dict

from behave import given, then, when

from virtuus import Table


def _tables(context) -> Dict[str, Table]:
    if not hasattr(context, "tables"):
        context.tables = {}
    return context.tables


def _get_table(context, name: str) -> Table:
    tables = _tables(context)
    if name not in tables:
        raise AssertionError(f"table {name} not found")
    context.current_table = name
    return tables[name]


def _ensure_table(
    context,
    name: str,
    primary_key: str = "id",
    partition_key: str | None = None,
    sort_key: str | None = None,
) -> Table:
    tables = _tables(context)
    if name in tables:
        return tables[name]
    table = Table(
        name,
        primary_key=primary_key,
        partition_key=partition_key,
        sort_key=sort_key,
    )
    tables[name] = table
    context.current_table = name
    return table


def _singular_to_table(singular: str) -> str:
    mapping = {
        "post": "posts",
        "user": "users",
        "job": "jobs",
        "category": "categories",
        "worker": "workers",
    }
    return mapping.get(singular, f"{singular}s")


@given(
    'a table "{name}" with primary key "{primary_key}" and a GSI "{gsi}" on "{field}"'
)
def step_table_with_gsi(context, name, primary_key, gsi, field):
    table = _ensure_table(context, name, primary_key=primary_key)
    table.add_gsi(gsi, field)


@given('a junction table "{name}" with a GSI "{gsi}" on "{field}"')
def step_junction_table_with_gsi(context, name, gsi, field):
    step_table_with_gsi(context, name, "id", gsi, field)


@when(
    'I define a belongs_to association "{assoc}" on "{table}" targeting table "{target}" via foreign key "{fk}"'
)
def step_define_belongs_to(context, assoc, table, target, fk):
    src = _ensure_table(context, table)
    _ensure_table(context, target)
    src.add_belongs_to(assoc, target, fk)


@given(
    'a table "{table}" with a belongs_to association "{assoc}" targeting "{target}" via "{fk}"'
)
def step_given_belongs_to(context, table, assoc, target, fk):
    step_define_belongs_to(context, assoc, table, target, fk)


@when(
    'I define a has_many association "{assoc}" on "{table}" targeting table "{target}" via index "{index}"'
)
def step_define_has_many(context, assoc, table, target, index):
    partition_field = f"{index.removeprefix('by_')}_id"
    target_table = _ensure_table(context, target)
    if index not in target_table.gsis:
        target_table.add_gsi(index, partition_field)
    src = _ensure_table(context, table)
    src.add_has_many(assoc, target, index)


@given(
    'a table "{table}" with a has_many association "{assoc}" via GSI "{index}" on table "{target}"'
)
def step_given_has_many(context, table, assoc, index, target):
    step_define_has_many(context, assoc, table, target, index)


@when(
    'I define a has_many_through association "{assoc}" on "{table}" through "{through}" via index "{index}" targeting "{target}" via foreign key "{fk}"'
)
def step_define_has_many_through(context, assoc, table, through, index, target, fk):
    partition_field = f"{index.removeprefix('by_')}_id"
    through_table = _ensure_table(context, through)
    if index not in through_table.gsis:
        through_table.add_gsi(index, partition_field)
    _ensure_table(context, target)
    src = _ensure_table(context, table)
    src.add_has_many_through(assoc, through, index, target, fk)


@given(
    'a has_many_through association from "{table}" to "{target}" through "{through}"'
)
def step_given_has_many_through(context, table, target, through):
    # Default names from feature files
    step_define_has_many_through(
        context, "workers", table, through, "by_job", target, "worker_id"
    )


@given("user {record_text}")
def step_user_record(context, record_text):
    users = _ensure_table(context, "users")
    users.put(json.loads(record_text))


@given("post {record_text}")
def step_post_record(context, record_text):
    remove_user = False
    if record_text.endswith(" with no user_id field"):
        remove_user = True
        record_text = record_text.removesuffix(" with no user_id field")
    posts = _ensure_table(context, "posts")
    record = json.loads(record_text)
    if remove_user:
        record.pop("user_id", None)
    posts.put(record)


@given("posts:")
def step_posts_table(context):
    posts = _ensure_table(context, "posts")
    headers = list(context.table.headings)
    for row in context.table:
        record = {header: row[header] for header in headers}
        posts.put(record)


@given('no posts with user_id "{user_id}"')
def step_no_posts(context, user_id):
    posts = _ensure_table(context, "posts")
    assert all(record.get("user_id") != user_id for record in posts.scan())


@given('no user with id "{user_id}"')
def step_no_user(context, user_id):
    users = _ensure_table(context, "users")
    assert users.get(user_id) is None


@given("workers:")
def step_workers_table(context):
    workers = _ensure_table(context, "workers")
    headers = list(context.table.headings)
    for row in context.table:
        record = {header: row[header] for header in headers}
        workers.put(record)


@given("job_assignments:")
def step_job_assignments(context):
    assignments = _ensure_table(context, "job_assignments")
    headers = list(context.table.headings)
    for row in context.table:
        record = {header: row[header] for header in headers}
        assignments.put(record)


@given("job {record_text}")
def step_job_record(context, record_text):
    jobs = _ensure_table(context, "jobs")
    jobs.put(json.loads(record_text))


@given('no job_assignments for "{job_id}"')
def step_no_job_assignments(context, job_id):
    assignments = _ensure_table(context, "job_assignments")
    assert all(record.get("job_id") != job_id for record in assignments.scan())


@given("categories:")
def step_categories_table(context):
    categories = _ensure_table(context, "categories")
    headers = list(context.table.headings)
    for row in context.table:
        record = {header: row[header] for header in headers if row[header] != ""}
        categories.put(record)


@given("category {record_text}")
def step_category_record(context, record_text):
    if record_text.endswith(" with no parent_id"):
        record_text = record_text.removesuffix(" with no parent_id")
    categories = _ensure_table(context, "categories")
    categories.put(json.loads(record_text))


@given('a table "{table}" with a self-referential has_many "{assoc}" via GSI "{index}"')
def step_self_referential_has_many(context, table, assoc, index):
    partition_field = f"{index.removeprefix('by_')}_id"
    table_ref = _ensure_table(context, table)
    if index not in table_ref.gsis:
        table_ref.add_gsi(index, partition_field)
    table_ref.add_has_many(assoc, table, index)


@given(
    'a table "{table}" with a self-referential belongs_to "{assoc}" via "{foreign_key}"'
)
def step_self_referential_belongs_to(context, table, assoc, foreign_key):
    table_ref = _ensure_table(context, table)
    table_ref.add_belongs_to(assoc, table, foreign_key)


@when('I resolve the "{association}" association for {singular} "{pk}"')
def step_resolve_association(context, association, singular, pk):
    table_name = _singular_to_table(singular)
    table = _get_table(context, table_name)
    result = table.resolve_association(association, pk, _tables(context))
    context.last_result = result


@then('the result should be the user record for "{user_id}"')
def step_result_user(context, user_id):
    assert context.last_result is not None
    assert context.last_result.get("id") == user_id


@then("the result should contain {count:d} posts")
def step_result_post_count(context, count):
    assert isinstance(context.last_result, list)
    assert len(context.last_result) == count


@then('the result should include "{pk1}" and "{pk2}"')
def step_result_includes_two(context, pk1, pk2):
    ids = {record["id"] for record in context.last_result}
    assert pk1 in ids
    assert pk2 in ids


@then('the result should not include "{pk}"')
def step_result_not_include(context, pk):
    ids = {record["id"] for record in context.last_result}
    assert pk not in ids


@then(
    'the result should contain workers "{worker1}" and "{worker2}" and not contain "{worker3}"'
)
def step_result_workers(context, worker1, worker2, worker3):
    ids = {record["id"] for record in context.last_result}
    assert worker1 in ids
    assert worker2 in ids
    assert worker3 not in ids


@then('the result should contain workers "{worker1}" and "{worker2}"')
def step_result_workers_two(context, worker1, worker2):
    ids = {record["id"] for record in context.last_result}
    assert worker1 in ids
    assert worker2 in ids


@then('the result should be the category "{cat_id}"')
def step_result_category(context, cat_id):
    assert context.last_result is not None
    assert context.last_result.get("id") == cat_id


@then('the result should contain "{cat1}" and "{cat2}"')
def step_result_categories(context, cat1, cat2):
    ids = {record["id"] for record in context.last_result}
    assert cat1 in ids
    assert cat2 in ids


@then('the "{table}" table should have an association named "{assoc}"')
def step_table_has_association(context, table, assoc):
    table_ref = _get_table(context, table)
    assert assoc in table_ref.associations
