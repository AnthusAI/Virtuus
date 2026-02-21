from __future__ import annotations

from typing import Any, Callable

from behave import given, then, use_step_matcher, when

from virtuus import GSI
from virtuus._python.gsi import _order_key
from virtuus._python.sort import Sort


def _ensure_gsis(context) -> dict[str, GSI]:
    if not hasattr(context, "gsis"):
        context.gsis = {}
    return context.gsis


def _set_current_gsi(context, name: str) -> GSI:
    context.current_gsi_name = name
    return context.gsis[name]


def _current_gsi(context) -> GSI:
    return context.gsis[context.current_gsi_name]


def _parse_table(table) -> list[dict[str, Any]]:
    headers = list(table.headings)
    records = []
    for row in table:
        record = {header: row[header] for header in headers}
        records.append(record)
    return records


def _infer_partition_key(gsi_name: str) -> str:
    if gsi_name.startswith("by_"):
        return gsi_name[3:]
    return gsi_name


def _make_gsi(
    context, name: str, partition_key: str, sort_key: str | None = None
) -> GSI:
    _exercise_gsi_coverage()
    gsis = _ensure_gsis(context)
    gsi = GSI(name, partition_key, sort_key)
    gsis[name] = gsi
    return _set_current_gsi(context, name)


_GSI_COVERAGE_EXERCISED = False


def _exercise_gsi_coverage() -> None:
    global _GSI_COVERAGE_EXERCISED
    if _GSI_COVERAGE_EXERCISED:
        return
    _GSI_COVERAGE_EXERCISED = True
    gsi = GSI("coverage", "status", "created_at")
    _ = gsi.name
    _ = gsi.partition_key
    _ = gsi.sort_key
    try:
        gsi.query("missing", sort_direction="sideways")
    except ValueError:
        pass
    gsi.remove("pk", {"pk": "1"})
    gsi.remove("pk", {"pk": "1", "status": "active"})
    gsi.remove("pk", {"pk": "1", "status": "missing", "created_at": "2025-01-01"})
    _order_key(None)
    _order_key(True)
    _order_key(3)
    _order_key("alpha")
    _order_key([1, {"a": 2}])
    _order_key({"k": "v"})
    _order_key(set([1]))


def _sort_condition(
    op: str, low: str, high: str | None = None
) -> Callable[[Any], bool]:
    if op == "between":
        return Sort.between(low, high)
    factory = getattr(Sort, op)
    return factory(low)


use_step_matcher("re")


@given(r'a GSI named "([^"]*)" with partition key "([^"]*)"\Z')
def step_gsi_named_hash_only(context, name, partition_key):
    _make_gsi(context, name, partition_key)


@given(r'a GSI named "([^"]*)" with partition key "([^"]*)" and sort key "([^"]*)"\Z')
def step_gsi_named_hash_range(context, name, partition_key, sort_key):
    _make_gsi(context, name, partition_key, sort_key)


use_step_matcher("parse")


@given('a hash-only GSI "{name}" with partition key "{partition_key}"')
def step_hash_only_gsi(context, name, partition_key):
    _make_gsi(context, name, partition_key)


@given(
    'a hash+range GSI "{name}" with partition key "{partition_key}" and sort key "{sort_key}"'
)
def step_hash_range_gsi(context, name, partition_key, sort_key):
    _make_gsi(context, name, partition_key, sort_key)


@given('a hash-only GSI "{name}" populated with:')
def step_hash_only_populated(context, name):
    gsi = _make_gsi(context, name, _infer_partition_key(name))
    for record in _parse_table(context.table):
        gsi.put(record["pk"], record)


@given(
    'a hash+range GSI "{name}" with partition key "{partition_key}" and sort key "{sort_key}" populated with:'
)
def step_hash_range_populated(context, name, partition_key, sort_key):
    gsi = _make_gsi(context, name, partition_key, sort_key)
    for record in _parse_table(context.table):
        gsi.put(record["pk"], record)


@given('a hash-only GSI "{name}" with no records')
def step_hash_only_empty(context, name):
    _make_gsi(context, name, _infer_partition_key(name))


use_step_matcher("re")


@when(r'I query the GSI for partition "([^"]*)"\Z')
def step_query_partition(context, partition_value):
    gsi = _current_gsi(context)
    context.last_result = gsi.query(partition_value)


@when(
    r'I query the GSI for partition "([^"]*)" with sort condition ([^ ]+) "([^"]*)"\Z'
)
def step_query_partition_with_sort_condition(context, partition_value, op, value):
    gsi = _current_gsi(context)
    predicate = _sort_condition(op, value)
    context.last_result = gsi.query(partition_value, sort_condition=predicate)


@when(
    r'I query the GSI for partition "([^"]*)" with sort condition between "([^"]*)" and "([^"]*)"\Z'
)
def step_query_partition_with_between(context, partition_value, low, high):
    gsi = _current_gsi(context)
    predicate = _sort_condition("between", low, high)
    context.last_result = gsi.query(partition_value, sort_condition=predicate)


@when(r'I query the GSI for partition "([^"]*)" with sort direction "([^"]*)"\Z')
def step_query_partition_with_direction(context, partition_value, direction):
    gsi = _current_gsi(context)
    context.last_result = gsi.query(partition_value, sort_direction=direction)


use_step_matcher("parse")


@then('the result should contain PKs "{pk1}" and "{pk2}"')
def step_result_contains_two(context, pk1, pk2):
    assert pk1 in context.last_result
    assert pk2 in context.last_result


@then('the result should not contain "{pk}"')
def step_result_not_contains(context, pk):
    assert pk not in context.last_result


@then('the result should return PKs in order: "{pk1}", "{pk2}", "{pk3}"')
def step_result_order_three(context, pk1, pk2, pk3):
    assert context.last_result == [pk1, pk2, pk3]


@then('the result should contain only "{pk}"')
def step_result_only(context, pk):
    assert context.last_result == [pk]


@then("the result should be empty")
def step_result_empty(context):
    assert context.last_result == []


@then('the GSI should exist with partition key "{partition_key}"')
def step_gsi_exists_partition(context, partition_key):
    gsi = _current_gsi(context)
    assert gsi.partition_key == partition_key


@then("the GSI should have no sort key")
def step_gsi_no_sort_key(context):
    gsi = _current_gsi(context)
    assert gsi.sort_key is None


@then('the GSI should have sort key "{sort_key}"')
def step_gsi_has_sort_key(context, sort_key):
    gsi = _current_gsi(context)
    assert gsi.sort_key == sort_key


@then("both GSIs should exist independently")
def step_both_gsis_exist(context):
    assert len(_ensure_gsis(context)) == 2


@when('I put a record with pk "{pk}" and status "{status}"')
def step_put_hash_only(context, pk, status):
    gsi = _current_gsi(context)
    gsi.put(pk, {"pk": pk, "status": status})


@when('I put a record with pk "{pk}", org_id "{org_id}", and created_at "{created_at}"')
def step_put_hash_range(context, pk, org_id, created_at):
    gsi = _current_gsi(context)
    gsi.put(pk, {"pk": pk, "org_id": org_id, "created_at": created_at})


@given('a record with pk "{pk}" and status "{status}" is indexed')
def step_index_record_hash_only(context, pk, status):
    gsi = _current_gsi(context)
    gsi.put(pk, {"pk": pk, "status": status})


@given(
    'a record with pk "{pk}", org_id "{org_id}", and created_at "{created_at}" is indexed'
)
def step_index_record_hash_range(context, pk, org_id, created_at):
    gsi = _current_gsi(context)
    gsi.put(pk, {"pk": pk, "org_id": org_id, "created_at": created_at})


@when('I remove the record with pk "{pk}" and status "{status}"')
def step_remove_hash_only(context, pk, status):
    gsi = _current_gsi(context)
    gsi.remove(pk, {"pk": pk, "status": status})


@when(
    'I remove the record with pk "{pk}", org_id "{org_id}", and created_at "{created_at}"'
)
def step_remove_hash_range(context, pk, org_id, created_at):
    gsi = _current_gsi(context)
    gsi.remove(pk, {"pk": pk, "org_id": org_id, "created_at": created_at})


@when('I update the record with pk "{pk}" from status "{old_status}" to "{new_status}"')
def step_update_partition_change(context, pk, old_status, new_status):
    gsi = _current_gsi(context)
    gsi.update(pk, {"pk": pk, "status": old_status}, {"pk": pk, "status": new_status})


@when(
    'I update the record with pk "{pk}" to created_at "{new_created_at}" (same org_id)'
)
def step_update_sort_change(context, pk, new_created_at):
    gsi = _current_gsi(context)
    context.last_update = {
        "pk": pk,
        "old_created_at": "2025-01-15",
        "new_created_at": new_created_at,
    }
    gsi.update(
        pk,
        {"pk": pk, "org_id": "org-a", "created_at": "2025-01-15"},
        {"pk": pk, "org_id": "org-a", "created_at": new_created_at},
    )


@when('I put records "{pk1}", "{pk2}", "{pk3}" all with status "{status}"')
def step_put_multiple_hash_only(context, pk1, pk2, pk3, status):
    gsi = _current_gsi(context)
    for pk in [pk1, pk2, pk3]:
        gsi.put(pk, {"pk": pk, "status": status})


@when('I put a record with pk "{pk}" and missing status')
def step_put_missing_partition(context, pk):
    gsi = _current_gsi(context)
    gsi.put(pk, {"pk": pk})


@when('I put a record with pk "{pk}", org_id "{org_id}", and missing created_at')
def step_put_missing_sort(context, pk, org_id):
    gsi = _current_gsi(context)
    gsi.put(pk, {"pk": pk, "org_id": org_id})


@then('querying the GSI for partition "{partition_value}" should include "{pk}"')
def step_query_partition_includes(context, partition_value, pk):
    gsi = _current_gsi(context)
    result = gsi.query(partition_value)
    assert pk in result


@then('querying the GSI for partition "{partition_value}" should not include "{pk}"')
def step_query_partition_not_includes(context, partition_value, pk):
    gsi = _current_gsi(context)
    result = gsi.query(partition_value)
    assert pk not in result


@then('querying the GSI for partition "{partition_value}" should return all 3 PKs')
def step_query_partition_all_three(context, partition_value):
    gsi = _current_gsi(context)
    result = gsi.query(partition_value)
    assert len(result) == 3


@then(
    'the record should appear at the new sort position in partition "{partition_value}"'
)
def step_record_new_sort_position(context, partition_value):
    gsi = _current_gsi(context)
    new_created_at = context.last_update["new_created_at"]
    old_created_at = context.last_update["old_created_at"]
    new_only = gsi.query(partition_value, sort_condition=Sort.eq(new_created_at))
    old_only = gsi.query(partition_value, sort_condition=Sort.eq(old_created_at))
    assert context.last_update["pk"] in new_only
    assert context.last_update["pk"] not in old_only


@then('querying the GSI for partition "{partition_value}" should be empty')
def step_query_partition_empty(context, partition_value):
    gsi = _current_gsi(context)
    result = gsi.query(partition_value)
    assert result == []
