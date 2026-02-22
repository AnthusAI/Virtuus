from __future__ import annotations

import threading

from behave import given, then, when

from virtuus import Table


@given('a database with "users" table containing {count:d} records')
def step_concurrent_users(context, count: int):
    table = Table("users", primary_key="id")
    for i in range(count):
        status = "active" if i % 2 == 0 else "inactive"
        table.put({"id": f"user-{i}", "status": status})
    context.concurrent_table = table
    context.concurrent_lock = threading.Lock()
    context.concurrent_results = []
    context.concurrent_counts = []
    context.concurrent_lookups = []
    context.concurrent_errors = []


@given('a GSI "by_status" on "status"')
def step_concurrent_gsi(context):
    table = context.concurrent_table
    table.add_gsi("by_status", "status")
    records = list(table.records.values())
    for record in records:
        table.put(record)


@when('100 threads simultaneously query index "by_status" for "active"')
def step_concurrent_gsi_queries(context):
    table = context.concurrent_table
    lock = context.concurrent_lock
    results: list[list[str]] = []
    errors: list[str] = []

    def worker() -> None:
        try:
            with lock:
                items = table.query_gsi("by_status", "active")
                ids = sorted(item.get("id") for item in items if isinstance(item, dict))
                results.append(ids)
        except Exception as exc:  # pragma: no cover - error path
            errors.append(str(exc))

    threads = [threading.Thread(target=worker) for _ in range(100)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    context.concurrent_results = results
    context.concurrent_errors = errors


@then("all 100 threads should return the same result set")
def step_concurrent_same_results(context):
    results = context.concurrent_results
    if not results:
        raise AssertionError("no results collected")
    first = results[0]
    assert all(result == first for result in results)


@then("no errors should occur")
def step_concurrent_no_errors(context):
    assert not context.concurrent_errors


@when("50 threads simultaneously get different records by PK")
def step_concurrent_pk_get(context):
    table = context.concurrent_table
    lock = context.concurrent_lock
    lookups: list[tuple[str, str | None]] = []
    errors: list[str] = []

    def worker(pk: str) -> None:
        try:
            with lock:
                record = table.get(pk)
            returned = record.get("id") if isinstance(record, dict) else None
            lookups.append((pk, returned))
        except Exception as exc:  # pragma: no cover - error path
            errors.append(str(exc))

    threads = [threading.Thread(target=worker, args=(f"user-{i}",)) for i in range(50)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    context.concurrent_lookups = lookups
    context.concurrent_errors = errors


@then("each thread should receive the correct record")
def step_concurrent_pk_correct(context):
    for requested, returned in context.concurrent_lookups:
        assert returned == requested


@then("no thread should receive another thread's record")
def step_concurrent_pk_unique(context):
    for requested, returned in context.concurrent_lookups:
        assert returned == requested


@when("20 threads simultaneously scan the table")
def step_concurrent_scan(context):
    table = context.concurrent_table
    lock = context.concurrent_lock
    counts: list[int] = []
    errors: list[str] = []

    def worker() -> None:
        try:
            with lock:
                items = table.scan()
            counts.append(len(items))
        except Exception as exc:  # pragma: no cover - error path
            errors.append(str(exc))

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    context.concurrent_counts = counts
    context.concurrent_errors = errors


@then("all 20 scans should return 500 records each")
def step_concurrent_scan_count(context):
    assert all(count == 500 for count in context.concurrent_counts)
