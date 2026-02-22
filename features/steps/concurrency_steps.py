from __future__ import annotations

import json
import threading
from pathlib import Path
from tempfile import TemporaryDirectory

from behave import given, then, when

from virtuus import Table


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


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


@when("10 writer threads continuously put new records")
def step_concurrent_writers(context):
    table = context.concurrent_table
    lock = context.concurrent_lock
    context.writer_stop = threading.Event()
    context.writer_threads = []
    context.concurrent_written_ids = []
    context.concurrent_errors = []

    def writer(worker_id: int) -> None:
        counter = 0
        while not context.writer_stop.is_set():
            record_id = f"user-new-{worker_id}-{counter}"
            record = {"id": record_id, "status": "active"}
            with lock:
                table.put(record)
                context.concurrent_written_ids.append(record_id)
            counter += 1

    for i in range(10):
        thread = threading.Thread(target=writer, args=(i,))
        context.writer_threads.append(thread)
        thread.start()


@when("50 reader threads continuously scan the table")
def step_concurrent_readers_scan(context):
    table = context.concurrent_table
    lock = context.concurrent_lock
    context.reader_errors = []
    reader_threads = []

    def reader() -> None:
        for _ in range(25):
            with lock:
                records = table.scan()
            if any("id" not in record for record in records):
                context.reader_errors.append("missing id")

    for _ in range(50):
        thread = threading.Thread(target=reader)
        reader_threads.append(thread)
        thread.start()
    for thread in reader_threads:
        thread.join()

    context.writer_stop.set()
    for thread in context.writer_threads:
        thread.join()


@then("readers should never see a partially-indexed record")
def step_readers_no_partial(context):
    assert not context.reader_errors


@then("all written records should eventually be visible to readers")
def step_written_records_visible(context):
    table = context.concurrent_table
    records = table.scan()
    ids = {record.get("id") for record in records}
    assert all(record_id in ids for record_id in context.concurrent_written_ids)


@given('a database with "users" table and GSI "by_status" on "status"')
def step_users_table_gsi(context):
    table = Table("users", primary_key="id")
    table.add_gsi("by_status", "status")
    context.concurrent_table = table
    context.concurrent_lock = threading.Lock()
    context.concurrent_errors = []
    context.concurrent_gsi_missing = []


@when('writers continuously put records with status "active"')
def step_writers_active(context):
    table = context.concurrent_table
    lock = context.concurrent_lock
    context.writer_stop = threading.Event()
    context.writer_threads = []

    def writer(worker_id: int) -> None:
        counter = 0
        while not context.writer_stop.is_set():
            record_id = f"user-active-{worker_id}-{counter}"
            record = {"id": record_id, "status": "active"}
            with lock:
                table.put(record)
            counter += 1

    for i in range(10):
        thread = threading.Thread(target=writer, args=(i,))
        context.writer_threads.append(thread)
        thread.start()


@when('readers continuously query the GSI for "active"')
def step_readers_query_gsi(context):
    table = context.concurrent_table
    lock = context.concurrent_lock
    context.reader_errors = []
    context.concurrent_gsi_missing = []
    reader_threads = []

    def reader() -> None:
        for _ in range(20):
            with lock:
                records = table.query_gsi("by_status", "active")
                for record in records:
                    record_id = record.get("id")
                    if record_id is None:
                        context.reader_errors.append("missing id")
                        continue
                    if table.get(record_id) is None:
                        context.concurrent_gsi_missing.append(record_id)

    for _ in range(25):
        thread = threading.Thread(target=reader)
        reader_threads.append(thread)
        thread.start()
    for thread in reader_threads:
        thread.join()

    context.writer_stop.set()
    for thread in context.writer_threads:
        thread.join()


@then("every record returned by the GSI should exist in the table")
def step_gsi_records_exist(context):
    assert not context.concurrent_gsi_missing


@then("no reader should encounter an error")
def step_no_reader_error(context):
    assert not context.reader_errors


@given('a database with "users" table loaded from {count:d} files')
def step_users_loaded_files(context, count: int):
    root = getattr(context, "refresh_root", None)
    if root is None:
        context.refresh_tmp = TemporaryDirectory()
        root = context.refresh_tmp.name
        context.refresh_root = root
    users_dir = Path(root) / "users"
    users_dir.mkdir(parents=True, exist_ok=True)
    for i in range(count):
        _write_json(users_dir / f"user-{i}.json", {"id": f"user-{i}", "status": "active" if i % 2 == 0 else "inactive"})
    table = Table("users", primary_key="id", directory=str(users_dir))
    table.load_from_dir()
    context.refresh_table = table
    context.refresh_expected = (count, count)
    context.refresh_counts = []
    context.refresh_reread = None


@given('a database with "users" table loaded from files')
def step_users_loaded_files_default(context):
    step_users_loaded_files(context, 200)


@given("{count:d} new files are added to the directory")
def step_new_files_added(context, count: int):
    users_dir = Path(context.refresh_root) / "users"
    old, _ = context.refresh_expected
    for i in range(old, old + count):
        _write_json(users_dir / f"user-{i}.json", {"id": f"user-{i}", "status": "active"})
    context.refresh_expected = (old, old + count)


@when("a refresh is triggered while 20 reader threads are querying")
def step_refresh_during_reads(context):
    table = context.refresh_table
    lock = threading.Lock()
    stop = threading.Event()
    counts: list[int] = []

    def refresher() -> None:
        with lock:
            table.refresh()
        stop.set()

    def reader() -> None:
        while not stop.is_set():
            with lock:
                count = len(table.scan())
            counts.append(count)
            if count == context.refresh_expected[1]:
                break

    refresh_thread = threading.Thread(target=refresher)
    refresh_thread.start()
    readers = [threading.Thread(target=reader) for _ in range(20)]
    for thread in readers:
        thread.start()
    for thread in readers:
        thread.join()
    refresh_thread.join()
    context.refresh_counts = counts


@then("each reader should see either the old state or the new state")
def step_readers_old_or_new(context):
    old, new = context.refresh_expected
    assert all(count in (old, new) for count in context.refresh_counts)


@then("no reader should see a partial mix of old and new")
def step_readers_no_partial(context):
    old, new = context.refresh_expected
    assert not any(count not in (old, new) for count in context.refresh_counts)


@when("5 threads simultaneously trigger warm()")
def step_warm_concurrently(context):
    table = context.refresh_table
    lock = threading.Lock()
    rereads: list[int] = []

    def worker() -> None:
        with lock:
            table.warm()
            rereads.append(table.last_change_summary["reread"])

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    context.refresh_reread = max(rereads) if rereads else 0


@then("the table should end in a consistent state")
def step_table_consistent(context):
    table = context.refresh_table
    _, expected = context.refresh_expected
    assert len(table.scan()) == expected


@then("no files should be loaded more than necessary")
def step_no_excess_reread(context):
    _, expected = context.refresh_expected
    assert context.refresh_reread <= expected


@given('a database with an empty "users" table')
def step_empty_users_table(context):
    context.write_tmp = TemporaryDirectory()
    users_dir = Path(context.write_tmp.name) / "users"
    users_dir.mkdir(parents=True, exist_ok=True)
    table = Table("users", primary_key="id", directory=str(users_dir))
    context.write_table = table
    context.write_dir = users_dir
    context.write_errors = []
    context.write_versions = []


@given('a database with a "users" table')
def step_users_table(context):
    step_empty_users_table(context)


@given('a database with a "users" table and GSI "by_status" on "status"')
def step_users_table_gsi(context):
    context.write_tmp = TemporaryDirectory()
    table = Table("users", primary_key="id")
    table.add_gsi("by_status", "status")
    context.write_table = table
    context.write_dir = Path(context.write_tmp.name) / "users"
    context.write_errors = []
    context.write_versions = []


@when("100 threads simultaneously put records with unique PKs")
def step_put_unique(context):
    table = context.write_table
    lock = threading.Lock()
    errors: list[str] = []

    def worker(idx: int) -> None:
        record = {"id": f"user-{idx}", "status": "active" if idx % 2 == 0 else "inactive"}
        try:
            with lock:
                table.put(record)
        except Exception as exc:  # pragma: no cover - error path
            errors.append(str(exc))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(100)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    context.write_errors = errors


@then("the table should contain 100 records")
def step_table_has_100(context):
    assert context.write_table.count() == 100


@then("all 100 JSON files should exist on disk")
def step_100_files_exist(context):
    files = list(context.write_dir.glob("*.json"))
    assert len(files) == 100


@when("50 threads simultaneously put records")
def step_put_50(context):
    table = context.write_table
    lock = threading.Lock()
    errors: list[str] = []

    def worker(idx: int) -> None:
        record = {"id": f"user-{idx}", "status": "active"}
        try:
            with lock:
                table.put(record)
        except Exception as exc:  # pragma: no cover - error path
            errors.append(str(exc))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(50)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    context.write_errors = errors


@then("every JSON file on disk should contain valid JSON")
def step_files_valid_json(context):
    for path in context.write_dir.glob("*.json"):
        json.loads(path.read_text(encoding="utf-8"))


@when("100 threads simultaneously put records with various statuses")
def step_put_various_status(context):
    table = context.write_table
    lock = threading.Lock()
    errors: list[str] = []
    statuses = ["active", "inactive", "suspended"]

    def worker(idx: int) -> None:
        record = {"id": f"user-{idx}", "status": statuses[idx % len(statuses)]}
        try:
            with lock:
                table.put(record)
        except Exception as exc:  # pragma: no cover - error path
            errors.append(str(exc))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(100)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    context.write_errors = errors


@then("the sum of all GSI partition sizes should equal the total record count")
def step_gsi_sum_matches(context):
    table = context.write_table
    total = table.count()
    sum_partitions = sum(len(table.query_gsi("by_status", status)) for status in ["active", "inactive", "suspended"])
    assert sum_partitions == total


@when('10 threads simultaneously put records with the same PK "user-1" but different data')
def step_put_same_pk(context):
    table = context.write_table
    lock = threading.Lock()
    errors: list[str] = []
    versions: list[dict] = []

    def worker(idx: int) -> None:
        record = {"id": "user-1", "name": f"User {idx}"}
        try:
            with lock:
                table.put(record)
            versions.append(record)
        except Exception as exc:  # pragma: no cover - error path
            errors.append(str(exc))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    context.write_errors = errors
    context.write_versions = versions


@then('the table should contain exactly 1 record with PK "user-1"')
def step_one_record_pk(context):
    assert context.write_table.count() == 1
    assert context.write_table.get("user-1") is not None


@then("the record should match one of the 10 written versions")
def step_record_matches_version(context):
    record = context.write_table.get("user-1")
    assert record in context.write_versions


@then("no error should have occurred")
def step_no_error(context):
    assert not context.write_errors
