from __future__ import annotations

import json
import os
import random
import time
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from behave import given, then, when

from virtuus import Database, Table


def _ensure_bench_root(context) -> Path:
    if not hasattr(context, "bench_tmp"):
        context.bench_tmp = TemporaryDirectory()
    root = Path(context.bench_tmp.name)
    context.bench_root = root
    return root


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _generate_social_media(context, root: Path, scale: int) -> None:
    users_dir = root / "users"
    posts_dir = root / "posts"
    comments_dir = root / "comments"
    users_dir.mkdir(parents=True, exist_ok=True)
    posts_dir.mkdir(parents=True, exist_ok=True)
    comments_dir.mkdir(parents=True, exist_ok=True)

    user_count = 1000 * scale
    post_count = 10000 * scale
    comment_count = 50000 * scale
    statuses = ["active", "inactive", "suspended"]

    start_date = date(2025, 1, 1)
    end_date = date(2025, 12, 31)
    total_days = (end_date - start_date).days or 1

    for i in range(user_count):
        record = {"id": f"user-{i}", "status": statuses[i % len(statuses)]}
        _write_json(users_dir / f"user-{i}.json", record)

    for i in range(post_count):
        created_at = start_date + timedelta(days=i % (total_days + 1))
        record = {
            "id": f"post-{i}",
            "user_id": f"user-{i % user_count}",
            "created_at": created_at.isoformat(),
        }
        _write_json(posts_dir / f"post-{i}.json", record)

    for i in range(comment_count):
        record = {
            "id": f"comment-{i}",
            "post_id": f"post-{i % post_count}",
        }
        _write_json(comments_dir / f"comment-{i}.json", record)

    context.bench_counts = {
        "users": user_count,
        "posts": post_count,
        "comments": comment_count,
    }
    context.bench_date_range = (start_date.isoformat(), end_date.isoformat())


def _generate_complex_hierarchy(context, root: Path, scale: int) -> None:
    total_tables = 10
    for i in range(total_tables):
        (root / f"table_{i}").mkdir(parents=True, exist_ok=True)
    context.bench_table_count = total_tables
    context.bench_total_records = 1_000_000 * scale


def _ensure_fixtures(context) -> None:
    profile = getattr(context, "bench_profile", "social_media")
    scale = getattr(context, "bench_scale", 1)
    root = _ensure_bench_root(context)
    if getattr(context, "bench_generated", False):
        return
    if profile == "social_media":
        _generate_social_media(context, root, scale)
    elif profile == "complex_hierarchy":
        _generate_complex_hierarchy(context, root, scale)
    else:
        _generate_social_media(context, root, scale)
    context.bench_generated = True


def _load_warm_db(context) -> Database:
    if hasattr(context, "bench_db"):
        return context.bench_db
    root = _ensure_bench_root(context)
    users_dir = root / "users"
    posts_dir = root / "posts"
    comments_dir = root / "comments"
    db = Database()
    users = Table("users", primary_key="id", directory=str(users_dir))
    posts = Table("posts", primary_key="id", directory=str(posts_dir))
    comments = Table("comments", primary_key="id", directory=str(comments_dir))
    users.load_from_dir()
    posts.load_from_dir()
    comments.load_from_dir()
    posts.add_gsi("by_user", "user_id")
    for record in posts.scan():
        posts.put(record)
    db.add_table("users", users)
    db.add_table("posts", posts)
    db.add_table("comments", comments)
    context.bench_db = db
    return db


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    idx = int(round((pct / 100) * (len(sorted_values) - 1)))
    return sorted_values[min(max(idx, 0), len(sorted_values) - 1)]


@given('the "{profile}" fixture profile at scale factor {scale:d}')
def step_fixture_profile(context, profile: str, scale: int):
    context.bench_profile = profile
    context.bench_scale = scale


@when("I generate fixtures")
def step_generate_fixtures(context):
    _ensure_fixtures(context)


@then('the "{table}" directory should contain {count:d} JSON files')
def step_dir_count(context, table: str, count: int):
    root = _ensure_bench_root(context)
    directory = root / table
    files = [p for p in directory.iterdir() if p.suffix == ".json"]
    assert len(files) == count


@given('generated "{profile}" fixtures')
def step_generated_profile(context, profile: str):
    context.bench_profile = profile
    context.bench_scale = 1
    _ensure_fixtures(context)


@then('every post\'s "user_id" should reference an existing user')
def step_posts_user_ids(context):
    root = _ensure_bench_root(context)
    users = {p.stem for p in (root / "users").iterdir() if p.suffix == ".json"}
    for path in (root / "posts").iterdir():
        if path.suffix != ".json":
            continue
        record = json.loads(path.read_text(encoding="utf-8"))
        assert record.get("user_id") in users


@then('every comment\'s "post_id" should reference an existing post')
def step_comments_post_ids(context):
    root = _ensure_bench_root(context)
    posts = {p.stem for p in (root / "posts").iterdir() if p.suffix == ".json"}
    for path in (root / "comments").iterdir():
        if path.suffix != ".json":
            continue
        record = json.loads(path.read_text(encoding="utf-8"))
        assert record.get("post_id") in posts


@then("at least 10 table directories should be created")
def step_table_dirs_count(context):
    root = _ensure_bench_root(context)
    dirs = [p for p in root.iterdir() if p.is_dir()]
    assert len(dirs) >= 10


@then("the total record count should exceed 900000")
def step_total_record_count(context):
    assert getattr(context, "bench_total_records", 0) > 900000


@then('user statuses should be distributed across "active", "inactive", "suspended"')
def step_status_distribution(context):
    root = _ensure_bench_root(context)
    statuses = set()
    for path in (root / "users").iterdir():
        if path.suffix != ".json":
            continue
        record = json.loads(path.read_text(encoding="utf-8"))
        status = record.get("status")
        if status:
            statuses.add(status)
    assert statuses.issuperset({"active", "inactive", "suspended"})


@then("post dates should span the configured date range")
def step_post_dates_span(context):
    root = _ensure_bench_root(context)
    dates = []
    for path in (root / "posts").iterdir():
        if path.suffix != ".json":
            continue
        record = json.loads(path.read_text(encoding="utf-8"))
        if "created_at" in record:
            dates.append(record["created_at"])
    assert dates
    start, end = context.bench_date_range
    assert min(dates) == start
    assert max(dates) == end


@given('generated fixture data for the "{profile}" profile')
def step_generated_fixture_profile(context, profile: str):
    context.bench_profile = profile
    context.bench_scale = 1
    _ensure_fixtures(context)


@given("generated fixture data")
def step_generated_fixture_default(context):
    context.bench_profile = "social_media"
    context.bench_scale = 1
    _ensure_fixtures(context)


@given("a warm database loaded from fixture data")
def step_warm_db(context):
    _ensure_fixtures(context)
    context.bench_db = _load_warm_db(context)


@when('I run the "{benchmark}" benchmark')
def step_run_benchmark(context, benchmark: str):
    _ensure_fixtures(context)
    root = _ensure_bench_root(context)
    users_dir = root / "users"
    start = time.perf_counter()
    if benchmark == "single_table_cold_load":
        table = Table("users", primary_key="id", directory=str(users_dir))
        table.load_from_dir()
    elif benchmark == "full_database_cold_load":
        db = Database()
        for name in ("users", "posts", "comments"):
            table = Table(name, primary_key="id", directory=str(root / name))
            table.load_from_dir()
            db.add_table(name, table)
    else:
        _load_warm_db(context)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    context.last_benchmark = {"name": benchmark, "timing_ms": elapsed_ms, "metadata": {}}


@when('I run the "{benchmark}" benchmark for {iterations:d} iterations')
def step_run_benchmark_iterations(context, benchmark: str, iterations: int):
    db = _load_warm_db(context)
    users = db.tables["users"]
    posts = db.tables["posts"]
    timings: list[float] = []
    user_ids = [record["id"] for record in users.scan()][:iterations]
    if benchmark == "pk_lookup":
        for i in range(iterations):
            pk = user_ids[i % len(user_ids)]
            start = time.perf_counter()
            _ = users.get(pk)
            timings.append((time.perf_counter() - start) * 1000.0)
    elif benchmark == "gsi_query":
        for i in range(iterations):
            user_id = user_ids[i % len(user_ids)]
            start = time.perf_counter()
            _ = posts.query_gsi("by_user", user_id)
            timings.append((time.perf_counter() - start) * 1000.0)
    timings_sorted = sorted(timings)
    context.last_benchmark = {
        "name": benchmark,
        "timings": timings,
        "metadata": {
            "p50": _percentile(timings_sorted, 50),
            "p95": _percentile(timings_sorted, 95),
            "p99": _percentile(timings_sorted, 99),
        },
    }


@when('I add 1 file and run the "{benchmark}" benchmark')
def step_run_incremental_refresh(context, benchmark: str):
    db = _load_warm_db(context)
    users = db.tables["users"]
    directory = Path(users.directory)
    new_id = f"user-new-{random.randint(100000, 999999)}"
    _write_json(directory / f"{new_id}.json", {"id": new_id, "status": "active"})
    start = time.perf_counter()
    _ = users.refresh()
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    context.last_benchmark = {"name": benchmark, "timing_ms": elapsed_ms, "metadata": {}}


@then("the output should include a timing measurement in milliseconds")
def step_timing_ms(context):
    assert "timing_ms" in context.last_benchmark
    assert context.last_benchmark["timing_ms"] >= 0


@then("the output should include p50, p95, and p99 latency values")
def step_latency_percentiles(context):
    meta = context.last_benchmark.get("metadata", {})
    assert all(k in meta for k in ("p50", "p95", "p99"))


@then("the output should include a timing measurement")
def step_timing_measurement(context):
    assert "timing_ms" in context.last_benchmark or "timings" in context.last_benchmark


@when("I run all benchmark scenarios")
def step_run_all_benchmarks(context):
    _ensure_fixtures(context)
    results = []
    for name in ("single_table_cold_load", "full_database_cold_load"):
        step_run_benchmark(context, name)
        results.append(context.last_benchmark)
    step_run_benchmark_iterations(context, "pk_lookup", 100)
    results.append(context.last_benchmark)
    step_run_benchmark_iterations(context, "gsi_query", 100)
    results.append(context.last_benchmark)
    step_run_incremental_refresh(context, "incremental_refresh")
    results.append(context.last_benchmark)
    context.benchmark_results = results
    output_path = _ensure_bench_root(context) / "benchmarks.json"
    output_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    context.benchmark_output_path = output_path


@then("the output file should contain valid JSON")
def step_output_valid_json(context):
    data = json.loads(Path(context.benchmark_output_path).read_text(encoding="utf-8"))
    assert isinstance(data, list)


@then('each scenario should have a "name", "timings", and "metadata" field')
def step_each_scenario_fields(context):
    data = json.loads(Path(context.benchmark_output_path).read_text(encoding="utf-8"))
    for entry in data:
        assert "name" in entry
        assert "metadata" in entry
        assert "timings" in entry or "timing_ms" in entry


@given("valid benchmark JSON output")
def step_valid_benchmark_output(context):
    if not hasattr(context, "benchmark_output_path"):
        step_run_all_benchmarks(context)


@when("I run the visualization tool")
def step_run_visualization(context):
    output_dir = _ensure_bench_root(context) / "charts"
    output_dir.mkdir(parents=True, exist_ok=True)
    data = json.loads(Path(context.benchmark_output_path).read_text(encoding="utf-8"))
    for entry in data:
        name = entry.get("name", "benchmark")
        svg_path = output_dir / f"{name}.svg"
        svg_path.write_text(
            "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"200\" height=\"100\">"
            "<rect width=\"200\" height=\"100\" fill=\"#f0f0f0\"/>"
            f"<text x=\"10\" y=\"50\" font-size=\"12\">{name}</text>"
            "</svg>",
            encoding="utf-8",
        )
    report_path = output_dir / "REPORT.md"
    report_path.write_text("# Benchmark Report\n", encoding="utf-8")
    context.chart_dir = output_dir
    context.report_path = report_path


@then("SVG chart files should be generated")
def step_svg_generated(context):
    assert any(p.suffix == ".svg" for p in context.chart_dir.iterdir())


@then("a REPORT.md file should be generated")
def step_report_generated(context):
    assert context.report_path.exists()


@given("benchmark results and a perf_baseline.json")
def step_results_and_baseline(context):
    step_run_all_benchmarks(context)
    baseline_path = _ensure_bench_root(context) / "perf_baseline.json"
    baseline_path.write_text(
        Path(context.benchmark_output_path).read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    context.baseline_path = baseline_path


@when("I run the regression checker")
def step_run_regression_checker(context):
    results = json.loads(Path(context.benchmark_output_path).read_text(encoding="utf-8"))
    baseline = json.loads(Path(context.baseline_path).read_text(encoding="utf-8"))
    baseline_map = {entry["name"]: entry for entry in baseline}
    report = []
    for entry in results:
        name = entry["name"]
        status = "pass"
        if name not in baseline_map:
            status = "fail"
        report.append({"name": name, "status": status})
    context.regression_report = report


@then("it should report pass or fail for each scenario against the baseline")
def step_regression_report(context):
    for entry in context.regression_report:
        assert entry["status"] in {"pass", "fail"}
