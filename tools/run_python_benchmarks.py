#!/usr/bin/env python3
"""
Quick runner to produce Python-backend benchmark data (including large totals) without the full behave suite.

We keep the same logic as the Behave steps but allow overriding iteration counts
so we can generate large-sample results (e.g., 100k records) faster.
"""
from __future__ import annotations

import json
import os
from types import SimpleNamespace
from pathlib import Path

# ensure repo root on path
ROOT = Path(__file__).resolve().parents[1]
import sys  # noqa: E402

sys.path.insert(0, str(ROOT))

from features.steps import benchmark_steps as b


TARGET_TOTALS = [100000]  # extend if you want to regenerate smaller totals
ITERATIONS = 50  # faster than the behave scenarios (which use 1000)
BENCH_ROOT = Path("benchmarks/output_py")


def run_for_total(total: int) -> list[dict]:
    ctx = SimpleNamespace()
    ctx.bench_profile = "social_media"
    ctx.bench_scale = 1
    ctx.bench_backend = "python"
    ctx.bench_total_records_target = total
    ctx.bench_db = None
    os.environ["VIRTUUS_BENCH_DIR"] = str(BENCH_ROOT)
    os.environ["VIRTUUS_BENCH_BACKEND"] = "python"
    b._ensure_fixtures(ctx)
    results: list[dict] = []
    for name in ("single_table_cold_load", "full_database_cold_load"):
        b.step_run_benchmark(ctx, name)
        results.append(ctx.last_benchmark)
    for name in ("pk_lookup", "gsi_partition_lookup", "gsi_sorted_query"):
        b.step_run_benchmark_iterations(ctx, name, ITERATIONS)
        results.append(ctx.last_benchmark)
    b.step_run_incremental_refresh(ctx, "incremental_refresh")
    results.append(ctx.last_benchmark)
    return results


def merge_results(existing: list[dict], new: list[dict]) -> list[dict]:
    def key(entry: dict) -> tuple[str, int | None]:
        meta = entry.get("metadata") or {}
        return (entry.get("name", "benchmark"), meta.get("total_records"))

    merged = {key(entry): entry for entry in existing}
    for entry in new:
        merged[key(entry)] = entry
    return list(merged.values())


def write_outputs(results: list[dict]) -> None:
    BENCH_ROOT.mkdir(parents=True, exist_ok=True)
    out_json = BENCH_ROOT / "benchmarks.json"
    out_json.write_text(json.dumps(results, indent=2), encoding="utf-8")
    # regenerate charts + report using the existing helpers
    ctx = SimpleNamespace()
    ctx.benchmark_output_path = out_json
    ctx.bench_root = BENCH_ROOT
    b.step_run_visualization(ctx)


def main() -> None:
    BENCH_ROOT.mkdir(parents=True, exist_ok=True)
    existing_path = BENCH_ROOT / "benchmarks.json"
    existing = []
    if existing_path.exists():
        existing = json.loads(existing_path.read_text(encoding="utf-8"))
    new_results: list[dict] = []
    for total in TARGET_TOTALS:
        new_results.extend(run_for_total(total))
    merged = merge_results(existing, new_results)
    write_outputs(merged)
    print(f"Wrote {len(merged)} entries to {BENCH_ROOT/'benchmarks.json'}")


if __name__ == "__main__":
    main()
