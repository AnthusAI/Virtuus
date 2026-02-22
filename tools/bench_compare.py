#!/usr/bin/env python3
"""
Generate side-by-side backend comparison charts (Rust vs Python) from existing benchmark outputs.

Inputs (assumed already generated):
 - benchmarks/output/benchmarks.json     # Rust backend
 - benchmarks/output_py/benchmarks.json  # Python backend

Outputs:
 - benchmarks/output_compare/charts/*.png
"""
from __future__ import annotations

import json
from pathlib import Path

import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from features.steps import benchmark_steps as viz


BACKENDS = {
    "rust": Path("benchmarks/output/benchmarks.json"),
    "python": Path("benchmarks/output_py/benchmarks.json"),
}

OUT_DIR = Path("benchmarks/output_compare/charts")


def _pick_value(entry: dict) -> float | None:
    meta = entry.get("metadata") or {}
    if "p95" in meta:
        return float(meta["p95"])
    if "timing_ms" in entry:
        return float(entry["timing_ms"])
    timings = entry.get("timings")
    if timings:
        vals = sorted(float(v) for v in timings)
        if vals:
            idx = int(0.95 * (len(vals) - 1))
            return vals[idx]
    return None


def load() -> dict[int, dict[str, dict[str, float]]]:
    """
    returns: totals -> benchmark -> backend -> value
    """
    combined: dict[int, dict[str, dict[str, float]]] = {}
    for backend, path in BACKENDS.items():
        if not path.exists():
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        for entry in data:
            name = entry.get("name", "benchmark")
            meta = entry.get("metadata") or {}
            total = meta.get("total_records")
            if not isinstance(total, int):
                continue
            value = _pick_value(entry)
            if value is None:
                continue
            combined.setdefault(total, {}).setdefault(name, {})[backend] = value
    return combined


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    combined = load()
    if not combined:
        raise SystemExit("No benchmark data found; run benchmarks first.")

    # target totals
    totals = [t for t in (1000, 10000, 100000) if t in combined]
    if not totals:
        totals = sorted(combined.keys())

    for total in totals:
        benchmarks = combined[total]
        categories = []
        data = {}
        for name in sorted(benchmarks.keys()):
            categories.append(viz._format_benchmark_name(name))
            data[categories[-1]] = {}
            for backend in ("rust", "python"):
                value = benchmarks[name].get(backend)
                if value is not None:
                    data[categories[-1]][backend] = float(value)
        if not categories:
            continue
        title = f"Rust vs Python (p95 unless timing_ms) — {total:,} records"
        path = OUT_DIR / f"compare_{total}.png"
        viz._render_grouped_bar_chart(title, categories, ["rust", "python"], data, path)


if __name__ == "__main__":
    main()
