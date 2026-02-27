#!/usr/bin/env python3
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from features.steps import benchmark_steps as viz  # type: ignore


def _instance_key(meta: dict) -> str:
    inst = meta.get("instance_type") or "local"
    return str(inst).replace(".", "_").replace(" ", "_")


def _engine_label(engine: str, storage_mode: str | None) -> str:
    if engine == "virtuus":
        return f"virtuus_{storage_mode or 'index_only'}"
    return engine


def main() -> None:
    out_dir = Path("benchmarks") / "output_cold_start"
    results_path = out_dir / "results.json"
    if not results_path.exists():
        print(f"No results at {results_path}")
        return
    entries = json.loads(results_path.read_text(encoding="utf-8"))
    charts_dir = out_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    # query -> record_size -> instance -> engine_label -> list[(total, p95)]
    grouped: dict[str, dict[float, dict[str, dict[str, list[tuple[int, float]]]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    )
    for entry in entries:
        name = entry.get("name")
        if not name or not name.startswith("cold_start_"):
            continue
        meta = entry.get("metadata", {}) or {}
        total = meta.get("total_records")
        record_size = meta.get("record_size_kb")
        engine = meta.get("engine")
        p95 = meta.get("p95")
        if total is None or record_size is None or engine is None or p95 is None:
            continue
        instance_key = _instance_key(meta)
        label = _engine_label(str(engine), meta.get("storage_mode"))
        grouped[name][float(record_size)][instance_key][label].append(
            (int(total), float(p95))
        )

    for query_name, record_map in grouped.items():
        for record_size, inst_map in record_map.items():
            for instance_key, series_map in inst_map.items():
                series = {
                    label: sorted(points, key=lambda pair: pair[0])
                    for label, points in series_map.items()
                }
                chart_path = (
                    charts_dir
                    / f"cold_start_{query_name}_record_{record_size:g}kb_instance_{instance_key}.png"
                )
                title = (
                    f"Cold-start {query_name.replace('cold_start_', '').replace('_', ' ')}"
                    f" – record {record_size:g} KB – {instance_key}"
                )
                viz._render_line_chart(
                    title,
                    series,
                    chart_path,
                    x_label="Total Records",
                    x_formatter=lambda v: f"{v:,}",
                    y_label="P95 Latency (ms)",
                    y_formatter=lambda v: f"{v:.2f}",
                )

    print(f"Wrote charts to {charts_dir}")


if __name__ == "__main__":
    main()
