#!/usr/bin/env python3
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Callable
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from features.steps import benchmark_steps as viz  # type: ignore


def _x_formatter_size(value: int) -> str:
    return f"{value / 1000:g} KB"


def _y_formatter_mb(value: float) -> str:
    return f"{value:.1f}"


def _load_results(out_root: Path) -> list[tuple[str, str, list[dict]]]:
    results: list[tuple[str, str, list[dict]]] = []
    if not out_root.exists():
        return results
    for backend_dir in out_root.iterdir():
        if not backend_dir.is_dir():
            continue
        for shape_dir in backend_dir.iterdir():
            if not shape_dir.is_dir():
                continue
            path = shape_dir / "results.json"
            if not path.exists():
                continue
            data = json.loads(path.read_text(encoding="utf-8"))
            results.append((backend_dir.name, shape_dir.name, data))
    return results


def _metric_value(entry: dict) -> float | None:
    meta = entry.get("metadata", {}) or {}
    for key in ("p95", "p50", "timing_ms"):
        if key in meta:
            return float(meta[key])
    if "timing_ms" in entry:
        return float(entry["timing_ms"])
    return None


def _instance_key(meta: dict) -> str:
    inst = meta.get("instance_type") or "local"
    return str(inst).replace(".", "_").replace(" ", "_")


def _generate_latency_charts(
    results: list[tuple[str, str, list[dict]]], charts_dir: Path
) -> None:
    operations = {
        "search_single_term",
        "search_multi_term",
        "pk_lookup",
        "gsi_partition_lookup",
        "gsi_sorted_query",
        "scan",
        "incremental_refresh",
    }
    for backend, shape, entries in results:
        # backend -> shape fixed in this loop
        # group by op -> total_records -> instance -> storage_mode -> list[(record_size, value)]
        grouped: dict[
            str, dict[int, dict[str, dict[str, list[tuple[int, float]]]]]
        ] = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(lambda: defaultdict(list))
            )
        )
        for entry in entries:
            name = entry.get("name")
            if name not in operations:
                continue
            meta = entry.get("metadata", {}) or {}
            total = meta.get("total_records")
            record_size = meta.get("record_size_kb")
            storage_mode = meta.get("storage_mode")
            if (
                total is None
                or record_size is None
                or storage_mode is None
                or not isinstance(total, int)
            ):
                continue
            instance_key = _instance_key(meta)
            value = _metric_value(entry)
            if value is None:
                continue
            x_val = int(float(record_size) * 1000)
            grouped[name][total][instance_key][storage_mode].append(
                (x_val, float(value))
            )

        for op_name, totals_map in grouped.items():
            for total, inst_map in totals_map.items():
                for instance_key, series_map in inst_map.items():
                    if not series_map:
                        continue
                    series = {
                        mode: sorted(points, key=lambda pair: pair[0])
                        for mode, points in series_map.items()
                    }
                    chart_path = (
                        charts_dir
                        / f"latency_{backend}_{shape}_{op_name}_total_{total}_instance_{instance_key}.png"
                    )
                    title = (
                        f"{viz._format_benchmark_name(op_name)} – {backend} / {shape} / total {total:,} / {instance_key}"
                    )
                    pretty_series = {
                        ("Index-only" if k == "index_only" else "Memory"): v
                        for k, v in series.items()
                    }
                    order = (
                        ["Index-only", "Memory"]
                        if {"Index-only", "Memory"} & set(pretty_series.keys())
                        else None
                    )
                    viz._render_line_chart(
                        title,
                        pretty_series,
                        chart_path,
                        series_order=order,
                        x_label="Record Size (KB)",
                        x_formatter=_x_formatter_size,
                        y_label="P95 Latency (ms)",
                        y_formatter=lambda v: f"{v:.2f}",
                    )


def _generate_memory_charts(
    results: list[tuple[str, str, list[dict]]], charts_dir: Path
) -> None:
    for backend, shape, entries in results:
        memory_entries = [e for e in entries if e.get("name") == "memory_rss"]
        if not memory_entries:
            continue
        # record_size -> instance -> storage_mode -> list[(total_records, rss_mb)]
        grouped: dict[
            float, dict[str, dict[str, list[tuple[int, float]]]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for entry in memory_entries:
            meta = entry.get("metadata", {}) or {}
            total = meta.get("total_records")
            record_size = meta.get("record_size_kb")
            storage_mode = meta.get("storage_mode")
            rss_kb = meta.get("rss_kb")
            if (
                total is None
                or record_size is None
                or storage_mode is None
                or rss_kb is None
                or not isinstance(total, int)
            ):
                continue
            instance_key = _instance_key(meta)
            grouped[float(record_size)][instance_key][storage_mode].append(
                (total, float(rss_kb) / 1024.0)
            )

        for record_size, inst_map in grouped.items():
            for instance_key, series_map in inst_map.items():
                series = {
                    mode: sorted(points, key=lambda pair: pair[0])
                    for mode, points in series_map.items()
                }
                chart_path = (
                    charts_dir
                    / f"memory_rss_{backend}_{shape}_record_{record_size:g}kb_instance_{instance_key}.png"
                )
                viz._render_line_chart(
                    f"RSS vs dataset size ({backend}, {shape}, record {record_size:g} KB, {instance_key})",
                    series,
                    chart_path,
                    series_order=(
                        ["index_only", "memory"]
                        if {"index_only", "memory"} & set(series.keys())
                        else None
                    ),
                    x_label="Total Records",
                    x_formatter=lambda v: f"{v:,}",
                    y_label="RSS (MB)",
                    y_formatter=_y_formatter_mb,
                )


def main() -> None:
    out_root = Path("benchmarks") / "output_storage"
    charts_dir = out_root / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)
    results = _load_results(out_root)
    if not results:
        print(f"No results found under {out_root}")
        return
    _generate_latency_charts(results, charts_dir)
    _generate_memory_charts(results, charts_dir)
    print(f"Wrote charts to {charts_dir}")


if __name__ == "__main__":
    main()
