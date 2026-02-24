#!/usr/bin/env python3
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from features.steps import benchmark_steps as viz  # type: ignore


def _x_formatter_size(value: int) -> str:
    return f"{value / 1000:g} KB"


def _y_formatter_mb(value: float) -> str:
    return f"{value:.1f}"


def _load_results(out_root: Path) -> list[dict]:
    entries: list[dict] = []
    if not out_root.exists():
        return entries
    for path in out_root.rglob("results.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(data, list):
            entries.extend(data)
    return entries


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


def _generate_latency_charts(entries: list[dict], charts_dir: Path) -> None:
    operations = {
        "search_single_term",
        "search_multi_term",
        "pk_lookup",
        "gsi_partition_lookup",
        "gsi_sorted_query",
        "scan",
        "incremental_refresh",
    }
    grouped: dict[
        str, dict[str, dict[str, dict[int, dict[str, dict[str, list[tuple[int, float]]]]]]]
    ] = defaultdict(
        lambda: defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
        )
    )

    for entry in entries:
        name = entry.get("name")
        if name not in operations:
            continue
        meta = entry.get("metadata", {}) or {}
        backend = meta.get("backend")
        shape = meta.get("profile")
        total = meta.get("total_records")
        record_size = meta.get("record_size_kb")
        storage_mode = meta.get("storage_mode")
        if (
            backend is None
            or shape is None
            or total is None
            or record_size is None
            or storage_mode is None
            or not isinstance(total, int)
        ):
            continue
        value = _metric_value(entry)
        if value is None:
            continue
        instance_key = _instance_key(meta)
        x_val = int(float(record_size) * 1000)
        grouped[str(backend)][str(shape)][name][total][instance_key][storage_mode].append(
            (x_val, float(value))
        )

    for backend, shapes_map in grouped.items():
        for shape, ops_map in shapes_map.items():
            for op_name, totals_map in ops_map.items():
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
                        if {"Index-only", "Memory"} & set(pretty_series.keys()):
                            order = [
                                label
                                for label in ["Index-only", "Memory"]
                                if label in pretty_series
                            ]
                        else:
                            order = None
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


def _generate_instance_latency_charts(entries: list[dict], charts_dir: Path) -> None:
    operations = {
        "search_single_term",
        "search_multi_term",
        "pk_lookup",
        "gsi_partition_lookup",
        "gsi_sorted_query",
        "scan",
        "incremental_refresh",
    }
    grouped: dict[
        str, dict[str, dict[str, dict[int, dict[str, dict[str, list[tuple[int, float]]]]]]]
    ] = defaultdict(
        lambda: defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
        )
    )

    for entry in entries:
        name = entry.get("name")
        if name not in operations:
            continue
        meta = entry.get("metadata", {}) or {}
        backend = meta.get("backend")
        shape = meta.get("profile")
        total = meta.get("total_records")
        record_size = meta.get("record_size_kb")
        storage_mode = meta.get("storage_mode")
        if (
            backend is None
            or shape is None
            or total is None
            or record_size is None
            or storage_mode is None
            or not isinstance(total, int)
        ):
            continue
        value = _metric_value(entry)
        if value is None:
            continue
        instance_key = _instance_key(meta)
        x_val = int(float(record_size) * 1000)
        grouped[str(backend)][str(shape)][name][total][storage_mode][instance_key].append(
            (x_val, float(value))
        )

    for backend, shapes_map in grouped.items():
        for shape, ops_map in shapes_map.items():
            for op_name, totals_map in ops_map.items():
                for total, storage_map in totals_map.items():
                    for storage_mode, inst_map in storage_map.items():
                        series = {
                            inst: sorted(points, key=lambda pair: pair[0])
                            for inst, points in inst_map.items()
                        }
                        if not series:
                            continue
                        chart_path = (
                            charts_dir
                            / f"latency_{backend}_{shape}_{op_name}_total_{total}_storage_{storage_mode}_instances.png"
                        )
                        title = (
                            f"{viz._format_benchmark_name(op_name)} – {backend} / {shape} / total {total:,} / {storage_mode}"
                        )
                        viz._render_line_chart(
                            title,
                            series,
                            chart_path,
                            x_label="Record Size (KB)",
                            x_formatter=_x_formatter_size,
                            y_label="P95 Latency (ms)",
                            y_formatter=lambda v: f"{v:.2f}",
                        )


def _generate_memory_charts(entries: list[dict], charts_dir: Path) -> None:
    memory_entries = [e for e in entries if e.get("name") == "memory_rss"]
    if not memory_entries:
        return
    grouped: dict[
        str, dict[str, dict[float, dict[str, dict[str, list[tuple[int, float]]]]]]
    ] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
    )

    for entry in memory_entries:
        meta = entry.get("metadata", {}) or {}
        backend = meta.get("backend")
        shape = meta.get("profile")
        total = meta.get("total_records")
        record_size = meta.get("record_size_kb")
        storage_mode = meta.get("storage_mode")
        rss_kb = meta.get("rss_kb")
        if (
            backend is None
            or shape is None
            or total is None
            or record_size is None
            or storage_mode is None
            or rss_kb is None
            or not isinstance(total, int)
        ):
            continue
        instance_key = _instance_key(meta)
        grouped[str(backend)][str(shape)][float(record_size)][instance_key][storage_mode].append(
            (total, float(rss_kb) / 1024.0)
        )

    for backend, shapes_map in grouped.items():
        for shape, record_map in shapes_map.items():
            for record_size, inst_map in record_map.items():
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


def _generate_instance_memory_charts(entries: list[dict], charts_dir: Path) -> None:
    memory_entries = [e for e in entries if e.get("name") == "memory_rss"]
    if not memory_entries:
        return
    grouped: dict[
        str, dict[str, dict[float, dict[str, dict[str, list[tuple[int, float]]]]]]
    ] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
    )

    for entry in memory_entries:
        meta = entry.get("metadata", {}) or {}
        backend = meta.get("backend")
        shape = meta.get("profile")
        total = meta.get("total_records")
        record_size = meta.get("record_size_kb")
        storage_mode = meta.get("storage_mode")
        rss_kb = meta.get("rss_kb")
        if (
            backend is None
            or shape is None
            or total is None
            or record_size is None
            or storage_mode is None
            or rss_kb is None
            or not isinstance(total, int)
        ):
            continue
        instance_key = _instance_key(meta)
        grouped[str(backend)][str(shape)][float(record_size)][storage_mode][instance_key].append(
            (total, float(rss_kb) / 1024.0)
        )

    for backend, shapes_map in grouped.items():
        for shape, record_map in shapes_map.items():
            for record_size, storage_map in record_map.items():
                for storage_mode, inst_map in storage_map.items():
                    series = {
                        inst: sorted(points, key=lambda pair: pair[0])
                        for inst, points in inst_map.items()
                    }
                    if not series:
                        continue
                    chart_path = (
                        charts_dir
                        / f"memory_rss_{backend}_{shape}_record_{record_size:g}kb_storage_{storage_mode}_instances.png"
                    )
                    viz._render_line_chart(
                        f"RSS vs dataset size ({backend}, {shape}, record {record_size:g} KB, {storage_mode})",
                        series,
                        chart_path,
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
    _generate_instance_latency_charts(results, charts_dir)
    _generate_instance_memory_charts(results, charts_dir)
    print(f"Wrote charts to {charts_dir}")


if __name__ == "__main__":
    main()
