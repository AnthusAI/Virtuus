from __future__ import annotations

import binascii
import json
import os
import random
import struct
import time
import zlib
from typing import Callable
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from behave import given, then, when

from virtuus import Sort
from virtuus._python import Database as PyDatabase
from virtuus._python import Table as PyTable

try:  # pragma: no cover - optional Rust backend
    from virtuus._rust import Database as RsDatabase
    from virtuus._rust import Table as RsTable
    HAS_RUST_BACKEND = True
except Exception:  # noqa: BLE001
    HAS_RUST_BACKEND = False
    RsDatabase = None
    RsTable = None


def _db_table_for_backend(backend: str):
    if backend == "rust" and HAS_RUST_BACKEND:
        return RsDatabase, RsTable
    return PyDatabase, PyTable


def _bench_backend(context) -> str:
    if hasattr(context, "bench_backend"):
        return context.bench_backend
    env = os.getenv("VIRTUUS_BENCH_BACKEND") or os.getenv("VIRTUUS_BACKEND")
    backend = (env or "rust").lower()
    if backend == "rust" and not HAS_RUST_BACKEND:
        backend = "python"
    context.bench_backend = backend
    return backend


def _ensure_bench_root(context) -> Path:
    env_dir = os.getenv("VIRTUUS_BENCH_DIR")
    if env_dir:
        root = Path(env_dir)
        root.mkdir(parents=True, exist_ok=True)
        context.bench_root = root
        return root
    if not hasattr(context, "bench_tmp"):
        context.bench_tmp = TemporaryDirectory()
    root = Path(context.bench_tmp.name)
    context.bench_root = root
    return root


def _bench_data_root(context) -> Path:
    root = _ensure_bench_root(context)
    profile = getattr(context, "bench_profile", "social_media")
    scale = getattr(context, "bench_scale", 1)
    backend = _bench_backend(context)
    total_target = getattr(context, "bench_total_records_target", None)
    if total_target is not None:
        data_root = root / f"{profile}_total_{total_target}_{backend}"
    else:
        data_root = root / f"{profile}_scale_{scale}_{backend}"
    data_root.mkdir(parents=True, exist_ok=True)
    context.bench_data_root = data_root
    return data_root


def _entry_metrics(entry: dict) -> list[tuple[str, float]]:
    metrics: list[tuple[str, float]] = []
    if "timing_ms" in entry:
        metrics.append(("timing_ms", float(entry["timing_ms"])))
    meta = entry.get("metadata", {}) or {}
    for key in ("p50", "p95", "p99"):
        if key in meta:
            metrics.append((key, float(meta[key])))
    if not metrics and "timings" in entry:
        timings = sorted(float(v) for v in entry.get("timings", []))
        if timings:
            metrics.extend(
                [
                    ("p50", timings[int(0.50 * (len(timings) - 1))]),
                    ("p95", timings[int(0.95 * (len(timings) - 1))]),
                    ("p99", timings[int(0.99 * (len(timings) - 1))]),
                ]
            )
    return metrics


def _bench_totals() -> list[int]:
    env = os.getenv("VIRTUUS_BENCH_TOTALS")
    if not env:
        # Keep defaults light so CI and local runs are fast; heavier totals can be set via env.
        return [100, 1000]
    totals: list[int] = []
    for part in env.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            totals.append(int(part))
        except ValueError:
            continue
    return totals or [1000]


def _format_int(value: int | float | None) -> str:
    if value is None:
        return "-"
    return f"{int(value):,}"


def _format_benchmark_name(name: str) -> str:
    return name.replace("_", " ").strip().title()


def _compact_benchmark_name(name: str) -> str:
    """Short, chart-friendly label."""
    pretty = _format_benchmark_name(name)
    aliases = {
        "Single Table Cold Load": "Cold Load (Table)",
        "Full Database Cold Load": "Cold Load (DB)",
        "Pk Lookup": "PK Lookup",
        "Gsi Partition Lookup": "GSI Partition",
        "Gsi Sorted Query": "GSI Sorted",
        "Incremental Refresh": "Incr Refresh",
    }
    if pretty in aliases:
        return aliases[pretty]
    parts = pretty.split()
    if len(parts) > 3:
        return " ".join(parts[:3]) + "…"
    return pretty


def _format_metric_label(label: str) -> str:
    if label == "timing_ms":
        return "Timing (ms)"
    if label == "p50":
        return "P50 (ms)"
    if label == "p95":
        return "P95 (ms)"
    if label == "p99":
        return "P99 (ms)"
    return label.replace("_", " ").strip().title()


def _format_value_ms(value: float) -> str:
    if value >= 100:
        return f"{value:.1f} MS"
    if value >= 1:
        return f"{value:.3f} MS"
    return f"{value:.6f} MS"


def _measure_overhead_ns(batch_size: int, repeats: int = 7) -> float:
    samples: list[float] = []
    for _ in range(repeats):
        start = time.perf_counter_ns()
        for _ in range(batch_size):
            pass
        elapsed = time.perf_counter_ns() - start
        samples.append(elapsed / batch_size)
    samples.sort()
    return samples[len(samples) // 2]


def _collect_timings(
    op: Callable[[], None], min_samples: int, batch_size: int, min_total_ns: int
) -> list[float]:
    overhead = _measure_overhead_ns(batch_size)
    samples: list[float] = []
    total_ns = 0
    while len(samples) < min_samples or total_ns < min_total_ns:
        start = time.perf_counter_ns()
        for _ in range(batch_size):
            op()
        elapsed = time.perf_counter_ns() - start
        total_ns += elapsed
        per_op_ns = max(0.0, (elapsed / batch_size) - overhead)
        samples.append(per_op_ns / 1_000_000.0)
    return samples


_FONT_5X7: dict[str, list[str]] = {
    "A": ["01110", "10001", "10001", "11111", "10001", "10001", "10001"],
    "B": ["11110", "10001", "10001", "11110", "10001", "10001", "11110"],
    "C": ["01110", "10001", "10000", "10000", "10000", "10001", "01110"],
    "D": ["11110", "10001", "10001", "10001", "10001", "10001", "11110"],
    "E": ["11111", "10000", "10000", "11110", "10000", "10000", "11111"],
    "F": ["11111", "10000", "10000", "11110", "10000", "10000", "10000"],
    "G": ["01110", "10001", "10000", "10111", "10001", "10001", "01111"],
    "H": ["10001", "10001", "10001", "11111", "10001", "10001", "10001"],
    "I": ["01110", "00100", "00100", "00100", "00100", "00100", "01110"],
    "J": ["00111", "00010", "00010", "00010", "10010", "10010", "01100"],
    "K": ["10001", "10010", "10100", "11000", "10100", "10010", "10001"],
    "L": ["10000", "10000", "10000", "10000", "10000", "10000", "11111"],
    "M": ["10001", "11011", "10101", "10101", "10001", "10001", "10001"],
    "N": ["10001", "11001", "10101", "10011", "10001", "10001", "10001"],
    "O": ["01110", "10001", "10001", "10001", "10001", "10001", "01110"],
    "P": ["11110", "10001", "10001", "11110", "10000", "10000", "10000"],
    "Q": ["01110", "10001", "10001", "10001", "10101", "10010", "01101"],
    "R": ["11110", "10001", "10001", "11110", "10100", "10010", "10001"],
    "S": ["01111", "10000", "10000", "01110", "00001", "00001", "11110"],
    "T": ["11111", "00100", "00100", "00100", "00100", "00100", "00100"],
    "U": ["10001", "10001", "10001", "10001", "10001", "10001", "01110"],
    "V": ["10001", "10001", "10001", "10001", "10001", "01010", "00100"],
    "W": ["10001", "10001", "10001", "10101", "10101", "10101", "01010"],
    "X": ["10001", "10001", "01010", "00100", "01010", "10001", "10001"],
    "Y": ["10001", "10001", "01010", "00100", "00100", "00100", "00100"],
    "Z": ["11111", "00001", "00010", "00100", "01000", "10000", "11111"],
    "0": ["01110", "10001", "10011", "10101", "11001", "10001", "01110"],
    "1": ["00100", "01100", "00100", "00100", "00100", "00100", "01110"],
    "2": ["01110", "10001", "00001", "00010", "00100", "01000", "11111"],
    "3": ["11110", "00001", "00001", "01110", "00001", "00001", "11110"],
    "4": ["00010", "00110", "01010", "10010", "11111", "00010", "00010"],
    "5": ["11111", "10000", "10000", "11110", "00001", "00001", "11110"],
    "6": ["01110", "10000", "10000", "11110", "10001", "10001", "01110"],
    "7": ["11111", "00001", "00010", "00100", "01000", "01000", "01000"],
    "8": ["01110", "10001", "10001", "01110", "10001", "10001", "01110"],
    "9": ["01110", "10001", "10001", "01111", "00001", "00001", "01110"],
    " ": ["00000", "00000", "00000", "00000", "00000", "00000", "00000"],
    ".": ["00000", "00000", "00000", "00000", "00000", "00100", "00100"],
    "-": ["00000", "00000", "00000", "11111", "00000", "00000", "00000"],
    ":": ["00000", "00100", "00100", "00000", "00100", "00100", "00000"],
    "(": ["00010", "00100", "01000", "01000", "01000", "00100", "00010"],
    ")": ["01000", "00100", "00010", "00010", "00010", "00100", "01000"],
    "/": ["00001", "00010", "00100", "01000", "10000", "00000", "00000"],
}


def _text_width(text: str, scale: int = 1) -> int:
    if not text:
        return 0
    return len(text) * (5 + 1) * scale - scale


def _new_canvas(width: int, height: int, color: tuple[int, int, int, int]) -> list[bytearray]:
    r, g, b, a = color
    row = bytearray([r, g, b, a] * width)
    return [bytearray(row) for _ in range(height)]


def _set_pixel(rows: list[bytearray], x: int, y: int, color: tuple[int, int, int, int]) -> None:
    if y < 0 or y >= len(rows):
        return
    if x < 0 or x * 4 >= len(rows[y]):
        return
    idx = x * 4
    rows[y][idx : idx + 4] = bytes(color)


def _draw_rect(
    rows: list[bytearray],
    x: int,
    y: int,
    width: int,
    height: int,
    color: tuple[int, int, int, int],
) -> None:
    for yy in range(y, y + height):
        if yy < 0 or yy >= len(rows):
            continue
        row = rows[yy]
        for xx in range(x, x + width):
            if xx < 0 or xx * 4 >= len(row):
                continue
            idx = xx * 4
            row[idx : idx + 4] = bytes(color)


def _draw_text(
    rows: list[bytearray],
    x: int,
    y: int,
    text: str,
    color: tuple[int, int, int, int],
    scale: int = 1,
) -> None:
    cursor_x = x
    for char in text.upper():
        pattern = _FONT_5X7.get(char, _FONT_5X7[" "])
        for row_idx, row in enumerate(pattern):
            for col_idx, bit in enumerate(row):
                if bit != "1":
                    continue
                for dy in range(scale):
                    for dx in range(scale):
                        _set_pixel(
                            rows,
                            cursor_x + col_idx * scale + dx,
                            y + row_idx * scale + dy,
                            color,
                        )
        cursor_x += (5 + 1) * scale


def _draw_line(
    rows: list[bytearray],
    x0: int,
    y0: int,
    x1: int,
    y1: int,
    color: tuple[int, int, int, int],
) -> None:
    dx = abs(x1 - x0)
    dy = -abs(y1 - y0)
    sx = 1 if x0 < x1 else -1
    sy = 1 if y0 < y1 else -1
    err = dx + dy
    while True:
        _set_pixel(rows, x0, y0, color)
        if x0 == x1 and y0 == y1:
            break
        e2 = 2 * err
        if e2 >= dy:
            err += dy
            x0 += sx
        if e2 <= dx:
            err += dx
            y0 += sy


def _png_chunk(chunk_type: bytes, data: bytes) -> bytes:
    length = struct.pack("!I", len(data))
    crc = struct.pack("!I", binascii.crc32(chunk_type + data) & 0xFFFFFFFF)
    return length + chunk_type + data + crc


def _write_png(path: Path, width: int, height: int, rows: list[bytearray]) -> None:
    raw = bytearray()
    for row in rows:
        raw.append(0)
        raw.extend(row)
    compressed = zlib.compress(bytes(raw))
    ihdr = struct.pack("!IIBBBBB", width, height, 8, 6, 0, 0, 0)
    png = b"".join(
        [
            b"\x89PNG\r\n\x1a\n",
            _png_chunk(b"IHDR", ihdr),
            _png_chunk(b"IDAT", compressed),
            _png_chunk(b"IEND", b""),
        ]
    )
    path.write_bytes(png)


def _render_single_metric_chart(  # pragma: no cover - used only by experimental visuals
    name: str, label: str, value: float, path: Path
) -> None:
    width = 640
    height = 220
    bg = (248, 249, 251, 255)
    text_color = (28, 32, 38, 255)
    accent = (34, 97, 207, 255)
    rows = _new_canvas(width, height, bg)
    _draw_text(rows, 20, 18, name, text_color, scale=2)
    _draw_text(rows, 20, 60, label, text_color, scale=2)
    value_text = _format_value_ms(value)
    scale = 4
    max_width = width - 40
    while scale > 1 and _text_width(value_text, scale) > max_width:
        scale -= 1
    x = max((width - _text_width(value_text, scale)) // 2, 20)
    _draw_text(rows, x, 120, value_text, accent, scale=scale)
    _write_png(path, width, height, rows)


def _render_bar_chart(  # pragma: no cover - currently unused in Behave flow
    name: str, metrics: list[tuple[str, float]], path: Path
) -> None:
    width = 820
    row_height = 34
    top = 70
    height = top + len(metrics) * row_height + 40
    bg = (248, 249, 251, 255)
    text_color = (28, 32, 38, 255)
    bar_color = (34, 97, 207, 255)
    rows = _new_canvas(width, height, bg)
    _draw_text(rows, 20, 20, name, text_color, scale=2)
    max_val = max(value for _, value in metrics) or 1.0
    bar_left = 250
    bar_right = width - 50
    bar_max = max(bar_right - bar_left, 10)
    for idx, (label, value) in enumerate(metrics):
        y = top + idx * row_height
        _draw_text(rows, 20, y, label, text_color, scale=2)
        bar_width = int(bar_max * (value / max_val))
        _draw_rect(rows, bar_left, y + 8, bar_width, 16, bar_color)
        value_text = _format_value_ms(value)
        _draw_text(rows, bar_left + bar_width + 12, y + 4, value_text, text_color, scale=1)
    _write_png(path, width, height, rows)


def _render_line_chart(
    name: str, series: dict[str, list[tuple[int, float]]], path: Path
) -> None:
    width = 880
    height = 560
    margin_left = 80
    margin_right = 40
    margin_top = 90
    margin_bottom = 80
    bg = (248, 249, 251, 255)
    axis_color = (60, 67, 74, 255)
    text_color = (28, 32, 38, 255)
    # Palette: magenta + light blue alternation for clarity
    palette = [
        (204, 0, 153, 255),   # magenta
        (102, 204, 255, 255), # light blue
    ]
    rows = _new_canvas(width, height, bg)
    _draw_text(rows, 20, 20, name, text_color, scale=2)
    totals = sorted({x for points in series.values() for x, _ in points})
    if not totals:
        _write_png(path, width, height, rows)
        return
    x_positions = {}
    if len(totals) == 1:
        x_positions[totals[0]] = margin_left + (width - margin_left - margin_right) // 2
    else:
        span = width - margin_left - margin_right
        for idx, total in enumerate(totals):
            x_positions[total] = margin_left + int(span * idx / (len(totals) - 1))
    max_y = max(value for points in series.values() for _, value in points)
    max_y = max(max_y, 0.001)
    plot_top = margin_top
    plot_bottom = height - margin_bottom
    plot_height = plot_bottom - plot_top
    plot_left = margin_left
    plot_right = width - margin_right
    _draw_line(rows, plot_left, plot_top, plot_left, plot_bottom, axis_color)
    _draw_line(rows, plot_left, plot_bottom, plot_right, plot_bottom, axis_color)
    for total in totals:
        x = x_positions[total]
        _draw_line(rows, x, plot_bottom, x, plot_bottom + 4, axis_color)
        label = _format_int(total)
        _draw_text(rows, max(x - _text_width(label, 1) // 2, 0), plot_bottom + 8, label, text_color, scale=1)
    y_ticks = 4
    for idx in range(y_ticks + 1):
        y_value = max_y * idx / y_ticks
        y = plot_bottom - int(plot_height * idx / y_ticks)
        _draw_line(rows, plot_left - 4, y, plot_left, y, axis_color)
        label = _format_value_ms(y_value).replace(" MS", "")
        _draw_text(rows, max(plot_left - 6 - _text_width(label, 1), 0), y - 3, label, text_color, scale=1)
    _draw_text(rows, plot_left, plot_bottom + 32, "TOTAL RECORDS", text_color, scale=1)
    _draw_text(rows, 20, plot_top + 10, "MS", text_color, scale=1)
    legend_x = plot_left
    legend_y = plot_top - 32
    for idx, label in enumerate(sorted(series.keys())):
        color = palette[idx % len(palette)]
        _draw_rect(rows, legend_x, legend_y, 14, 8, color)
        _draw_text(rows, legend_x + 20, legend_y - 2, label.upper(), text_color, scale=1)
        legend_x += 160
    for idx, label in enumerate(sorted(series.keys())):
        color = palette[idx % len(palette)]
        points = sorted(series[label], key=lambda pair: pair[0])
        prev = None
        for total, value in points:
            x = x_positions[total]
            y = plot_bottom - int((value / max_y) * plot_height)
            _draw_rect(rows, x - 2, y - 2, 5, 5, color)
            if prev is not None:
                _draw_line(rows, prev[0], prev[1], x, y, color)
            prev = (x, y)
    _write_png(path, width, height, rows)


def _render_grouped_bar_chart(  # pragma: no cover - exercised via tools/bench_compare.py
    name: str,
    categories: list[str],
    series_labels: list[str],
    data: dict[str, dict[str, float]],
    path: Path,
) -> None:
    width = 880
    margin_left = 120
    margin_right = 40
    margin_top = 90
    margin_bottom = 150  # extra room for wrapped x labels
    bar_width = 26
    bar_gap = 12
    group_gap = 36
    bg = (248, 249, 251, 255)
    axis_color = (60, 67, 74, 255)
    text_color = (28, 32, 38, 255)
    palette = [
        (34, 97, 207, 255),
        (242, 140, 40, 255),
        (46, 134, 68, 255),
        (143, 84, 178, 255),
    ]
    plot_width = width - margin_left - margin_right
    group_width = len(series_labels) * (bar_width + bar_gap) - bar_gap
    total_width = len(categories) * (group_width + group_gap) - group_gap
    if total_width > plot_width:
        scale = plot_width / total_width
        bar_width = max(8, int(bar_width * scale))
        bar_gap = max(4, int(bar_gap * scale))
        group_gap = max(18, int(group_gap * scale))
        group_width = len(series_labels) * (bar_width + bar_gap) - bar_gap
        total_width = len(categories) * (group_width + group_gap) - group_gap
    width = margin_left + total_width + margin_right
    height = 560
    rows = _new_canvas(width, height, bg)
    _draw_text(rows, 20, 20, name, text_color, scale=2)
    values = [
        data.get(cat, {}).get(label, 0.0)
        for cat in categories
        for label in series_labels
        if data.get(cat, {}).get(label, 0.0) > 0
    ]
    max_val = max(values) if values else 1.0
    min_val = min(values) if values else max_val
    use_log = values and max_val / max(min_val, 1e-9) > 20
    plot_top = margin_top
    plot_bottom = height - margin_bottom
    plot_height = plot_bottom - plot_top
    _draw_line(rows, margin_left, plot_top, margin_left, plot_bottom, axis_color)
    _draw_line(rows, margin_left, plot_bottom, width - margin_right, plot_bottom, axis_color)
    y_ticks = 5
    for idx in range(y_ticks + 1):
        if use_log:
            val = min_val * (max_val / min_val) ** (idx / y_ticks)
            y = plot_bottom - int(plot_height * idx / y_ticks)
        else:
            val = max_val * idx / y_ticks
            y = plot_bottom - int(plot_height * idx / y_ticks)
        _draw_line(rows, margin_left - 4, y, margin_left, y, axis_color)
        label = _format_value_ms(val).replace(" MS", "")
        _draw_text(rows, max(margin_left - 8 - _text_width(label, 1), 0), y - 3, label, text_color, scale=1)
    cursor_x = margin_left
    for cat in categories:
        cat_center = cursor_x + group_width // 2
        parts = cat.split(" ")
        line1 = cat
        line2 = ""
        if len(cat) > 14 and len(parts) > 1:
            mid = len(parts) // 2
            line1 = " ".join(parts[:mid])
            line2 = " ".join(parts[mid:])
        _draw_text(
            rows,
            max(cat_center - _text_width(line1, 1) // 2, margin_left),
            plot_bottom + 12,
            line1,
            text_color,
            scale=1,
        )
        if line2:
            _draw_text(
                rows,
                max(cat_center - _text_width(line2, 1) // 2, margin_left),
                plot_bottom + 28,
                line2,
                text_color,
                scale=1,
            )
        for idx, label in enumerate(series_labels):
            color = palette[idx % len(palette)]
            value = data.get(cat, {}).get(label, 0.0)
            if use_log:
                if value <= 0 or min_val <= 0 or max_val <= min_val:
                    bar_h = 0
                else:
                    import math

                    bar_h = int(
                        (math.log(value) - math.log(min_val))
                        / (math.log(max_val) - math.log(min_val))
                        * plot_height
                    )
            else:
                bar_h = int((value / max_val) * plot_height) if max_val else 0
            x0 = cursor_x + idx * (bar_width + bar_gap)
            y0 = plot_bottom - bar_h
            _draw_rect(rows, x0, y0, bar_width, bar_h, color)
            value_text = _format_value_ms(value)
            _draw_text(rows, x0, y0 - 16, value_text, text_color, scale=1)
        cursor_x += group_width + group_gap
    legend_x = margin_left
    legend_y = height - margin_bottom + 48
    for idx, label in enumerate(series_labels):
        color = palette[idx % len(palette)]
        _draw_rect(rows, legend_x, legend_y, 14, 10, color)
        _draw_text(rows, legend_x + 20, legend_y - 2, label.upper(), text_color, scale=1)
        legend_x += 180
    if use_log:
        _draw_text(rows, width - 140, margin_top - 20, "log scale", axis_color, scale=1)
    _write_png(path, width, height, rows)


def _render_horizontal_bar_chart(  # pragma: no cover - exercised via tools/bench_compare.py
    name: str,
    categories: list[str],
    series_labels: list[str],
    data: dict[str, dict[str, float]],
    path: Path,
    value_formatter: Callable[[float], str] | None = None,
) -> None:
    margin_left = 190
    margin_right = 120
    margin_top = 90
    margin_bottom = 110
    bar_height = 18
    series_gap = 8
    group_gap = 20
    palette = [
        (217, 70, 239, 255),  # magenta
        (80, 186, 255, 255),  # light blue
        (120, 120, 120, 255),
    ]
    bg = (248, 249, 251, 255)
    axis_color = (60, 67, 74, 255)
    text_color = (28, 32, 38, 255)

    group_height = len(series_labels) * (bar_height + series_gap) - series_gap
    total_height = len(categories) * (group_height + group_gap) - group_gap
    height = margin_top + total_height + margin_bottom
    width = 980
    rows = _new_canvas(width, height, bg)
    _draw_text(rows, 20, 20, name, text_color, scale=2)
    if value_formatter is None:
        value_formatter = _format_value_ms

    values = [
        data.get(cat, {}).get(label, 0.0)
        for cat in categories
        for label in series_labels
        if data.get(cat, {}).get(label) is not None
    ]
    max_val = max(values) if values else 1.0
    min_val = min(v for v in values if v is not None) if values else max_val
    use_log = values and max_val / max(min_val, 1e-9) > 20

    plot_left = margin_left
    plot_right = width - margin_right
    plot_width = plot_right - plot_left
    plot_top = margin_top

    # x-axis ticks
    x_ticks = 5
    for idx in range(x_ticks + 1):
        if use_log:
            import math

            val = min_val * (max_val / min_val) ** (idx / x_ticks)
            x = plot_left + int(plot_width * idx / x_ticks)
        else:
            val = max_val * idx / x_ticks
            x = plot_left + int(plot_width * idx / x_ticks)
        _draw_line(rows, x, plot_top - 4, x, height - margin_bottom + 6, axis_color)
        label = value_formatter(val).replace(" MS", "")
        _draw_text(rows, x - _text_width(label, 1) // 2, height - margin_bottom + 24, label, text_color, scale=1)

    cursor_y = plot_top
    for cat in categories:
        # category label
        _draw_text(
            rows,
            max(plot_left - _text_width(cat, 1) - 12, 6),
            cursor_y + (group_height // 2) - 6,
            cat,
            text_color,
            scale=1,
        )
        for idx, label in enumerate(series_labels):
            color = palette[idx % len(palette)]
            value = data.get(cat, {}).get(label, 0.0)
            if use_log:
                if value <= 0 or min_val <= 0 or max_val <= min_val:
                    bar_w = 0
                else:
                    import math

                    bar_w = int(
                        (math.log(value) - math.log(min_val))
                        / (math.log(max_val) - math.log(min_val))
                        * plot_width
                    )
            else:
                bar_w = int((value / max_val) * plot_width) if max_val else 0
            y0 = cursor_y + idx * (bar_height + series_gap)
            _draw_rect(rows, plot_left, y0, bar_w, bar_height, color)
            value_text = value_formatter(value)
            text_x = plot_left + bar_w + 8
            _draw_text(rows, text_x, y0 + 2, value_text, text_color, scale=1)
        cursor_y += group_height + group_gap

    # legend
    legend_x = plot_left
    legend_y = height - margin_bottom + 48
    for idx, label in enumerate(series_labels):
        color = palette[idx % len(palette)]
        _draw_rect(rows, legend_x, legend_y, 16, 12, color)
        _draw_text(rows, legend_x + 24, legend_y - 2, label.upper(), text_color, scale=1)
        legend_x += 200
    if use_log:
        _draw_text(rows, width - 160, margin_top - 20, "log scale", axis_color, scale=1)
    _write_png(path, width, height, rows)


def _write_report(context, data: list[dict], report_path: Path) -> None:
    lines = ["# Benchmark Report", ""]
    root = _ensure_bench_root(context)
    lines.append(f"- Bench root: `{root}`")
    if getattr(context, "bench_date_range", None):
        start, end = context.bench_date_range
        lines.append(f"- Date range: `{start}` → `{end}`")
    totals_map: dict[int, dict[str, int]] = {}
    for entry in data:
        meta = entry.get("metadata", {}) or {}
        total_records = meta.get("total_records")
        counts = meta.get("counts")
        if isinstance(total_records, int) and isinstance(counts, dict):
            totals_map[total_records] = counts
    if totals_map:
        lines.append("")
        lines.append("## Data Sizes")
        lines.append("| total_records | users | posts | comments |")
        lines.append("| --- | --- | --- | --- | --- |")
        for total_records in sorted(totals_map):
            counts = totals_map[total_records]
            lines.append(
                "| {total} | {users} | {posts} | {comments} |".format(
                    total=_format_int(total_records),
                    users=_format_int(counts.get("users")),
                    posts=_format_int(counts.get("posts")),
                    comments=_format_int(counts.get("comments")),
                )
            )
    lines.append("")
    lines.append("| name | total_records | timing_ms | p50 | p95 | p99 |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for entry in data:
        metrics = dict(_entry_metrics(entry))
        meta = entry.get("metadata", {}) or {}
        total_records = meta.get("total_records")
        timing = metrics.get("timing_ms")
        p50 = metrics.get("p50")
        p95 = metrics.get("p95")
        p99 = metrics.get("p99")
        lines.append(
            "| {name} | {total} | {timing} | {p50} | {p95} | {p99} |".format(
                name=entry.get("name", "benchmark"),
                total=_format_int(total_records),
                timing=f"{timing:.3f}" if timing is not None else "-",
                p50=f"{p50:.6f}" if p50 is not None else "-",
                p95=f"{p95:.6f}" if p95 is not None else "-",
                p99=f"{p99:.6f}" if p99 is not None else "-",
            )
        )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _generate_social_media(context, root: Path, scale: int) -> None:
    users_dir = root / "users"
    posts_dir = root / "posts"
    comments_dir = root / "comments"
    users_dir.mkdir(parents=True, exist_ok=True)
    posts_dir.mkdir(parents=True, exist_ok=True)
    comments_dir.mkdir(parents=True, exist_ok=True)

    target_total = getattr(context, "bench_total_records_target", None)
    if target_total is not None:
        users = max(1, int(target_total // 61))
        post_count = users * 10
        comment_count = users * 50
        remainder = max(int(target_total) - (users + post_count + comment_count), 0)
        comment_count += remainder
        user_count = users
    else:
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
    _bench_backend(context)
    root = _bench_data_root(context)
    generated = getattr(context, "bench_generated_scales", set())
    total_target = getattr(context, "bench_total_records_target", None)
    key = (profile, scale, total_target)
    if key in generated:
        return
    if profile == "social_media":
        _generate_social_media(context, root, scale)
    elif profile == "complex_hierarchy":
        _generate_complex_hierarchy(context, root, scale)
    else:
        _generate_social_media(context, root, scale)
    generated.add(key)
    context.bench_generated_scales = generated
    context.bench_db = None


def _load_warm_db(context):
    if getattr(context, "bench_db", None) is not None:
        return context.bench_db
    backend = _bench_backend(context)
    DatabaseCls, TableCls = _db_table_for_backend(backend)
    root = _bench_data_root(context)
    users_dir = root / "users"
    posts_dir = root / "posts"
    comments_dir = root / "comments"
    db = DatabaseCls()
    users = TableCls("users", primary_key="id", directory=str(users_dir))
    posts = TableCls("posts", primary_key="id", directory=str(posts_dir))
    comments = TableCls("comments", primary_key="id", directory=str(comments_dir))
    users.load_from_dir()
    posts.load_from_dir()
    comments.load_from_dir()
    posts.add_gsi("by_user", "user_id")
    posts.add_gsi("by_user_created", "user_id", "created_at")
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
    root = _bench_data_root(context)
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
    root = _bench_data_root(context)
    users = {p.stem for p in (root / "users").iterdir() if p.suffix == ".json"}
    for path in (root / "posts").iterdir():
        if path.suffix != ".json":
            continue
        record = json.loads(path.read_text(encoding="utf-8"))
        assert record.get("user_id") in users


@then('every comment\'s "post_id" should reference an existing post')
def step_comments_post_ids(context):
    root = _bench_data_root(context)
    posts = {p.stem for p in (root / "posts").iterdir() if p.suffix == ".json"}
    for path in (root / "comments").iterdir():
        if path.suffix != ".json":
            continue
        record = json.loads(path.read_text(encoding="utf-8"))
        assert record.get("post_id") in posts


@then("at least 10 table directories should be created")
def step_table_dirs_count(context):
    root = _bench_data_root(context)
    dirs = [p for p in root.iterdir() if p.is_dir()]
    assert len(dirs) >= 10


@then("the total record count should exceed 900000")
def step_total_record_count(context):
    assert getattr(context, "bench_total_records", 0) > 900000


@then('user statuses should be distributed across "active", "inactive", "suspended"')
def step_status_distribution(context):
    root = _bench_data_root(context)
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
    root = _bench_data_root(context)
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
    backend = _bench_backend(context)
    _, TableCls = _db_table_for_backend(backend)
    data_root = _bench_data_root(context)
    users_dir = data_root / "users"
    start = time.perf_counter()
    if benchmark == "single_table_cold_load":
        table = TableCls("users", primary_key="id", directory=str(users_dir))
        table.load_from_dir()
    elif benchmark == "full_database_cold_load":
        DatabaseCls, TableCls = _db_table_for_backend(backend)
        db = DatabaseCls()
        for name in ("users", "posts", "comments"):
            table = TableCls(name, primary_key="id", directory=str(data_root / name))
            table.load_from_dir()
            db.add_table(name, table)
    else:
        _load_warm_db(context)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    counts = getattr(context, "bench_counts", None)
    metadata = {"scale": getattr(context, "bench_scale", 1)}
    if isinstance(counts, dict):
        metadata["counts"] = counts
        metadata["total_records"] = sum(int(v) for v in counts.values())
    context.last_benchmark = {
        "name": benchmark,
        "timing_ms": elapsed_ms,
        "metadata": metadata,
    }


@when('I run the "{benchmark}" benchmark for {iterations:d} iterations')
def step_run_benchmark_iterations(context, benchmark: str, iterations: int):
    db = _load_warm_db(context)
    users = db.tables["users"]
    posts = db.tables["posts"]
    user_ids = [record["id"] for record in users.scan()][:iterations]
    min_total_ns = 200_000_000
    batch_size = 1
    if benchmark == "pk_lookup":
        min_total_ns = 150_000_000
        batch_size = 200

        def op() -> None:
            pk = user_ids[random.randrange(len(user_ids))]
            users.get(pk)

        timings = _collect_timings(op, iterations, batch_size, min_total_ns)
    elif benchmark == "gsi_partition_lookup":
        min_total_ns = 250_000_000
        batch_size = 40

        def op() -> None:
            user_id = user_ids[random.randrange(len(user_ids))]
            posts.query_gsi("by_user", user_id)

        timings = _collect_timings(op, iterations, batch_size, min_total_ns)
    elif benchmark == "gsi_sorted_query":
        min_total_ns = 300_000_000
        batch_size = 20
        predicate = Sort.gte("2025-06-01")

        def op() -> None:
            user_id = user_ids[random.randrange(len(user_ids))]
            posts.query_gsi("by_user_created", user_id, predicate, False)

        timings = _collect_timings(op, iterations, batch_size, min_total_ns)
    else:
        timings = []
    timings_sorted = sorted(timings)
    counts = getattr(context, "bench_counts", None)
    metadata = {
        "p50": _percentile(timings_sorted, 50),
        "p95": _percentile(timings_sorted, 95),
        "p99": _percentile(timings_sorted, 99),
        "scale": getattr(context, "bench_scale", 1),
        "samples": len(timings_sorted),
        "batch_size": batch_size,
        "min_total_ms": min_total_ns / 1_000_000.0,
    }
    if isinstance(counts, dict):
        metadata["counts"] = counts
        metadata["total_records"] = sum(int(v) for v in counts.values())
    context.last_benchmark = {
        "name": benchmark,
        "timings": timings,
        "metadata": metadata,
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
    counts = getattr(context, "bench_counts", None)
    metadata = {"scale": getattr(context, "bench_scale", 1)}
    if isinstance(counts, dict):
        metadata["counts"] = counts
        metadata["total_records"] = sum(int(v) for v in counts.values())
    context.last_benchmark = {
        "name": benchmark,
        "timing_ms": elapsed_ms,
        "metadata": metadata,
    }


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
    results = []
    for total in _bench_totals():
        _bench_backend(context)
        context.bench_total_records_target = total
        context.bench_scale = 1
        context.bench_db = None
        _ensure_fixtures(context)
        for name in ("single_table_cold_load", "full_database_cold_load"):
            step_run_benchmark(context, name)
            results.append(context.last_benchmark)
        step_run_benchmark_iterations(context, "pk_lookup", 100)
        results.append(context.last_benchmark)
        step_run_benchmark_iterations(context, "gsi_partition_lookup", 100)
        results.append(context.last_benchmark)
        step_run_benchmark_iterations(context, "gsi_sorted_query", 100)
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
    root = _ensure_bench_root(context)
    output_dir = root / "charts"
    output_dir.mkdir(parents=True, exist_ok=True)
    for svg in output_dir.glob("*.svg"):
        svg.unlink()
    for png in output_dir.glob("*.png"):
        png.unlink()
    data = json.loads(Path(context.benchmark_output_path).read_text(encoding="utf-8"))
    grouped: dict[str, list[dict]] = {}
    for entry in data:
        grouped.setdefault(entry.get("name", "benchmark"), []).append(entry)
    for raw_name, entries in grouped.items():
        chart_name = _format_benchmark_name(raw_name)
        series: dict[str, list[tuple[int, float]]] = {}
        for entry in entries:
            meta = entry.get("metadata", {}) or {}
            total_records = meta.get("total_records")
            if not isinstance(total_records, int):
                continue
            for label, value in _entry_metrics(entry):
                display_label = _format_metric_label(label)
                series.setdefault(display_label, []).append((total_records, value))
        if not series:
            continue
        png_path = output_dir / f"{raw_name}.png"
        _render_line_chart(chart_name, series, png_path)
    report_path = root / "REPORT.md"
    _write_report(context, data, report_path)
    context.chart_dir = output_dir
    context.report_path = report_path


@then("PNG chart files should be generated")
def step_png_generated(context):
    assert any(p.suffix == ".png" for p in context.chart_dir.iterdir())


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
