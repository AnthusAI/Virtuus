#!/usr/bin/env python3
"""
Aggregate storage-mode benchmark results (local or downloaded from EC2)
into a single flat CSV/JSON summary with p95s and RSS if available.

Usage:
  python tools/aggregate_storage_results.py \
    --roots benchmarks/output_storage/python benchmarks/output_storage/rust \
    --out benchmarks/output_storage/summary.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


def load_results(root: Path) -> List[dict]:
    entries: List[dict] = []
    for path in root.rglob("results.json"):
        try:
            entries.extend(json.loads(path.read_text()))
        except Exception:
            continue
    return entries


def row_from_entry(entry: dict) -> Dict[str, Any]:
    meta = entry.get("metadata", {}) or {}
    return {
        "backend": meta.get("backend"),
        "profile": meta.get("profile"),
        "storage_mode": meta.get("storage_mode"),
        "record_size_kb": meta.get("record_size_kb"),
        "total_records": meta.get("total_records"),
        "instance_type": meta.get("instance_type"),
        "name": entry.get("name"),
        "status": entry.get("status", "ok"),
        "timing_ms": entry.get("timing_ms"),
        "p50": meta.get("p50"),
        "p95": meta.get("p95"),
        "p99": meta.get("p99"),
        "rss_kb": meta.get("rss_kb"),
    }


def write_outputs(rows: List[dict], out_json: Path | None, out_csv: Path | None) -> None:
    if out_json:
        out_json.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    if out_csv:
        header = [
            "backend",
            "profile",
            "storage_mode",
            "record_size_kb",
            "total_records",
            "instance_type",
            "name",
            "status",
            "timing_ms",
            "p50",
            "p95",
            "p99",
            "rss_kb",
        ]
        lines = [",".join(header)]
        for r in rows:
            lines.append(
                ",".join(str(r.get(col, "")) if r.get(col, "") is not None else "" for col in header)
            )
        out_csv.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--roots", nargs="+", required=True, help="Result roots to scan")
    parser.add_argument("--out-json", help="Output JSON path")
    parser.add_argument("--out-csv", help="Output CSV path")
    args = parser.parse_args()

    rows: List[dict] = []
    for root in args.roots:
        rows.extend(row_from_entry(e) for e in load_results(Path(root)))

    out_json = Path(args.out_json) if args.out_json else None
    out_csv = Path(args.out_csv) if args.out_csv else None
    write_outputs(rows, out_json, out_csv)
    print(f"aggregated {len(rows)} entries")


if __name__ == "__main__":
    main()
