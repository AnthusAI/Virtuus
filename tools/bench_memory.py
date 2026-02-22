#!/usr/bin/env python3
"""
Measure RSS memory for the Virtuus server across corpus sizes, index counts, and association density.

Usage:
  python tools/bench_memory.py

Options:
  --totals  Comma-separated user counts (default: 100,500,1000,5000,10000)
  --gsis    Comma-separated GSI counts per users table (default: 0,1,3)
  --posts/--no-posts  Include a posts table with has_many/belongs_to associations (default: --posts)
  --port    Starting port to try (default: 18080)
  --output  Output directory (default: benchmarks/output_memory)

Outputs:
  - results.json (structured data)
  - results.csv  (summary table)
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import signal
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Iterable
import sys
import socket

ROOT = Path(__file__).resolve().parents[1]
VIRTUUS_BIN = ROOT / "rust" / "target" / "release" / "virtuus"
sys.path.insert(0, str(ROOT))
from features.steps import benchmark_steps as viz  # type: ignore


def ensure_binary() -> Path:
    if VIRTUUS_BIN.exists():
        return VIRTUUS_BIN
    # fallback to debug build
    debug_bin = ROOT / "rust" / "target" / "debug" / "virtuus"
    if debug_bin.exists():
        return debug_bin
    raise SystemExit("virtuus binary not found; build with `cd rust && cargo build --release`")


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def write_yaml_schema(path: Path, include_posts: bool, gsi_count: int) -> None:
    lines = ["tables:", "  users:", "    primary_key: id", "    directory: users"]
    if gsi_count > 0:
        lines.append("    gsis:")
        if gsi_count >= 1:
            lines.append("      by_status:")
            lines.append("        partition_key: status")
        if gsi_count >= 2:
            lines.append("      by_org:")
            lines.append("        partition_key: org_id")
            lines.append("        sort_key: created_at")
        if gsi_count >= 3:
            lines.append("      by_segment:")
            lines.append("        partition_key: segment")
    if include_posts:
        lines.extend(
            [
                "    associations:",
                "      posts:",
                "        type: has_many",
                "        table: posts",
                "        index: by_user",
                "  posts:",
                "    primary_key: id",
                "    directory: posts",
                "    gsis:",
                "      by_user:",
                "        partition_key: user_id",
                "        sort_key: created_at",
                "    associations:",
                "      author:",
                "        type: belongs_to",
                "        table: users",
                "        foreign_key: user_id",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def generate_data(root: Path, total_users: int, include_posts: bool) -> None:
    users_dir = root / "users"
    users_dir.mkdir(parents=True, exist_ok=True)
    posts_dir = root / "posts"
    if include_posts:
        posts_dir.mkdir(parents=True, exist_ok=True)

    statuses = ["active", "inactive", "suspended"]
    for i in range(total_users):
        record = {
            "id": f"user-{i}",
            "status": statuses[i % len(statuses)],
            "org_id": f"org-{i % max(1, total_users // 10)}",
            "created_at": f"2025-01-{(i % 28) + 1:02d}",
            "segment": "vip" if i % 10 == 0 else "std",
        }
        write_json(users_dir / f"user-{i}.json", record)

    if include_posts:
        post_count = max(1, total_users * 2)
        for i in range(post_count):
            record = {
                "id": f"post-{i}",
                "user_id": f"user-{i % total_users}",
                "created_at": f"2025-02-{(i % 28) + 1:02d}",
            }
            write_json(posts_dir / f"post-{i}.json", record)


def start_server(bin_path: Path, data_dir: Path, schema_path: Path, port: int) -> subprocess.Popen:
    env = os.environ.copy()
    env.setdefault("RUST_LOG", "warn")
    return subprocess.Popen(
        [str(bin_path), "serve", "--dir", str(data_dir), "--schema", str(schema_path), "--port", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )


def wait_for_port(host: str, port: int, timeout: float = 10.0) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            try:
                sock.connect((host, port))
                return True
            except OSError:
                time.sleep(0.2)
    return False


def query_memory(bin_path: Path, port: int, retries: int = 20, delay: float = 0.25) -> dict:
    for attempt in range(retries):
        try:
            output = subprocess.check_output(
                [str(bin_path), "memory", "--port", str(port)], text=True, stderr=subprocess.DEVNULL
            )
            return json.loads(output.strip())
        except Exception:
            time.sleep(delay)
    raise RuntimeError("memory endpoint not reachable")


def stop_process(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def parse_list(value: str) -> list[int]:
    out: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            raise argparse.ArgumentTypeError(f"invalid int: {part}")
    if not out:
        raise argparse.ArgumentTypeError("empty list")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure Virtuus server RSS across dataset shapes.")
    parser.add_argument("--totals", default="100,500,1000,5000,10000", type=str)
    parser.add_argument("--gsis", default="0,1,3", type=str)
    parser.add_argument("--posts", dest="posts", action="store_true", default=True)
    parser.add_argument("--no-posts", dest="posts", action="store_false")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--output", type=Path, default=ROOT / "benchmarks" / "output_memory")
    args = parser.parse_args()

    totals = parse_list(args.totals)
    gsi_counts = parse_list(args.gsis)
    bin_path = ensure_binary()
    out_dir = args.output
    out_dir.mkdir(parents=True, exist_ok=True)

    results = []
    port = args.port

    for total in totals:
        for gsi_count in gsi_counts:
            for include_posts in ([True] if args.posts else [False]):
                with tempfile.TemporaryDirectory() as tmpdir:
                    data_root = Path(tmpdir)
                    generate_data(data_root, total, include_posts)
                    schema_path = data_root / "schema.yml"
                    write_yaml_schema(schema_path, include_posts, gsi_count)

                    proc = start_server(bin_path, data_root, schema_path, port)
                    try:
                        if not wait_for_port("127.0.0.1", port, timeout=10.0):
                            raise RuntimeError(f"server did not open port {port}")
                        if proc.poll() is not None:
                            raise RuntimeError(f"server exited early with code {proc.returncode}")
                        mem = query_memory(bin_path, port)
                    finally:
                        stop_process(proc)

                    rss_kb = mem.get("rss_kb")
                    results.append(
                        {
                            "total_users": total,
                            "gsi_count": gsi_count,
                            "include_posts": include_posts,
                            "rss_kb": rss_kb,
                            "rss_bytes": mem.get("rss_bytes"),
                        }
                    )
                    # bump port to reduce reuse collisions
                    port = port + 1 + random.randint(0, 5)

    results_path = out_dir / "results.json"
    results_path.write_text(json.dumps(results, indent=2), encoding="utf-8")

    csv_lines = ["total_users,gsi_count,include_posts,rss_kb"]
    for row in results:
        csv_lines.append(
            f"{row['total_users']},{row['gsi_count']},{int(row['include_posts'])},{row.get('rss_kb','')}"
        )
    (out_dir / "results.csv").write_text("\n".join(csv_lines) + "\n", encoding="utf-8")

    # render chart
    categories = sorted({r["total_users"] for r in results})
    series_labels = [str(g) for g in sorted({r["gsi_count"] for r in results})]
    data: dict[str, dict[str, float]] = {}
    for total in categories:
        label = f"{total:,} users"
        data[label] = {}
        for g in series_labels:
            match = next(
                (
                    r
                    for r in results
                    if r["total_users"] == total and str(r["gsi_count"]) == g and r["include_posts"]
                ),
                None,
            )
            if match:
                data[label][g] = float(match.get("rss_kb") or 0)
    chart_path = out_dir / "memory_rss.png"
    viz._render_horizontal_bar_chart(
        "RSS by corpus size and GSI count (includes posts associations)",
        [f"{c:,} users" for c in categories],
        [f"{g} GSIs" for g in series_labels],
        data,
        chart_path,
    )

    print(f"Wrote {len(results)} samples to {results_path}")


if __name__ == "__main__":
    main()
