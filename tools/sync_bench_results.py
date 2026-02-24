#!/usr/bin/env python3
"""
Pull benchmark artifacts (optional S3), aggregate results, and regenerate charts.

Usage examples:
  # Just aggregate local results and refresh charts
  python tools/sync_bench_results.py

  # Pull from S3 then aggregate and refresh charts
  python tools/sync_bench_results.py \
    --bucket my-bucket \
    --prefix virtuus-bench/ \
    --dest benchmarks/output_storage/ec2-sync
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> None:
    subprocess.check_call(cmd, cwd=ROOT)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", help="S3 bucket to pull from")
    parser.add_argument("--prefix", default="", help="S3 prefix")
    parser.add_argument(
        "--dest",
        default="benchmarks/output_storage/ec2-sync",
        help="Destination directory for pulled artifacts",
    )
    parser.add_argument(
        "--cold-dest",
        default="benchmarks/output_cold_start/ec2-sync",
        help="Destination directory for cold-start artifacts",
    )
    parser.add_argument("--profile", help="AWS profile")
    args = parser.parse_args()

    if args.bucket:
        cmd = [
            "python3",
            "tools/pull_storage_results_s3.py",
            "--bucket",
            args.bucket,
            "--prefix",
            args.prefix,
            "--dest",
            args.dest,
        ]
        if args.profile:
            cmd.extend(["--profile", args.profile])
        run(cmd)

        cold_prefix = args.prefix.rstrip("/") + "/cold_start/"
        cmd = [
            "python3",
            "tools/pull_cold_results_s3.py",
            "--bucket",
            args.bucket,
            "--prefix",
            cold_prefix,
            "--dest",
            args.cold_dest,
        ]
        if args.profile:
            cmd.extend(["--profile", args.profile])
        run(cmd)

    run(
        [
            "python3",
            "tools/aggregate_storage_results.py",
            "--roots",
            "benchmarks/output_storage/python",
            "benchmarks/output_storage/rust",
            "benchmarks/output_storage/ec2-sync",
            "--out-json",
            "benchmarks/output_storage/summary.json",
            "--out-csv",
            "benchmarks/output_storage/summary.csv",
        ]
    )

    run(["python3", "tools/bench_storage_mode_charts.py"])
    run(["python3", "tools/bench_cold_start_charts.py"])
    print("sync complete: aggregation + charts refreshed")


if __name__ == "__main__":
    main()
