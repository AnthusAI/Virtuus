#!/usr/bin/env python3
"""
Convenience wrapper to launch EC2 storage-mode benchmarks from a JSON params file.

Usage:
  python tools/run_ec2_from_params.py --params tools/ec2_params.example.json --no-dry-run
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
from pathlib import Path


def build_args(params: dict, no_dry_run: bool) -> list[str]:
    args: list[str] = []
    mapping = {
        "ami": "--ami",
        "subnet_id": "--subnet-id",
        "security_group_id": "--security-group-id",
        "key_name": "--key-name",
        "profile": "--profile",
        "s3_bucket": "--s3-bucket",
        "s3_prefix": "--s3-prefix",
        "totals": "--totals",
    }
    for key, flag in mapping.items():
        if params.get(key):
            args.extend([flag, str(params[key])])
    if params.get("instance_types"):
        args.extend(["--instance-types", ",".join(params["instance_types"])])
    if params.get("repo_url"):
        args.extend(["--repo-url", params["repo_url"]])
    if params.get("branch"):
        args.extend(["--branch", params["branch"]])
    if no_dry_run:
        args.append("--no-dry-run")
    return args


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--params", required=True, help="Path to JSON file (see tools/ec2_params.example.json)"
    )
    parser.add_argument(
        "--no-dry-run",
        action="store_true",
        help="Pass through to actually launch instances",
    )
    args = parser.parse_args()

    path = Path(args.params)
    params = json.loads(path.read_text())
    cmd = ["python3", "tools/run_ec2_storage_benchmarks.py"] + build_args(
        params, args.no_dry_run
    )
    print("Running:", " ".join(shlex.quote(c) for c in cmd))
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as exc:  # noqa: BLE001
        print("EC2 runner failed. Did you install boto3 and configure AWS credentials?")
        raise exc


if __name__ == "__main__":
    main()
