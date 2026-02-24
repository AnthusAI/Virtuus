#!/usr/bin/env python3
"""
Validate an EC2 params JSON file before launching benchmarks.

Checks:
- required keys: ami, subnet_id, security_group_id
- instance_types non-empty
- totals not empty

Usage:
  python tools/check_ec2_params.py --params tools/ec2_params.example.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REQUIRED_KEYS = ["ami", "subnet_id", "security_group_id"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--params", required=True, help="Path to params JSON")
    args = parser.parse_args()

    path = Path(args.params)
    data = json.loads(path.read_text())
    missing = [k for k in REQUIRED_KEYS if not data.get(k)]
    if missing:
        print(f"Missing required keys: {', '.join(missing)}")
        sys.exit(1)

    def _looks_placeholder(val: str) -> bool:
        return (
            "xxxx" in val.lower()
            or val.lower().endswith("dryrun")
            or val in {"ami-dryrun", "subnet-dryrun", "sg-dryrun"}
        )

    placeholders = [k for k in REQUIRED_KEYS if _looks_placeholder(str(data.get(k, "")))]
    if placeholders:
        print(
            f"Params still look like placeholders for keys: {', '.join(placeholders)}"
        )
        sys.exit(1)

    inst = data.get("instance_types", [])
    if not inst:
        print("instance_types is empty")
        sys.exit(1)

    totals = data.get("totals")
    if not totals:
        print("totals is empty")
        sys.exit(1)

    print("Params file looks valid.")


if __name__ == "__main__":
    main()
