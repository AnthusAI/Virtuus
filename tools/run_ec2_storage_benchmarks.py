#!/usr/bin/env python3
"""
Provision ephemeral EC2 instances and run storage-mode benchmarks up to 1M records.

Goals:
- Compare instance classes (e.g., nano vs. mid vs. large) under identical benchmark configs.
- Keep instances short-lived and idempotent; results can be pushed to S3 for later charting.

Safeguards:
- Dry-run by default (no instances launched).
- Requires explicit --subnet-id and --security-group-id; will refuse to run without them.
- Supports --profile to pick an AWS credential profile.
"""

from __future__ import annotations

import argparse
import json
import textwrap
from pathlib import Path
from typing import List

try:
    import boto3  # type: ignore
    from botocore.config import Config as BotoConfig  # type: ignore
except ImportError:  # pragma: no cover - optional dep
    boto3 = None
    BotoConfig = None

ROOT = Path(__file__).resolve().parents[1]


def _parse_list(value: str | None) -> List[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _default_user_data(
    repo_url: str,
    branch: str,
    totals: str,
    s3_bucket: str | None,
    s3_prefix: str | None,
) -> str:
    """Cloud-init user data to install deps and run benchmarks."""
    s3_lines = ""
    if s3_bucket:
        prefix = s3_prefix or "virtuus-bench"
        s3_lines = textwrap.dedent(
            f"""
            if command -v aws >/dev/null 2>&1; then
              aws s3 cp benchmarks/output_storage s3://{s3_bucket}/{prefix}/ --recursive || true
            fi
            """
        )
    return textwrap.dedent(
        f"""\
        #!/bin/bash
        set -eux
        apt-get update
        apt-get install -y git python3 python3-pip tmux awscli
        git clone --branch {branch} {repo_url} /opt/virtuus
        cd /opt/virtuus
        INSTANCE_TYPE=$(curl -s http://169.254.169.254/latest/meta-data/instance-type || echo unknown)
        python3 -m pip install -r requirements.txt || true
        export PYTHONPATH=python/src
        export VIRTUUS_BENCH_INSTANCE_TYPE=$INSTANCE_TYPE
        export VIRTUUS_BENCH_TOTALS={totals}
        export VIRTUUS_BENCH_RECORD_SIZES_KB=0.5,2.0,10
        export VIRTUUS_BENCH_STORAGE_MODES=index_only,memory
        export VIRTUUS_BENCH_DATASET_SHAPES=single_table,social_media
        export VIRTUUS_BENCH_BACKENDS=rust,python
        export VIRTUUS_BENCH_TIMEOUT=900
        export VIRTUUS_BENCH_SKIP_RSS=0
        tmux new -d -s bench "python3 tools/run_storage_mode_benchmarks.py && python3 tools/bench_storage_mode_charts.py"
        {s3_lines}
        """
    )


def ensure_boto(profile: str | None):
    if boto3 is None:
        raise SystemExit("boto3 is required: pip install boto3")
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    return session


def launch_instances(
    session,
    instance_types: list[str],
    ami: str,
    subnet_id: str,
    security_group_id: str,
    key_name: str | None,
    user_data: str,
    tags: dict[str, str],
    dry_run: bool,
):
    ec2 = session.client("ec2", config=BotoConfig(retries={"max_attempts": 5}))
    responses = []
    for itype in instance_types:
        resp = ec2.run_instances(
            ImageId=ami,
            InstanceType=itype,
            MinCount=1,
            MaxCount=1,
            SubnetId=subnet_id,
            SecurityGroupIds=[security_group_id],
            KeyName=key_name,
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": k, "Value": v} for k, v in tags.items()],
                }
            ],
            UserData=user_data,
            DryRun=dry_run,
        )
        responses.append(resp)
    return responses


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", help="AWS profile")
    parser.add_argument("--ami", required=True, help="AMI ID with Ubuntu-like userland")
    parser.add_argument("--subnet-id", required=True)
    parser.add_argument("--security-group-id", required=True)
    parser.add_argument("--key-name", help="SSH key name (optional)")
    parser.add_argument(
        "--instance-types",
        default="t3.nano,t3.small,m6i.large",
        help="Comma list of instance types",
    )
    parser.add_argument(
        "--totals",
        default="1000,10000,100000,1000000",
        help="Comma list of totals to run in remote env",
    )
    parser.add_argument(
        "--repo-url",
        default="https://github.com/virtuus/virtuus.git",
        help="Git repo to clone on the instance",
    )
    parser.add_argument(
        "--branch", default="dev", help="Branch to checkout on the instance"
    )
    parser.add_argument(
        "--tag",
        action="append",
        default=[],
        help='Extra tag key=value (repeat). Always sets Project=VirTBenchmark',
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Dry-run EC2 launch (default: True)",
    )
    parser.add_argument("--s3-bucket", help="Optional S3 bucket to upload results")
    parser.add_argument("--s3-prefix", help="Optional S3 key prefix for uploads")
    args = parser.parse_args()

    tags = {"Project": "VirTBenchmark"}
    for entry in args.tag:
        if "=" in entry:
            k, v = entry.split("=", 1)
            tags[k] = v

    instance_types = _parse_list(args.instance_types)
    if not instance_types:
        raise SystemExit("No instance types provided")

    user_data = _default_user_data(
        args.repo_url, args.branch, args.totals, args.s3_bucket, args.s3_prefix
    )

    session = ensure_boto(args.profile)
    responses = launch_instances(
        session=session,
        instance_types=instance_types,
        ami=args.ami,
        subnet_id=args.subnet_id,
        security_group_id=args.security_group_id,
        key_name=args.key_name,
        user_data=user_data,
        tags=tags,
        dry_run=args.dry_run,
    )
    print(json.dumps(responses, indent=2, default=str))
    if args.dry_run:
        print("Dry-run only; re-run with --no-dry-run to launch.")


if __name__ == "__main__":
    main()
