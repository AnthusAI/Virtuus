#!/usr/bin/env python3
"""
Download storage-mode benchmark artifacts from S3 into a local folder.

Example:
  python tools/pull_storage_results_s3.py \
    --bucket my-bucket \
    --prefix virtuus-bench/ \
    --dest benchmarks/output_storage/ec2-sync
"""

from __future__ import annotations

import argparse
from pathlib import Path, PurePosixPath

try:
    import boto3  # type: ignore
except ImportError:  # pragma: no cover - optional dep
    boto3 = None


def ensure_boto(profile: str | None):
    if boto3 is None:
        raise SystemExit("boto3 is required: pip install boto3")
    return boto3.Session(profile_name=profile) if profile else boto3.Session()


def pull(bucket: str, prefix: str, dest: Path, profile: str | None) -> None:
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"
    session = ensure_boto(profile)
    s3 = session.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    dest.mkdir(parents=True, exist_ok=True)
    dest_root = dest.resolve()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            rel = key[len(prefix) :] if key.startswith(prefix) else key
            rel_path = PurePosixPath(rel)
            if (
                rel_path.is_absolute()
                or ".." in rel_path.parts
                or rel_path.as_posix()
                in {
                    "",
                    ".",
                }
            ):
                raise ValueError(f"Refusing unsafe relative path from S3 key: {key}")
            out_path = (dest_root / rel_path.as_posix()).resolve()
            try:
                out_path.relative_to(dest_root)
            except ValueError as exc:
                raise ValueError(f"Refusing path traversal from S3 key: {key}") from exc
            out_path.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, key, str(out_path))
            print(f"downloaded s3://{bucket}/{key} -> {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="")
    parser.add_argument("--dest", default="benchmarks/output_storage/ec2-sync")
    parser.add_argument("--profile", help="AWS profile name")
    args = parser.parse_args()

    pull(args.bucket, args.prefix, Path(args.dest), args.profile)


if __name__ == "__main__":
    main()
