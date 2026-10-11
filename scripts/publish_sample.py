"""Generate and upload a synthetic CSV batch to the private raw S3 prefix."""

from __future__ import annotations

import argparse
import csv
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import boto3

from data.generate_events import FIELDS, generate


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True, help="Private bucket name from Terraform output")
    parser.add_argument("--region", default="ap-south-1")
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if not 1 <= args.rows <= 1_000_000:
        parser.error("--rows must be between 1 and 1,000,000")

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    key = f"raw/events/events-{stamp}.csv"
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "events.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(generate(args.rows, args.seed))
        boto3.client("s3", region_name=args.region).upload_file(str(path), args.bucket, key)
    print(f"Uploaded {args.rows:,} synthetic rows to s3://{args.bucket}/{key}")


if __name__ == "__main__":
    main()
