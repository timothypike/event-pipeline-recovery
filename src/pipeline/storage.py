"""S3 layout and Parquet I/O.

Layout (one bucket per zone):
  raw:        raw/arrival_date=YYYY-MM-DD/<file>.jsonl        producers write here
              replay/<batch_id>.jsonl                          snapshot copies made by recovery
  curated:    events/event_date=YYYY-MM-DD/batch=<batch_id>.parquet
  quarantine: rejects/<batch_id>.jsonl                         bad lines from a batch that passed
              batches/<batch_id>/...                           whole batches the quality gate blocked
  curated:    manifests/<batch_id>.json                        what each batch received and wrote

Why one Parquet file per (event_date, batch): a rerun of the same batch overwrites exactly the files it wrote
before, so processing is idempotent and two batches never read-modify-write the same object.
Duplicates that span batches are removed at query time by the events_dedup view.
"""
from __future__ import annotations

import hashlib
import io
import json

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("site_id", pa.string()),
    ("event_time_utc", pa.string()),
    ("metric", pa.string()),
    ("value", pa.float64()),
])


def batch_id(source_key: str) -> str:
    """Stable id from the ORIGINAL raw key, so a replayed snapshot maps to the same outputs."""
    return hashlib.sha256(source_key.encode()).hexdigest()[:16]


def curated_key(event_date: str, bid: str) -> str:
    return f"events/event_date={event_date}/batch={bid}.parquet"


def to_parquet(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist([{k: r[k] for k in SCHEMA.names} for r in rows], schema=SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def from_parquet(data: bytes) -> list[dict]:
    return pq.read_table(io.BytesIO(data)).to_pylist()


def read_lines(s3, bucket: str, key: str) -> tuple[list[str], dict]:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8", errors="replace").splitlines(), obj.get("Metadata", {})


def put_json(s3, bucket: str, key: str, payload) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload, indent=1).encode(), ContentType="application/json")


def list_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in page.get("Contents", [])]
        if not page.get("IsTruncated"):
            return keys
        token = page["NextContinuationToken"]
