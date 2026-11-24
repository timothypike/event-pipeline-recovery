"""Lambda entry points. One package, four handlers, each with its own least-privilege IAM role in Terraform.

ingest   S3 event (via SQS) or {"keys": [...]} from the backfill state machine -> curated Parquet + manifest
replay   drains the dead-letter queue: snapshot-copies each failed raw object and sends it back through ingest
list_raw lists raw keys for a date range (backfill helper for Step Functions)
check    destination check: every raw object has a manifest, and every manifest balances
"""
from __future__ import annotations

import base64
import json
import logging
import os
import urllib.parse
from datetime import date, datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

from . import quality, storage, transform

log = logging.getLogger()
log.setLevel(logging.INFO)

RAW = os.environ.get("RAW_BUCKET", "")
CURATED = os.environ.get("CURATED_BUCKET", "")
QUARANTINE = os.environ.get("QUARANTINE_BUCKET", "")
DLQ_URL = os.environ.get("DLQ_URL", "")
FAIL_MARKER = "__force_failure__"  # demo hook: a raw file containing this line fails ingest on purpose


def _s3():
    return boto3.client("s3")


def _sqs():
    return boto3.client("sqs")


# ---------------------------------------------------------------- ingest
def ingest(event, context=None, s3=None):
    s3 = s3 or _s3()
    keys = event.get("keys") or [k for k in _keys_from_sqs(event)]
    return {"batches": [process_object(s3, k) for k in keys]}


def _keys_from_sqs(event):
    for record in event.get("Records", []):
        body = json.loads(record["body"])
        for s3rec in body.get("Records", []):  # S3 -> SQS notification shape
            yield urllib.parse.unquote_plus(s3rec["s3"]["object"]["key"])


def process_object(s3, key: str) -> dict:
    lines, meta = storage.read_lines(s3, RAW, key)
    source_key = meta.get("source-key", key)          # replay copies point back to the original
    bid = storage.batch_id(source_key)
    if key.startswith("raw/"):
        # A recovery snapshot, if one exists, is the corrected copy of this batch. Backfills use it too.
        snapshot = f"replay/{bid}.jsonl"
        try:
            s3.head_object(Bucket=RAW, Key=snapshot)
            lines, _ = storage.read_lines(s3, RAW, snapshot)
        except ClientError:
            pass
    if any(line.strip() == FAIL_MARKER for line in lines):
        raise RuntimeError(f"forced failure for {source_key}")  # lets the demo exercise the DLQ path
    batch = transform.parse_lines(lines, source=source_key)
    verdict = quality.check(batch)
    manifest = {"batch_id": bid, "source_key": source_key, "processed_key": key,
                "received": batch.received, "written": 0, "rejected": len(batch.rejected),
                "duplicates": batch.duplicates, "dates": [], "status": "", "reasons": verdict.reasons}
    if not verdict.passed:
        s3.put_object(Bucket=QUARANTINE, Key=f"batches/{bid}/{source_key.rsplit('/', 1)[-1]}",
                      Body="\n".join(lines).encode())
        storage.put_json(s3, QUARANTINE, f"batches/{bid}/verdict.json", manifest)
        manifest["status"] = "quarantined"
        storage.put_json(s3, CURATED, f"manifests/{bid}.json", manifest)
        log.warning("quarantined %s: %s", source_key, verdict.reasons)
        return manifest
    for event_date, rows in transform.partition(batch.rows).items():
        s3.put_object(Bucket=CURATED, Key=storage.curated_key(event_date, bid), Body=storage.to_parquet(rows))
        manifest["dates"].append(event_date)
        manifest["written"] += len(rows)
    if batch.rejected:
        s3.put_object(Bucket=QUARANTINE, Key=f"rejects/{bid}.jsonl",
                      Body="\n".join(json.dumps(r) for r in batch.rejected).encode())
    manifest["status"] = "loaded"
    storage.put_json(s3, CURATED, f"manifests/{bid}.json", manifest)
    log.info("loaded %s: %s", source_key, {k: manifest[k] for k in ("received", "written", "rejected", "duplicates")})
    return manifest


# ---------------------------------------------------------------- replay
def replay(event, context=None, s3=None, sqs=None):
    """Snapshot-copy recovery. Copy first, delete the DLQ message second, so a crash mid-way loses nothing."""
    s3, sqs = s3 or _s3(), sqs or _sqs()
    replayed, missing = [], []
    while True:
        resp = sqs.receive_message(QueueUrl=DLQ_URL, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        msgs = resp.get("Messages", [])
        if not msgs:
            break
        for m in msgs:
            for key in _keys_from_sqs({"Records": [{"body": m["Body"]}]}):
                try:
                    head = s3.head_object(Bucket=RAW, Key=key)
                except ClientError:
                    missing.append(key)
                    continue
                source_key = head.get("Metadata", {}).get("source-key", key)
                body = s3.get_object(Bucket=RAW, Key=key)["Body"].read().decode()
                clean = "\n".join(line for line in body.splitlines() if line.strip() != FAIL_MARKER)
                s3.put_object(Bucket=RAW, Key=f"replay/{storage.batch_id(source_key)}.jsonl",
                              Body=clean.encode(), Metadata={"source-key": source_key})
                replayed.append(source_key)
            sqs.delete_message(QueueUrl=DLQ_URL, ReceiptHandle=m["ReceiptHandle"])
    return {"replayed": replayed, "missing_source": missing}


# ---------------------------------------------------------------- backfill helper
def list_raw(event, context=None, s3=None):
    s3 = s3 or _s3()
    start, end = date.fromisoformat(event["start_date"]), date.fromisoformat(event["end_date"])
    keys, d = [], start
    while d <= end:
        keys += storage.list_keys(s3, RAW, f"raw/arrival_date={d.isoformat()}/")
        d += timedelta(days=1)
    return {"chunks": [{"keys": keys[i:i + 10]} for i in range(0, len(keys), 10)], "count": len(keys)}


# ---------------------------------------------------------------- optional streaming input
def stream_to_raw(event, context=None, s3=None):
    """Kinesis batch -> one raw JSONL file. The key comes from the shard and first sequence number, so a
    retried batch overwrites the same file instead of creating a second copy. From raw/, streamed data takes
    the same path as files: quality gate, manifests, DLQ replay, destination check."""
    s3 = s3 or _s3()
    records = event.get("Records", [])
    if not records:
        return {"written": None, "records": 0}
    lines = [base64.b64decode(r["kinesis"]["data"]).decode("utf-8", errors="replace").strip() for r in records]
    first = records[0]
    shard = first["eventID"].split(":")[0]
    seq = first["kinesis"]["sequenceNumber"]
    arrived = datetime.fromtimestamp(first["kinesis"]["approximateArrivalTimestamp"], tz=timezone.utc)
    key = f"raw/arrival_date={arrived:%Y-%m-%d}/stream-{shard}-{seq}.jsonl"
    s3.put_object(Bucket=RAW, Key=key, Body="\n".join(lines).encode())
    return {"written": key, "records": len(lines)}


# ---------------------------------------------------------------- destination check
def check(event=None, context=None, s3=None):
    s3 = s3 or _s3()
    raw = [k for k in storage.list_keys(s3, RAW, "raw/") if k.endswith(".jsonl")]
    manifests = {}
    for k in storage.list_keys(s3, CURATED, "manifests/"):
        m = json.loads(s3.get_object(Bucket=CURATED, Key=k)["Body"].read())
        manifests[m["source_key"]] = m
    unprocessed = [k for k in raw if k not in manifests]
    loaded = [m for m in manifests.values() if m["status"] == "loaded"]
    gap = {m["source_key"]: m["received"] - (m["written"] + m["rejected"] + m["duplicates"]) for m in loaded}
    unbalanced = [k for k, g in gap.items() if g]
    totals = {f: sum(m[f] for m in manifests.values()) for f in ("received", "written", "rejected", "duplicates")}
    report = {"raw_objects": len(raw), "manifests": len(manifests), "unprocessed": unprocessed,
              "unbalanced": unbalanced, "quarantined": [m["source_key"] for m in manifests.values() if m["status"] == "quarantined"],
              "totals": totals, "lost_events": sum(gap.values()),
              "ok": not unprocessed and not unbalanced}
    log.info("destination check: %s", json.dumps({k: report[k] for k in ("raw_objects", "manifests", "ok")}))
    return report
