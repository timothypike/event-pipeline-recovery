"""End-to-end handler tests against mocked S3 and SQS (moto). No AWS account needed."""
import json
import os
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

os.environ.update(AWS_DEFAULT_REGION="us-east-1", AWS_ACCESS_KEY_ID="test", AWS_SECRET_ACCESS_KEY="test",
                  RAW_BUCKET="raw", CURATED_BUCKET="curated", QUARANTINE_BUCKET="quarantine")

from pipeline import generator, handlers, storage  # noqa: E402

START = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def aws():
    with mock_aws():
        s3, sqs = boto3.client("s3"), boto3.client("sqs")
        for b in ("raw", "curated", "quarantine"):
            s3.create_bucket(Bucket=b)
        handlers.DLQ_URL = sqs.create_queue(QueueName="dlq")["QueueUrl"]
        yield s3, sqs


def put_raw(s3, name, lines, day="2026-11-02"):
    key = f"raw/arrival_date={day}/{name}.jsonl"
    s3.put_object(Bucket="raw", Key=key, Body="\n".join(lines).encode())
    return key


def sqs_event(key):
    return {"Records": [{"body": json.dumps({"Records": [{"s3": {"object": {"key": key}}}]})}]}


def curated_rows(s3):
    rows = []
    for k in storage.list_keys(s3, "curated", "events/"):
        rows += storage.from_parquet(s3.get_object(Bucket="curated", Key=k)["Body"].read())
    return rows


def test_batch_loads_balances_and_partitions_late_data(aws):
    s3, _ = aws
    key = put_raw(s3, "b1", generator.generate(START, 60, seed=5))
    m = handlers.ingest(sqs_event(key), s3=s3)["batches"][0]
    assert m["status"] == "loaded"
    assert m["received"] == m["written"] + m["rejected"] + m["duplicates"]
    assert len(m["dates"]) > 1  # late arrivals went to older partitions
    assert len(curated_rows(s3)) == m["written"]
    assert s3.get_object(Bucket="quarantine", Key=f"rejects/{m['batch_id']}.jsonl")


def test_rerunning_a_batch_changes_nothing(aws):
    s3, _ = aws
    key = put_raw(s3, "b1", generator.generate(START, 30, seed=6))
    handlers.ingest({"keys": [key]}, s3=s3)
    first = sorted(storage.list_keys(s3, "curated", "events/")), curated_rows(s3)
    handlers.ingest({"keys": [key]}, s3=s3)
    assert (sorted(storage.list_keys(s3, "curated", "events/")), curated_rows(s3)) == first


def test_broken_feed_is_quarantined_whole(aws):
    s3, _ = aws
    key = put_raw(s3, "bad", ["garbage"] * 50)
    m = handlers.ingest({"keys": [key]}, s3=s3)["batches"][0]
    assert m["status"] == "quarantined"
    assert storage.list_keys(s3, "curated", "events/") == []
    assert storage.list_keys(s3, "quarantine", f"batches/{m['batch_id']}/")


def test_failed_batch_is_recovered_through_the_normal_path(aws):
    s3, sqs = aws
    lines = generator.generate(START, 20, seed=8)
    key = put_raw(s3, "flaky", lines + [handlers.FAIL_MARKER])
    with pytest.raises(RuntimeError):
        handlers.ingest(sqs_event(key), s3=s3)
    # After retries, SQS moves the S3 notification to the DLQ. Simulate that:
    sqs.send_message(QueueUrl=handlers.DLQ_URL, MessageBody=sqs_event(key)["Records"][0]["body"])
    assert handlers.check(s3=s3)["unprocessed"] == [key]

    out = handlers.replay({}, s3=s3, sqs=sqs)
    assert out == {"replayed": [key], "missing_source": []}
    replay_key = f"replay/{storage.batch_id(key)}.jsonl"
    m = handlers.ingest(sqs_event(replay_key), s3=s3)["batches"][0]  # S3 notification on replay/ prefix
    assert m["source_key"] == key and m["status"] == "loaded"
    assert sqs.receive_message(QueueUrl=handlers.DLQ_URL).get("Messages") is None

    report = handlers.check(s3=s3)
    assert report["ok"] and report["lost_events"] == 0 and report["unprocessed"] == []


def test_replay_of_same_batch_twice_writes_same_files(aws):
    s3, sqs = aws
    key = put_raw(s3, "b2", generator.generate(START, 10, seed=9))
    handlers.ingest({"keys": [key]}, s3=s3)
    before = curated_rows(s3)
    for _ in range(2):
        sqs.send_message(QueueUrl=handlers.DLQ_URL, MessageBody=sqs_event(key)["Records"][0]["body"])
        handlers.replay({}, s3=s3, sqs=sqs)
        handlers.ingest({"keys": [f"replay/{storage.batch_id(key)}.jsonl"]}, s3=s3)
    assert curated_rows(s3) == before


def test_backfill_lists_keys_in_chunks_by_arrival_date(aws):
    s3, _ = aws
    for i in range(12):
        put_raw(s3, f"f{i:02d}", ["x"], day="2026-11-01")
    put_raw(s3, "other", ["x"], day="2026-11-05")
    out = handlers.list_raw({"start_date": "2026-11-01", "end_date": "2026-11-02"}, s3=s3)
    assert out["count"] == 12 and [len(c["keys"]) for c in out["chunks"]] == [10, 2]


def test_destination_check_flags_unprocessed_objects(aws):
    s3, _ = aws
    done = put_raw(s3, "done", generator.generate(START, 5, seed=1))
    todo = put_raw(s3, "todo", generator.generate(START, 5, seed=2))
    handlers.ingest({"keys": [done]}, s3=s3)
    report = handlers.check(s3=s3)
    assert report["unprocessed"] == [todo] and not report["ok"]


def test_backfill_of_a_recovered_batch_uses_the_snapshot(aws):
    s3, sqs = aws
    key = put_raw(s3, "flaky", generator.generate(START, 10, seed=4) + [handlers.FAIL_MARKER])
    sqs.send_message(QueueUrl=handlers.DLQ_URL, MessageBody=sqs_event(key)["Records"][0]["body"])
    handlers.replay({}, s3=s3, sqs=sqs)
    handlers.ingest({"keys": [f"replay/{storage.batch_id(key)}.jsonl"]}, s3=s3)
    before = curated_rows(s3)
    m = handlers.ingest({"keys": [key]}, s3=s3)["batches"][0]  # what the backfill state machine does
    assert m["status"] == "loaded" and curated_rows(s3) == before


def kinesis_event(lines, seq="4960000001", shard="shardId-000000000000", ts=1793620800.0):
    import base64
    return {"Records": [{"eventID": f"{shard}:{int(seq) + i}",
                         "kinesis": {"data": base64.b64encode(line.encode()).decode(),
                                     "sequenceNumber": str(int(seq) + i), "approximateArrivalTimestamp": ts}}
                        for i, line in enumerate(lines)]}


def test_stream_batch_lands_in_raw_and_takes_the_normal_path(aws):
    s3, _ = aws
    lines = generator.generate(START, 5, seed=11)
    out = handlers.stream_to_raw(kinesis_event(lines), s3=s3)
    assert out["records"] == len(lines) and out["written"].startswith("raw/arrival_date=2026-11-")
    m = handlers.ingest({"keys": [out["written"]]}, s3=s3)["batches"][0]
    assert m["status"] == "loaded" and m["received"] == len(lines)
    assert handlers.check(s3=s3)["ok"]


def test_retried_stream_batch_overwrites_instead_of_duplicating(aws):
    s3, _ = aws
    ev = kinesis_event(generator.generate(START, 3, seed=12))
    first = handlers.stream_to_raw(ev, s3=s3)["written"]
    second = handlers.stream_to_raw(ev, s3=s3)["written"]
    assert first == second and len(storage.list_keys(s3, "raw", "raw/")) == 1
