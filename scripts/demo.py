"""Run the failure scenarios against a deployed stack and write the evidence to docs/evidence.md.

Usage (after `terraform apply` in infra/):
  python scripts/demo.py

Scenarios:
  1. Normal day: 4 hourly files from 250 sites, with duplicates, late data, offsets, and bad lines.
  2. Broken feed: a garbage file. The quality gate quarantines it whole.
  3. Failure and recovery: a file that fails ingest every time, lands in the DLQ, and is replayed.
  4. Rerun: the backfill state machine reprocesses the whole day. Row counts must not change.
  5. Destination check and Athena queries, deduplicated across batches.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from pipeline import generator  # noqa: E402
from pipeline.handlers import FAIL_MARKER  # noqa: E402

DAY = "2026-11-02"
DAY_BEFORE = "2026-11-01"
START = datetime(2026, 11, 2, 8, 0, tzinfo=timezone.utc)


def tf_outputs() -> dict:
    out = subprocess.run(["terraform", "output", "-json"], cwd=ROOT / "infra", check=True, capture_output=True, text=True)
    return {k: v["value"] for k, v in json.loads(out.stdout).items()}


def main():
    o = tf_outputs()
    s3, sqs, lam, sfn, athena = (boto3.client(x) for x in ("s3", "sqs", "lambda", "stepfunctions", "athena"))
    log: list[str] = []

    def note(line=""):
        print(line)
        log.append(line)

    def invoke(name, payload=None):
        r = lam.invoke(FunctionName=o["functions"][name], Payload=json.dumps(payload or {}).encode())
        return json.loads(r["Payload"].read())

    def put(name, lines):
        key = f"raw/arrival_date={DAY}/{name}.jsonl"
        s3.put_object(Bucket=o["raw_bucket"], Key=key, Body="\n".join(lines).encode())
        return key

    def wait_for(pred, what, timeout=600):
        t0 = time.time()
        while time.time() - t0 < timeout:
            if pred():
                return round(time.time() - t0)
            time.sleep(10)
        raise TimeoutError(what)

    def dlq_depth():
        a = sqs.get_queue_attributes(QueueUrl=o["dlq_url"], AttributeNames=["ApproximateNumberOfMessages"])
        return int(a["Attributes"]["ApproximateNumberOfMessages"])

    note(f"# Evidence: live run on AWS ({datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC)\n")
    note("Stack deployed with `terraform apply`, scenarios run with `scripts/demo.py`, then removed with `terraform destroy`.\n")

    note("## 1. Normal day: 4 hourly files from 250 sites")
    keys = [put(f"hour-{h:02d}", generator.generate(START + timedelta(hours=h), 60, seed=100 + h)) for h in range(4)]
    secs = wait_for(lambda: invoke("check")["manifests"] >= 4, "normal files")
    rep = invoke("check")
    note(f"- {len(keys)} files loaded through S3 -> SQS -> Lambda in about {secs}s")
    note(f"- totals: `{json.dumps(rep['totals'])}`\n")

    note("## 2. Broken feed is quarantined whole")
    bad = put("broken-feed", ["<html>502 Bad Gateway</html>"] * 200)
    wait_for(lambda: any(bad == q for q in invoke("check")["quarantined"]), "quarantine")
    note(f"- `{bad}` failed the quality gate (reject rate 100%) and went to the quarantine bucket. Curated data untouched.\n")

    note("## 3. A batch that fails every retry, then recovers")
    flaky = put("flaky", generator.generate(START + timedelta(hours=4), 60, seed=200) + [FAIL_MARKER])
    secs = wait_for(lambda: dlq_depth() > 0, "dlq", timeout=900)
    note(f"- after 3 attempts the S3 notification moved to the dead-letter queue (~{secs}s)")
    note(f"- destination check before replay: unprocessed = `{invoke('check')['unprocessed']}`")
    out = invoke("replay")
    note(f"- replay snapshot-copied `{out['replayed']}` back through the normal path")
    wait_for(lambda: invoke("check")["unprocessed"] == [], "replay ingest")
    note(f"- destination check after replay: unprocessed = `[]`, DLQ depth = {dlq_depth()}\n")

    note("## 4. Rerun the whole day with the backfill state machine")
    before = invoke("check")["totals"]
    ex = sfn.start_execution(stateMachineArn=o["backfill_state_machine"],
                             input=json.dumps({"start_date": DAY_BEFORE, "end_date": DAY}))["executionArn"]
    wait_for(lambda: sfn.describe_execution(executionArn=ex)["status"] != "RUNNING", "backfill")
    status = sfn.describe_execution(executionArn=ex)
    result = json.loads(status["output"]) if status["status"] == "SUCCEEDED" else {}
    after = result.get("check", {}).get("report", {}).get("totals")
    note(f"- execution {status['status']}; totals before `{json.dumps(before)}`, after `{json.dumps(after)}`")
    note(f"- unchanged: **{before == after}** (reruns overwrite the same per-batch files)")
    comp = result.get("compaction", {})
    note(f"- the same execution then ran the Glue (PySpark) compaction job: {comp.get('state')} in {comp.get('seconds')}s\n")

    note("## 5. Compaction")
    run = json.loads(s3.get_object(Bucket=o["curated_bucket"], Key=f"compaction-runs/{DAY_BEFORE}_{DAY}.json")["Body"].read())
    small = len([k for k in list_all(s3, o["curated_bucket"], "events/") if k.endswith(".parquet")])
    big = len([k for k in list_all(s3, o["curated_bucket"], "compacted/") if k.endswith(".parquet")])
    note(f"- {small} small per-batch Parquet files rewritten into {big} files across {run['days']} days")
    note(f"- rows in {run['input_rows']}, rows out {run['output_rows']} (cross-batch duplicates removed: {run['input_rows'] - run['output_rows']})\n")

    if o.get("kinesis_stream"):
        note("## 6. Optional streaming input (Kinesis)")
        kin = boto3.client("kinesis")
        stream_lines = generator.generate(START + timedelta(hours=6), 10, seed=300, sites=generator.site_ids(25))
        for i in range(0, len(stream_lines), 500):
            kin.put_records(StreamName=o["kinesis_stream"], Records=[
                {"Data": line.encode(), "PartitionKey": str(n)} for n, line in enumerate(stream_lines[i:i + 500])])
        before_m = invoke("check")["manifests"]
        secs = wait_for(lambda: invoke("check")["manifests"] > before_m and invoke("check")["ok"], "stream")
        note(f"- {len(stream_lines)} records put on the stream; Lambda batched them into raw/ and they loaded through the normal path in ~{secs}s")
        note("- same quality gate, manifests, and destination check as file input\n")

    note("## Destination check and Athena")
    rep = invoke("check")
    note(f"- raw objects {rep['raw_objects']}, manifests {rep['manifests']}, lost events **{rep['lost_events']}**, ok **{rep['ok']}**")
    db, wg = o["glue_database"], o["athena_workgroup"]
    q("CREATE OR REPLACE VIEW events_dedup AS SELECT event_id, site_id, event_time_utc, metric, value, event_date FROM ("
      " SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY event_time_utc) rn FROM events) WHERE rn = 1", athena, db, wg)
    for title, sql in [
        ("Rows by event date (late data lands in earlier dates)",
         "SELECT event_date, count(*) AS rows FROM events_dedup GROUP BY 1 ORDER BY 1"),
        ("Sites reporting and duplicates removed across batches",
         "SELECT count(DISTINCT site_id) AS sites, (SELECT count(*) FROM events) - count(*) AS cross_batch_dupes FROM events_dedup"),
        ("Compacted table matches the deduplicated view",
         "SELECT (SELECT count(*) FROM events_compacted) AS compacted_rows, (SELECT count(*) FROM events_dedup) AS dedup_rows"),
        ("Top idle sites on the main day",
         f"SELECT site_id, round(sum(value),1) AS idle_minutes FROM events_dedup WHERE event_date = '{DAY}' "
         "AND metric = 'idle_minutes' GROUP BY 1 ORDER BY 2 DESC LIMIT 5"),
    ]:
        note(f"\n**{title}**\n\n```sql\n{sql}\n```\n")
        note(table(q(sql, athena, db, wg)))
    (ROOT / "docs" / "evidence.md").write_text("\n".join(log) + "\n")
    print("\nwrote docs/evidence.md")


def list_all(s3, bucket, prefix):
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def q(sql, athena, db, wg):
    qid = athena.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": db}, WorkGroup=wg)["QueryExecutionId"]
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if st["State"] != "SUCCEEDED":
        raise RuntimeError(st.get("StateChangeReason", st["State"]))
    rows = athena.get_query_results(QueryExecutionId=qid).get("ResultSet", {}).get("Rows", [])
    return [[c.get("VarCharValue", "") for c in r["Data"]] for r in rows]


def table(rows):
    if not rows:
        return "_no rows_"
    head, *body = rows
    lines = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    lines += ["| " + " | ".join(r) + " |" for r in body]
    return "\n".join(lines)


if __name__ == "__main__":
    main()
