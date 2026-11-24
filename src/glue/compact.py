"""Glue (PySpark) compaction job.

Ingest writes one small Parquet file per (event_date, batch) so reruns are idempotent. That leaves many small
files per day. This job rewrites each day in the range into a few large files, removing duplicates that span
batches, so Athena scans less and lists fewer objects.

The per-batch files stay the system of record. Compaction is derived: rerunning it for the same dates
overwrites only those dates (dynamic partition overwrite) and gives the same result.

Arguments: --CURATED_BUCKET, --start_date YYYY-MM-DD, --end_date YYYY-MM-DD
"""
import json
import sys
from datetime import date, datetime, timedelta, timezone

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["CURATED_BUCKET", "start_date", "end_date"])
bucket = args["CURATED_BUCKET"]
start, end = date.fromisoformat(args["start_date"]), date.fromisoformat(args["end_date"])

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
s3 = boto3.client("s3")


def has_data(prefix):
    return s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1).get("KeyCount", 0) > 0


days = [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]
paths = [f"s3://{bucket}/events/event_date={d}/" for d in days if has_data(f"events/event_date={d}/")]
summary = {"start_date": args["start_date"], "end_date": args["end_date"], "days": len(paths),
           "finished_at": None, "input_files": 0, "input_rows": 0, "output_rows": 0}

if paths:
    df = spark.read.option("basePath", f"s3://{bucket}/events/").parquet(*paths)
    summary["input_files"] = len(df.inputFiles())
    latest_first = Window.partitionBy("event_id").orderBy(F.col("event_time_utc"))
    out = (df.withColumn("_rn", F.row_number().over(latest_first))
             .filter("_rn = 1").drop("_rn"))
    out = out.cache()
    summary["input_rows"] = df.count()
    summary["output_rows"] = out.count()
    (out.repartition("event_date")                       # one task per day -> one file per day at this volume
        .sortWithinPartitions("site_id", "event_time_utc")
        .write.mode("overwrite").partitionBy("event_date")
        .parquet(f"s3://{bucket}/compacted/"))

summary["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
s3.put_object(Bucket=bucket, Key=f"compaction-runs/{args['start_date']}_{args['end_date']}.json",
              Body=json.dumps(summary, indent=1).encode(), ContentType="application/json")
print(json.dumps(summary))
