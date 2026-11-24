# Evidence: live run on AWS (2026-11-24 18:21 UTC)

Stack deployed with `terraform apply`, scenarios run with `scripts/demo.py`, then removed with `terraform destroy`.

## 1. Normal day: 4 hourly files from 250 sites
- 4 files loaded through S3 -> SQS -> Lambda in about 13s
- totals: `{"received": 30702, "written": 29557, "rejected": 276, "duplicates": 869}`

## 2. Broken feed is quarantined whole
- `raw/arrival_date=2026-11-02/broken-feed.jsonl` failed the quality gate (reject rate 100%) and went to the quarantine bucket. Curated data untouched.

## 3. A batch that fails every retry, then recovers
- after 3 attempts the S3 notification moved to the dead-letter queue (~555s); in the first live run the `dlq-not-empty` alarm went OK -> ALARM at 17:51 UTC and back to OK after replay. In this run the DLQ was drained within about a minute, before SQS published the metric, so the alarm did not trip
- destination check before replay: unprocessed = `['raw/arrival_date=2026-11-02/flaky.jsonl']`
- replay snapshot-copied `['raw/arrival_date=2026-11-02/flaky.jsonl']` back through the normal path
- destination check after replay: unprocessed = `[]`, DLQ depth = 0

## 4. Rerun the whole day with the backfill state machine
- execution SUCCEEDED; totals before `{"received": 38541, "written": 36919, "rejected": 539, "duplicates": 1083}`, after `{"received": 38541, "written": 36919, "rejected": 539, "duplicates": 1083}`
- unchanged: **True** (reruns overwrite the same per-batch files)
- the same execution then ran the Glue (PySpark) compaction job: SUCCEEDED in 79s

## 5. Compaction
- 10 small per-batch Parquet files rewritten into 2 files across 2 days
- rows in 36919, rows out 36919 (cross-batch duplicates removed: 0)

## Destination check and Athena
- raw objects 6, manifests 6, lost events **0**, ok **True**

**Rows by event date (late data lands in earlier dates)**

```sql
SELECT event_date, count(*) AS rows FROM events_dedup GROUP BY 1 ORDER BY 1
```

| event_date | rows |
|---|---|
| 2026-11-01 | 535 |
| 2026-11-02 | 36384 |

**Sites reporting and duplicates removed across batches**

```sql
SELECT count(DISTINCT site_id) AS sites, (SELECT count(*) FROM events) - count(*) AS cross_batch_dupes FROM events_dedup
```

| sites | cross_batch_dupes |
|---|---|
| 250 | 0 |

**Compacted table matches the deduplicated view**

```sql
SELECT (SELECT count(*) FROM events_compacted) AS compacted_rows, (SELECT count(*) FROM events_dedup) AS dedup_rows
```

| compacted_rows | dedup_rows |
|---|---|
| 36919 | 36919 |

**Top idle sites on the main day**

```sql
SELECT site_id, round(sum(value),1) AS idle_minutes FROM events_dedup WHERE event_date = '2026-11-02' AND metric = 'idle_minutes' GROUP BY 1 ORDER BY 2 DESC LIMIT 5
```

| site_id | idle_minutes |
|---|---|
| SITE-144 | 17399.4 |
| SITE-183 | 17334.5 |
| SITE-238 | 16776.6 |
| SITE-052 | 16346.8 |
| SITE-069 | 16187.0 |
