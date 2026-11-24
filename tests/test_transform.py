import json
from datetime import datetime, timezone

from pipeline import generator, quality, transform

START = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)


def ev(**kw):
    base = {"event_id": "e1", "site_id": "SITE-001", "event_time": "2026-11-02T12:00:00+00:00",
            "metric": "queue_depth", "value": 4.0}
    base.update(kw)
    return json.dumps(base)


def test_offsets_normalize_to_utc_and_pick_the_utc_date():
    batch = transform.parse_lines([ev(event_time="2026-11-01T22:30:00-07:00")])
    row = batch.rows[0]
    assert row["event_time_utc"] == "2026-11-02T05:30:00Z"
    assert row["event_date"] == "2026-11-02"


def test_duplicates_collapse_to_one_row():
    batch = transform.parse_lines([ev(), ev(), ev(event_id="e2")])
    assert [r["event_id"] for r in batch.rows] == ["e1", "e2"]
    assert batch.duplicates == 1
    assert batch.received == 3


def test_every_malformed_line_is_kept_with_a_reason():
    lines = ['{"event_id": "x"', ev(value="n/a"), ev(event_time="yesterday-ish"),
             json.dumps({"event_id": "e9", "event_time": "2026-11-02T12:00:00Z", "metric": "queue_depth", "value": 1}),
             ev(event_time="2026-11-02T12:00:00"), ev(metric="temperature"), "[1,2]"]
    batch = transform.parse_lines(lines)
    assert batch.rows == []
    assert [r["reason"] for r in batch.rejected] == [
        "invalid_json", "bad_value", "bad_event_time", "missing:site_id",
        "event_time_without_offset", "unknown_metric", "not_an_object"]


def test_late_events_land_in_the_partition_they_happened_in():
    batch = transform.parse_lines([ev(event_id="now"), ev(event_id="late", event_time="2026-11-01T03:00:00Z")])
    parts = transform.partition(batch.rows)
    assert set(parts) == {"2026-11-01", "2026-11-02"}
    assert [r["event_id"] for r in parts["2026-11-01"]] == ["late"]


def test_merge_is_idempotent_and_order_independent():
    a = transform.parse_lines([ev(event_id="a"), ev(event_id="b", site_id="SITE-002")]).rows
    once = transform.merge([], a)
    assert transform.merge(once, a) == once
    assert transform.merge([], list(reversed(a))) == once


def test_generator_produces_the_defects_the_pipeline_must_handle():
    lines = generator.generate(START, minutes=60, seed=1)
    batch = transform.parse_lines(lines)
    assert len({r["site_id"] for r in batch.rows}) == generator.SITE_COUNT
    assert batch.duplicates > 0 and batch.rejected
    assert any(r["event_date"] != "2026-11-02" for r in batch.rows)  # late arrivals
    stamps = [json.loads(line).get("event_time", "") for line in lines if line.endswith("}")]
    assert any(s[19:] not in ("", "+00:00") for s in stamps if len(s) > 19)  # some local offsets


def test_generator_is_deterministic():
    assert generator.generate(START, 10, seed=3) == generator.generate(START, 10, seed=3)


def test_quality_gate_passes_normal_noise_and_blocks_a_broken_feed():
    normal = transform.parse_lines(generator.generate(START, 30, seed=2))
    assert quality.check(normal).passed
    broken = transform.parse_lines(['not json'] * 10 + [ev()])
    v = quality.check(broken)
    assert not v.passed and v.reasons[0].startswith("reject_rate")


def test_quality_gate_blocks_impossible_values():
    v = quality.check(transform.parse_lines([ev(value=-3), ev(event_id="e2", value=99_999)]))
    assert v.reasons == ["negative_values", "values_above_10000"]


def test_empty_batch_is_blocked():
    assert quality.check(transform.parse_lines([])).reasons == ["empty_batch"]
