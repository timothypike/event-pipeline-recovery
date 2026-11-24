"""Pure parsing and cleaning. No AWS here, so every rule is unit-tested directly.

Contract:
- Valid events are normalized to UTC and keyed by (event_date, site_id).
- Malformed lines are kept, with a reason, for the quarantine zone. Nothing is silently dropped.
- Duplicates (same event_id) collapse to one row, so reruns and producer retries are harmless.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

REQUIRED = ("event_id", "site_id", "event_time", "metric", "value")
METRICS = {"units_processed", "idle_minutes", "queue_depth"}


@dataclass
class Batch:
    rows: list[dict] = field(default_factory=list)
    rejected: list[dict] = field(default_factory=list)
    duplicates: int = 0

    @property
    def received(self) -> int:
        return len(self.rows) + len(self.rejected) + self.duplicates


def parse_lines(lines, source: str = "") -> Batch:
    batch = Batch()
    seen: set[str] = set()
    for n, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
        row, reason = parse_one(line)
        if reason:
            batch.rejected.append({"source": source, "line": n, "reason": reason, "raw": line[:500]})
            continue
        if row["event_id"] in seen:
            batch.duplicates += 1
            continue
        seen.add(row["event_id"])
        batch.rows.append(row)
    return batch


def parse_one(line: str):
    try:
        e = json.loads(line)
    except json.JSONDecodeError:
        return None, "invalid_json"
    if not isinstance(e, dict):
        return None, "not_an_object"
    missing = [k for k in REQUIRED if k not in e or e[k] in (None, "")]
    if missing:
        return None, "missing:" + ",".join(missing)
    try:
        ts = datetime.fromisoformat(str(e["event_time"]))
    except ValueError:
        return None, "bad_event_time"
    if ts.tzinfo is None:
        return None, "event_time_without_offset"
    if e["metric"] not in METRICS:
        return None, "unknown_metric"
    try:
        value = float(e["value"])
    except (TypeError, ValueError):
        return None, "bad_value"
    utc = ts.astimezone(timezone.utc)
    return {
        "event_id": str(e["event_id"]),
        "site_id": str(e["site_id"]),
        "event_time_utc": utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "event_date": utc.strftime("%Y-%m-%d"),
        "metric": e["metric"],
        "value": value,
    }, None


def partition(rows):
    """Group rows by event_date. Late events land in the date they happened, not the date they arrived."""
    out: dict[str, list[dict]] = {}
    for r in rows:
        out.setdefault(r["event_date"], []).append(r)
    return out


def merge(existing: list[dict], incoming: list[dict]) -> list[dict]:
    """Idempotent upsert by event_id, sorted so the same input always writes the same file."""
    by_id = {r["event_id"]: r for r in existing}
    by_id.update({r["event_id"]: r for r in incoming})
    return sorted(by_id.values(), key=lambda r: (r["site_id"], r["event_time_utc"], r["event_id"]))
