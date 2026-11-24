"""Synthetic operational events: 250 sites reporting every 1-3 minutes, with the mess real feeds have.

Nothing here comes from a real system. The defects are deliberate so the pipeline has something to handle:
duplicates (retries), late arrivals, mixed timezone offsets, and malformed records.
"""
from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

SITE_COUNT = 250
METRICS = ("units_processed", "idle_minutes", "queue_depth")


@dataclass(frozen=True)
class Defects:
    duplicate_rate: float = 0.03
    late_rate: float = 0.02
    malformed_rate: float = 0.01
    offset_rate: float = 0.25  # share of events stamped with a local offset instead of UTC


def site_ids(count: int = SITE_COUNT) -> list[str]:
    return [f"SITE-{i:03d}" for i in range(1, count + 1)]


def generate(start: datetime, minutes: int, seed: int = 7, defects: Defects = Defects(),
             sites: list[str] | None = None) -> list[str]:
    """Return JSON lines for every site reporting every 1-3 minutes over `minutes` minutes from `start` (UTC)."""
    rng = random.Random(seed)
    sites = sites or site_ids()
    offsets = [timedelta(hours=h) for h in (-7, -5, -4, 0, 1)]
    lines: list[str] = []
    for site in sites:
        t = start + timedelta(minutes=rng.randint(0, 2))
        seq = 0
        while t < start + timedelta(minutes=minutes):
            seq += 1
            event = {
                "event_id": f"{site}-{int(t.timestamp())}-{seq}",
                "site_id": site,
                "event_time": _stamp(t, rng.choice(offsets) if rng.random() < defects.offset_rate else timedelta(0)),
                "metric": rng.choice(METRICS),
                "value": round(rng.uniform(0, 500), 2),
            }
            if rng.random() < defects.late_rate:
                # Arrives with an event_time hours earlier than its neighbors: belongs to an older partition.
                event["event_time"] = _stamp(t - timedelta(hours=rng.randint(3, 30)), timedelta(0))
            if rng.random() < defects.malformed_rate:
                lines.append(_malformed(event, rng))
            else:
                lines.append(json.dumps(event))
                if rng.random() < defects.duplicate_rate:
                    lines.append(json.dumps(event))  # producer retry: same event_id twice
            t += timedelta(minutes=rng.randint(1, 3))
    rng.shuffle(lines)
    return lines


def _stamp(t: datetime, offset: timedelta) -> str:
    return t.astimezone(timezone(offset)).isoformat()


def _malformed(event: dict, rng: random.Random) -> str:
    kind = rng.choice(("truncated", "bad_value", "missing_site", "bad_time"))
    if kind == "truncated":
        return json.dumps(event)[: rng.randint(10, 40)]
    broken = dict(event)
    if kind == "bad_value":
        broken["value"] = "n/a"
    elif kind == "missing_site":
        broken.pop("site_id")
    else:
        broken["event_time"] = "yesterday-ish"
    return json.dumps(broken)
