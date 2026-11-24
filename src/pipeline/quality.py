"""Data-quality gate. A batch that fails is quarantined whole, so curated tables never see a half-bad load."""
from __future__ import annotations

from dataclasses import dataclass

from .transform import Batch

MAX_REJECT_RATE = 0.05   # more than 5% unparseable lines means the producer is broken, not noisy
MAX_VALUE = 10_000.0     # physical ceiling for any metric in one reading


@dataclass
class Verdict:
    passed: bool
    reasons: list[str]
    reject_rate: float


def check(batch: Batch) -> Verdict:
    reasons = []
    total = len(batch.rows) + len(batch.rejected)
    rate = (len(batch.rejected) / total) if total else 0.0
    if total == 0:
        reasons.append("empty_batch")
    if rate > MAX_REJECT_RATE:
        reasons.append(f"reject_rate {rate:.1%} > {MAX_REJECT_RATE:.0%}")
    if any(r["value"] < 0 for r in batch.rows):
        reasons.append("negative_values")
    if any(r["value"] > MAX_VALUE for r in batch.rows):
        reasons.append(f"values_above_{int(MAX_VALUE)}")
    return Verdict(passed=not reasons, reasons=reasons, reject_rate=rate)
