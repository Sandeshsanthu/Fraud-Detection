# filename: Brain/app/rules.py
from dataclasses import dataclass, field
import yaml


@dataclass
class Features:
    freshness: str                 # fresh | stale | missing | unavailable
    count_5m: int                  # includes the current payment
    amount_ratio: float | None     # this amount / 30-day average
    new_device: bool | None        # None = device unknown
    blocked: bool
    amount_minor: int


@dataclass
class RuleResult:
    decision: str = "APPROVE"      # APPROVE | STEP_UP | DECLINE
    reasons: list[str] = field(default_factory=list)
    score: float | None = None     # ML score, if produced
    shadow: str | None = None      # what the ML score alone would have decided


class Rules:
    def __init__(self, path: str):
        self.path, self.cfg = path, {}
        self.reload()

    def reload(self) -> None:
        with open(self.path) as f:
            new = yaml.safe_load(f)
        p = new.get("policy", {})
        if not p.get("approve_below", 0.70) < p.get("decline_above", 0.92):
            raise ValueError("approve_below must be lower than decline_above")
        self.cfg = new                  # swap only after validation passes

    def evaluate(self, f: Features) -> RuleResult:
        c = self.cfg
        if f.blocked:                   # trust the flag even if stale
            return RuleResult("DECLINE", ["blocked_card"])
        if f.freshness == "fresh" and f.count_5m > c["velocity_max_5m"]:
            return RuleResult("DECLINE", ["velocity_5m"])
        reasons = []
        if f.new_device and f.amount_ratio and f.amount_ratio >= c["large_amount_multiple"]:
            reasons.append("large_amount_new_device")
        floor = c["stepup_min_minor"].get(f.freshness)
        if floor is not None and f.amount_minor >= floor:
            reasons.append(f"{f.freshness}_features")
        return RuleResult("STEP_UP" if reasons else "APPROVE", reasons)

    def fallback(self, amount_minor: int) -> RuleResult:
        if amount_minor >= self.cfg["stepup_min_minor"]["unavailable"]:
            return RuleResult("STEP_UP", ["fallback"])
        return RuleResult("APPROVE", ["fallback"])
