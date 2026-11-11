"""Pure feature-update logic (stdlib only). Used by the Lambda, the local poller and the offline simulator.

Replaces the Flink job [12]. Item shape (native Python types):
  known_devices: list[str]   blocked: bool        avg_amount_30d: float (major units)
  avg_n: int                 recent: [[event_ts, event_id], ...] (last 5 min of event time)
  txn_count_5m: int          last_country: str    last_event_ts: float
  updated_at: float (wall clock)                  expires_at: int (DynamoDB TTL)
"""
#fraud-Detection/feature_updater/core.py
from datetime import datetime

WINDOW_S = 300
MAX_RECENT = 200
AVG_CAP = 50          # smoothing: roughly "average over the last ~50 approved payments"
TTL_S = 30 * 86400


def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def compute_update(item, rec, now):
    """Return the updated item, or None if rec['event_id'] was already applied (duplicate delivery)."""
    it = dict(item) if item else {"blocked": False}
    recent = [list(e) for e in it.get("recent", [])]
    eid = rec["event_id"]
    if any(e[1] == eid for e in recent):
        return None
    ts = parse_ts(rec["event_time"])          # EVENT time, not wall clock
    recent.append([ts, eid])
    newest = max(e[0] for e in recent)
    recent = sorted(e for e in recent if e[0] >= newest - WINDOW_S)[-MAX_RECENT:]
    it["recent"] = recent
    it["txn_count_5m"] = len(recent)

    # Only learn "normal spend" from approved payments, so fraud cannot drag the average up.
    if rec.get("decision") == "APPROVE":
        amt = rec["amount_minor"] / 100.0
        avg = it.get("avg_amount_30d")
        if avg is None:
            avg, n = amt, 1
        else:
            n = min(max(int(it.get("avg_n", 1)), 1) + 1, AVG_CAP)
            avg = avg + (amt - avg) / n
        it["avg_amount_30d"] = round(avg, 2)
        it["avg_n"] = n

    if ts >= it.get("last_event_ts", 0):
        it["last_event_ts"] = ts
        it["last_country"] = rec["merchant"]["country"]

    # known_devices is deliberately NOT learned here: a device seen for the first time must not
    # become trusted just because we approved it. In real life a successful 3-D Secure adds it.
    it["updated_at"] = now
    it["expires_at"] = int(now + TTL_S)
    return it
