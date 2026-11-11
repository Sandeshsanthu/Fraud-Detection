"""Turn a low-level DynamoDB item ({'S': ..}, {'N': ..}) into the plain dict the engine expects."""
#Fraud-Detection/decision_service/store.py
import json


def parse_item(it):
    out = {"known_devices": it.get("known_devices", {}).get("SS", []),
           "blocked": it.get("blocked", {}).get("BOOL", False),
           "recent": json.loads(it["recent"]["S"]) if "recent" in it else []}
    for k in ("avg_amount_30d", "updated_at", "last_event_ts", "txn_count_5m"):
        if k in it:
            out[k] = float(it[k]["N"])
    if "last_country" in it:
        out["last_country"] = it["last_country"]["S"]
    return out
