"""[12] Feature updater as an AWS Lambda (replaces Flink). Trigger: Kinesis event source mapping.
Kinesis keeps each card in one shard, in order, and Lambda processes a shard serially, so per-card
read-modify-write is safe. Duplicate deliveries are ignored via event_id inside core.compute_update."""
#fraud-Detection/feature_updater/handler.py
import base64, json, os, time
from decimal import Decimal

import boto3

try:
    from . import core            # running from the repo / poller
except ImportError:
    import core                   # running inside the Lambda zip (flat layout)

TABLE = os.environ.get("TABLE_NAME", "card_features")
_table = None
_DATA_ERRORS = (KeyError, TypeError, ValueError) 

def _endpoint():
    if os.environ.get("DDB_ENDPOINT"):
        return os.environ["DDB_ENDPOINT"]
    if os.environ.get("LOCALSTACK_HOSTNAME"):
        return f"http://{os.environ['LOCALSTACK_HOSTNAME']}:{os.environ.get('EDGE_PORT', '4566')}"
    return os.environ.get("AWS_ENDPOINT_URL") or None      # None on real AWS


def table():
    global _table
    if _table is None:
        kw = {"region_name": os.environ.get("AWS_REGION", "us-east-1")}
        ep = _endpoint()
        if ep:
            kw.update(endpoint_url=ep, aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
                      aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        _table = boto3.resource("dynamodb", **kw).Table(TABLE)
    return _table


def _from_ddb(raw):
    it = {}
    for k, v in raw.items():
        if k == "card_token":
            continue
        if k == "recent":
            it[k] = json.loads(v)
        elif k == "known_devices":
            it[k] = sorted(v)
        elif isinstance(v, Decimal):
            it[k] = float(v)
        else:
            it[k] = v
    return it


_WRITE = ("recent", "txn_count_5m", "avg_amount_30d", "avg_n", "last_country", "last_event_ts",
          "updated_at", "expires_at")


def _write(token, it):
    """update_item (not put_item) so attributes owned by others, like `blocked`, are never overwritten."""
    names, values, sets = {}, {}, []
    for i, k in enumerate(a for a in _WRITE if a in it):
        v = it[k]
        v = json.dumps(v) if k == "recent" else (Decimal(str(round(v, 3))) if isinstance(v, float) else v)
        names[f"#n{i}"], values[f":v{i}"] = k, v
        sets.append(f"#n{i} = :v{i}")
    table().update_item(Key={"card_token": token}, UpdateExpression="SET " + ", ".join(sets),
                        ExpressionAttributeNames=names, ExpressionAttributeValues=values)


def lambda_handler(event, context=None):
    by_card, skipped = {}, 0
    for r in event["Records"]:
        try:
            rec = json.loads(base64.b64decode(r["kinesis"]["data"]))
            by_card.setdefault(rec["card_token"], []).append(rec)      # arrival order preserved
        except _DATA_ERRORS as e:                                      # CHANGED (A4): skip, don't block the shard
            skipped += 1
            print(json.dumps({"skipped": r.get("kinesis", {}).get("sequenceNumber"),
                              "error": f"{type(e).__name__}: {e}"}), flush=True)
    now, applied, dups = time.time(), 0, 0
    for token, recs in by_card.items():
        # DynamoDB errors are NOT caught: Lambda retries the batch (capped by the mapping settings in setup)
        raw = table().get_item(Key={"card_token": token}, ConsistentRead=True).get("Item")
        item = _from_ddb(raw) if raw else None
        changed = False
        for rec in recs:
            try:
                new = core.compute_update(item, rec, now)
            except _DATA_ERRORS as e:                                  # CHANGED (A4): e.g. missing event_time
                skipped += 1
                print(json.dumps({"skipped": rec.get("event_id"), "error": f"{type(e).__name__}: {e}"}),
                      flush=True)
                continue
            if new is None:
                dups += 1
                continue
            item, changed, applied = new, True, applied + 1
        if changed:
            _write(token, item)
    print(json.dumps({"records": len(event["Records"]), "applied": applied, "duplicates": dups,
                      "skipped": skipped}))                            # CHANGED (A4)
    return {"applied": applied, "duplicates": dups, "skipped": skipped}
