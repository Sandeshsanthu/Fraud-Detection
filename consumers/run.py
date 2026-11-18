# filename: consumers/run.py
import json, os, threading, time
from datetime import datetime
from decimal import Decimal                                         # CHANGED (C3)

from botocore.exceptions import ClientError

from common.aws import client, resource
from common.kinesis_reader import StreamReader

STREAM = os.environ.get("STREAM_NAME", "payment-events")
BUCKET = os.environ.get("LABEL_BUCKET", "fraud-labels")
TOPIC = os.environ.get("ALERT_TOPIC", "fraud-alerts")
CASE_WINDOW_S = 1800                 # CHANGED (C3): now "max gap between flags", not a clock slot
DATA_ERRORS = (KeyError, TypeError, ValueError)   # a broken record: retrying will never fix it

ddb = resource("dynamodb")
cases, alerts, ck = ddb.Table("cases"), ddb.Table("alerts"), ddb.Table("consumer_checkpoints")
s3, sns, kin = client("s3"), client("sns"), client("kinesis")
topic_arn = None


def _ts(evt):
    return datetime.fromisoformat(evt["event_time"].replace("Z", "+00:00")).timestamp()


def _actionable(evt):
    """CHANGED (A3): only real rule/ML flags open cases or text customers. A fallback STEP_UP means
    'we could not see the card history', not 'this looks like fraud'. The label writer still stores it."""
    return evt["decision"] != "APPROVE" and not evt.get("fallback")


def new_case_id(token, ts):
    """Named after the first flag's time, so reprocessing that event always gives the same ID."""
    return f"{token}#{int(ts * 1000)}"


def choose_case(ptr, token, ts, window=CASE_WINDOW_S):
    """C3: same case while flags keep arriving less than `window` apart, otherwise a new one."""
    if ptr and ts - float(ptr["last_flag_ts"]) <= window:      # also true for an older, replayed event
        return ptr["open_case_id"]
    return new_case_id(token, ts)                              # CHANGED (C5): shared helper


def _add_to_case(case_id, evt, ts_d):
    """CHANGED (C5): add the event only if the case is new or still OPEN. False = an analyst closed it."""
    try:
        cases.update_item(
            Key={"case_id": case_id},
            UpdateExpression="ADD txn_ids :t SET card_token = :c, opened_at = if_not_exists(opened_at, :n), "
                             "first_flag_ts = if_not_exists(first_flag_ts, :ts), "
                             "last_decision = :d, last_score = :s, #st = if_not_exists(#st, :o)",
            ConditionExpression="attribute_not_exists(#st) OR #st = :o",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":t": {evt["event_id"]}, ":c": evt["card_token"], ":n": int(time.time()),
                                       ":ts": ts_d, ":d": evt["decision"], ":s": str(evt.get("score")),
                                       ":o": "OPEN"})
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def case_creator(evt):
    """C3: one case per fraud EPISODE per card. C5: never adds to a case an analyst has closed."""
    if not _actionable(evt):                                   # A3: fallback never opens a case
        return
    token, eid, ts = evt["card_token"], evt["event_id"], _ts(evt)
    if "Item" in cases.get_item(Key={"case_id": f"evt#{eid}"}, ConsistentRead=True):
        return                                                 # already filed: duplicate or replay
    ptr = cases.get_item(Key={"case_id": f"card#{token}"}, ConsistentRead=True).get("Item")
    case_id, ts_d = choose_case(ptr, token, ts), Decimal(str(round(ts, 3)))

    # Order matters for crash safety: case -> pointer -> index (see C3).
    if not _add_to_case(case_id, evt, ts_d):                   # CHANGED (C5): pointer led to a CLOSED case
        case_id = new_case_id(token, ts)                       #   (a close raced with us) -> open a new one
        if not _add_to_case(case_id, evt, ts_d):               # this event's own case is closed: it was
            cases.put_item(Item={"case_id": f"evt#{eid}", "case": case_id})   # filed there already; stop
            return
    try:                                                       # never move the pointer backwards in time
        cases.update_item(Key={"case_id": f"card#{token}"},
                          UpdateExpression="SET open_case_id = :id, last_flag_ts = :ts",
                          ConditionExpression="attribute_not_exists(last_flag_ts) OR last_flag_ts <= :ts",
                          ExpressionAttributeValues={":id": case_id, ":ts": ts_d})
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise                                              # an older event: pointer is already newer
    cases.put_item(Item={"case_id": f"evt#{eid}", "case": case_id})


def alert_sender(evt):
    """CHANGED (A2): claim as PENDING -> send -> mark SENT. If the send fails, the row stays PENDING,
    so the retry sends it instead of skipping it forever."""
    if not _actionable(evt):                                          # CHANGED (A3)
        return
    try:
        alerts.put_item(Item={"event_id": evt["event_id"], "card_token": evt["card_token"],
                              "decision": evt["decision"], "status": "PENDING"},
                        ConditionExpression="attribute_not_exists(event_id) OR #s = :p",   # CHANGED (A2)
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={":p": "PENDING"})
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return                                                    # already SENT: duplicate delivery
        raise
    global topic_arn
    topic_arn = topic_arn or sns.create_topic(Name=TOPIC)["TopicArn"]
    sns.publish(TopicArn=topic_arn, Message=json.dumps(               # raises -> row stays PENDING -> retried
        {"text": "Was this you?", "card_token": evt["card_token"], "amount_minor": evt["amount_minor"],
         "event_id": evt["event_id"], "decision": evt["decision"]}))
    alerts.update_item(Key={"event_id": evt["event_id"]},             # CHANGED (A2): only now is it "sent"
                       UpdateExpression="SET #s = :s, sent_at = :t",
                       ExpressionAttributeNames={"#s": "status"},
                       ExpressionAttributeValues={":s": "SENT", ":t": int(time.time())})


def label_writer(evt):
    """Deterministic key = event_id, so a duplicate just overwrites itself. Features are stored AS THEY WERE
    at payment time, which is exactly what training needs."""
    s3.put_object(Bucket=BUCKET, Key=f"decisions/{evt['event_id']}.json", Body=json.dumps(evt).encode())


def worker(name, handler):
    reader = StreamReader(name, STREAM, kin, ck)
    print(f"[{name}] started", flush=True)
    while True:
        try:
            batches = reader.poll()
            for sid, records in batches:
                for r in records:
                    try:
                        handler(json.loads(r["Data"]))
                    except DATA_ERRORS as e:                          # CHANGED (A4): skip a poison record,
                        print(f"[{name}] skipped {r['SequenceNumber']}: {type(e).__name__}: {e}", flush=True)
                reader.commit(sid, records[-1]["SequenceNumber"])     # commit AFTER processing
            if not batches:
                time.sleep(0.3)
        except Exception as e:                                        # AWS/network errors: retry from checkpoint
            print(f"[{name}] error: {e}", flush=True)
            reader.reset()
            time.sleep(1)


if __name__ == "__main__":
    threads = [threading.Thread(target=worker, args=(n, h), daemon=True)
               for n, h in (("case_creator", case_creator), ("alert_sender", alert_sender),
                            ("label_writer", label_writer))]
    for t in threads:
        t.start()
    while True:
        time.sleep(3600)
