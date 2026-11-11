"""[13] Streaming consumers: case creator, alert sender, label writer. Each reads the stream independently
(own checkpoint) and is idempotent, so replays and duplicate deliveries never create duplicate cases/alerts."""
#Fraud-detection/consumers/run.py
import json, os, threading, time
from datetime import datetime

from botocore.exceptions import ClientError

from common.aws import client, resource
from common.kinesis_reader import StreamReader

STREAM = os.environ.get("STREAM_NAME", "payment-events")
BUCKET = os.environ.get("LABEL_BUCKET", "fraud-labels")
TOPIC = os.environ.get("ALERT_TOPIC", "fraud-alerts")
CASE_WINDOW_S = 1800

ddb = resource("dynamodb")
cases, alerts, ck = ddb.Table("cases"), ddb.Table("alerts"), ddb.Table("consumer_checkpoints")
s3, sns, kin = client("s3"), client("sns"), client("kinesis")
topic_arn = None


def _ts(evt):
    return datetime.fromisoformat(evt["event_time"].replace("Z", "+00:00")).timestamp()


def case_creator(evt):
    """One case per card per 30 min of event time. txn_ids is a DynamoDB string-set, so ADD is naturally idempotent."""
    if evt["decision"] == "APPROVE":
        return
    case_id = f"{evt['card_token']}#{int(_ts(evt) // CASE_WINDOW_S)}"
    cases.update_item(
        Key={"case_id": case_id},
        UpdateExpression="ADD txn_ids :t SET card_token = :c, opened_at = if_not_exists(opened_at, :n), "
                         "last_decision = :d, last_score = :s, #st = if_not_exists(#st, :o)",
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={":t": {evt["event_id"]}, ":c": evt["card_token"], ":n": int(time.time()),
                                   ":d": evt["decision"], ":s": str(evt.get("score")), ":o": "OPEN"})


def alert_sender(evt):
    if evt["decision"] == "APPROVE":
        return
    try:                                         # dedupe first, so a replayed event never texts the customer twice
        alerts.put_item(Item={"event_id": evt["event_id"], "card_token": evt["card_token"],
                              "decision": evt["decision"]},
                        ConditionExpression="attribute_not_exists(event_id)")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return
        raise
    global topic_arn
    topic_arn = topic_arn or sns.create_topic(Name=TOPIC)["TopicArn"]
    sns.publish(TopicArn=topic_arn, Message=json.dumps(
        {"text": "Was this you?", "card_token": evt["card_token"], "amount_minor": evt["amount_minor"],
         "event_id": evt["event_id"], "decision": evt["decision"]}))


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
                    handler(json.loads(r["Data"]))
                reader.commit(sid, records[-1]["SequenceNumber"])   # commit AFTER processing
            if not batches:
                time.sleep(0.3)
        except Exception as e:
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
