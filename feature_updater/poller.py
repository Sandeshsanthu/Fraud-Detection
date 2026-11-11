"""Fallback for the Lambda trigger: reads the same Kinesis stream and calls the SAME lambda_handler.
Use it if LocalStack's Lambda/event-source-mapping gives trouble on your machine:
    FEATURE_MODE=poller docker compose --profile poller up -d --build
Same code path, so what you test here is what the Lambda runs."""
#Fraud-Detection/feature_updater/poller.py
import base64, os, time

from common.aws import client, resource
from common.kinesis_reader import StreamReader
from feature_updater.handler import lambda_handler

STREAM = os.environ.get("STREAM_NAME", "payment-events")


def main():
    reader = StreamReader("feature_poller", STREAM, client("kinesis"),
                          resource("dynamodb").Table("consumer_checkpoints"))
    print("[poller] started", flush=True)
    while True:
        try:
            batches = reader.poll()
            for sid, records in batches:
                ev = {"Records": [{"kinesis": {"data": base64.b64encode(r["Data"]).decode(),
                                               "sequenceNumber": r["SequenceNumber"]}} for r in records]}
                lambda_handler(ev)
                reader.commit(sid, records[-1]["SequenceNumber"])
            if not batches:
                time.sleep(0.2)
        except Exception as e:
            print(f"[poller] error: {e}", flush=True)
            reader.reset()
            time.sleep(1)


if __name__ == "__main__":
    main()
