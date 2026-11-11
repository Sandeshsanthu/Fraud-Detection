"""One-shot setup against LocalStack: tables, stream, bucket, topic, Lambda + trigger, seed cards.
Idempotent: safe to re-run (re-running also re-seeds the cards, which resets their history)."""
#Fraud-Detection/scripts/setup_infra.py
import io, json, os, sys, time, zipfile
from decimal import Decimal

from botocore.exceptions import ClientError

from common.aws import client, resource

STREAM, FN, TABLE = "payment-events", "feature-updater", "card_features"
FEATURE_MODE = os.environ.get("FEATURE_MODE", "lambda")      # lambda | poller
LAMBDA_ENDPOINT = os.environ.get("LAMBDA_AWS_ENDPOINT", "http://localstack:4566")
CARDS_FILE = os.environ.get("CARDS_FILE", "/data/cards.jsonl")


def wait_for_localstack():
    import urllib.request
    url = os.environ["AWS_ENDPOINT_URL"] + "/_localstack/health"
    for _ in range(90):
        try:
            h = json.load(urllib.request.urlopen(url, timeout=2))["services"]
            if all(h.get(s) in ("available", "running") for s in ("dynamodb", "kinesis", "s3", "sns")):
                return
        except Exception:
            pass
        time.sleep(1)
    sys.exit("LocalStack did not become healthy")


def ensure_table(ddb, name, key, ttl_attr=None, pitr=False):
    try:
        ddb.create_table(TableName=name, BillingMode="PAY_PER_REQUEST",
                         AttributeDefinitions=[{"AttributeName": key, "AttributeType": "S"}],
                         KeySchema=[{"AttributeName": key, "KeyType": "HASH"}])
        ddb.get_waiter("table_exists").wait(TableName=name)
        print(f"created table {name}")
    except ddb.exceptions.ResourceInUseException:
        print(f"table {name} exists")
    for fn in (
        (lambda: ddb.update_time_to_live(TableName=name, TimeToLiveSpecification={
            "Enabled": True, "AttributeName": ttl_attr})) if ttl_attr else None,
        (lambda: ddb.update_continuous_backups(TableName=name, PointInTimeRecoverySpecification={
            "PointInTimeRecoveryEnabled": True})) if pitr else None,
    ):
        if fn:
            try:
                fn()
            except Exception as e:                      # nice-to-have locally
                print(f"  (skipped TTL/PITR on {name}: {e})")


def ensure_stream(kin):
    try:
        kin.create_stream(StreamName=STREAM, ShardCount=2)   # on AWS: StreamModeDetails ON_DEMAND
        print("created stream")
    except kin.exceptions.ResourceInUseException:
        print("stream exists")
    for _ in range(60):
        d = kin.describe_stream(StreamName=STREAM)["StreamDescription"]
        if d["StreamStatus"] == "ACTIVE":
            return d["StreamARN"]
        time.sleep(1)
    sys.exit("stream never became ACTIVE")


def ensure_lambda(lam, stream_arn):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:      # flat layout: handler.py + core.py
        z.write("feature_updater/handler.py", "handler.py")
        z.write("feature_updater/core.py", "core.py")
    code = buf.getvalue()
    env = {"Variables": {"TABLE_NAME": TABLE, "DDB_ENDPOINT": LAMBDA_ENDPOINT}}
    try:
        lam.get_function(FunctionName=FN)
        lam.update_function_code(FunctionName=FN, ZipFile=code)
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN, Runtime="python3.12",
                            Role="arn:aws:iam::000000000000:role/lambda-role", Handler="handler.lambda_handler",
                            Code={"ZipFile": code}, Timeout=30, MemorySize=256, Environment=env)
    for _ in range(180):                                  # first run pulls the Lambda runtime image
        st = lam.get_function(FunctionName=FN)["Configuration"]
        if st.get("State") == "Active" and st.get("LastUpdateStatus", "Successful") == "Successful":
            break
        if st.get("State") == "Failed":
            raise RuntimeError(st.get("StateReason"))
        time.sleep(1)
    if not lam.list_event_source_mappings(FunctionName=FN)["EventSourceMappings"]:
        lam.create_event_source_mapping(EventSourceArn=stream_arn, FunctionName=FN,
                                        StartingPosition="TRIM_HORIZON", BatchSize=100,
                                        MaximumBatchingWindowInSeconds=1)
    print("lambda + Kinesis trigger ready")


def seed(ddb_res):
    if not os.path.exists(CARDS_FILE):
        print(f"!! {CARDS_FILE} not found. Generate data first (see README step 2), then re-run setup to seed.")
        sys.exit(f"!! {CARDS_FILE} not found. Run the generator first so data/cards.jsonl exists, then: docker compose up -d")
        return
    now, n = time.time(), 0
    with ddb_res.Table(TABLE).batch_writer() as bw, open(CARDS_FILE) as fh:
        for line in fh:
            c = json.loads(line)
            bw.put_item(Item={"card_token": c["token"], "txn_count_5m": 0, "blocked": False,
                              "avg_amount_30d": Decimal(c["avg"]), "avg_n": 20,
                              "known_devices": set(c["devices"]), "recent": "[]",
                              "updated_at": Decimal(str(round(now, 3))), "expires_at": int(now + 30 * 86400)})
            n += 1
    print(f"seeded {n} cards")


def main():
    wait_for_localstack()
    ddb, ddb_res, kin = client("dynamodb"), resource("dynamodb"), client("kinesis")
    ensure_table(ddb, TABLE, "card_token", ttl_attr="expires_at", pitr=True)
    ensure_table(ddb, "cases", "case_id")
    ensure_table(ddb, "alerts", "event_id")
    ensure_table(ddb, "consumer_checkpoints", "consumer_id")
    arn = ensure_stream(kin)
    s3 = client("s3")
    try:
        s3.create_bucket(Bucket="fraud-labels")
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise
    client("sns").create_topic(Name="fraud-alerts")
    if FEATURE_MODE == "lambda":
        try:
            ensure_lambda(client("lambda"), arn)
        except Exception as e:
            print(f"!! Lambda setup failed: {e}\n!! Use the poller instead: see README 'If the Lambda trigger fails'.")
    else:
        print("FEATURE_MODE=poller: skipping Lambda; run the poller service")
    seed(ddb_res)
    print("SETUP DONE")


if __name__ == "__main__":
    main()
