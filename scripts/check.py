"""Scenario-level scoreboard after a replay. Joins the generator's secret labels.jsonl with what the system
actually did (decisions written by the label writer, cases written by the case creator).
  docker compose run --rm --no-deps setup python -m scripts.check
"""
#Fraud-Detection/scripts/check.py
import json, os
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor

from common.aws import client, resource

LABELS = os.environ.get("LABELS_FILE", "/data/labels.jsonl")
FLAGGED = ("DECLINE", "STEP_UP")


def main():
    truth = {}
    with open(LABELS) as fh:
        for line in fh:
            r = json.loads(line)
            truth[r["transaction_id"]] = (r["label"], r["scenario_id"])

    s3 = client("s3")
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket="fraud-labels", Prefix="decisions/"):
        keys += [o["Key"] for o in page.get("Contents", [])]
    with ThreadPoolExecutor(16) as ex:
        evts = list(ex.map(lambda k: json.loads(s3.get_object(Bucket="fraud-labels", Key=k)["Body"].read()), keys))
    seen = {e["event_id"]: e for e in evts}
    print(f"payments in truth file: {len(truth)}   decisions stored by label writer: {len(seen)}")
    if len(seen) < len(truth):
        print("  (consumers may still be catching up: wait a few seconds and run again)")

    scen = defaultdict(list)
    good_flagged = good_total = 0
    for tid, (label, sid) in truth.items():
        e = seen.get(tid)
        if not e:
            continue
        flagged = e["decision"] in FLAGGED
        if label == "normal":
            good_total += 1
            good_flagged += flagged
        else:
            scen[(label, sid)].append((tid, flagged, e["decision"]))
    print(f"good customers flagged: {good_flagged}/{good_total} ({good_flagged / max(good_total, 1):.2%})")

    cases = resource("dynamodb").Table("cases").scan()["Items"]
    t2c = {t: c["case_id"] for c in cases for t in c.get("txn_ids", [])}
    by_label = defaultdict(Counter)
    for (label, sid), rows in scen.items():
        caught = any(f for _, f, _ in rows)
        first = next((i for i, (_, f, _) in enumerate(rows) if f), None)
        n_cases = len({t2c[t] for t, f, _ in rows if f and t in t2c})
        by_label[label]["total"] += 1
        by_label[label]["caught"] += caught
        by_label[label]["exactly_one_case"] += (n_cases == 1)
        if first is not None:
            by_label[label]["flagged_at_payment_no_" + str(min(first + 1, 5)) + ("+" if first + 1 >= 5 else "")] += 1
    print("\nper fraud scenario (a scenario counts as caught if ANY of its payments was flagged):")
    for label, c in sorted(by_label.items()):
        extra = {k.replace("flagged_at_payment_no_", "first flag @#"): v for k, v in c.items()
                 if k.startswith("flagged_")}
        print(f"  {label:16s} caught {c['caught']}/{c['total']}   exactly one case {c['exactly_one_case']}/{c['total']}   {extra}")
    print(f"\ncases in DynamoDB: {len(cases)}   alerts: {len(resource('dynamodb').Table('alerts').scan()['Items'])}")


if __name__ == "__main__":
    main()
