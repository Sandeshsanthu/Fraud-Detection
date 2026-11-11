"""Quick manual test from your host (stdlib only). Needs data/cards.jsonl from the generator.
  python scripts/smoke.py                 # one normal payment, then a 12-payment velocity burst on one card
  python scripts/smoke.py --url http://localhost:8080/v1/decide
"""
#Fraud-Detection/scripts/smoke.py
import argparse, json, time, urllib.request, uuid
from datetime import datetime, timedelta, timezone


def post(url, body):
    req = urllib.request.Request(url, json.dumps(body).encode(), {"Content-Type": "application/json"})
    t = time.perf_counter()
    with urllib.request.urlopen(req, timeout=5) as r:
        out = json.load(r)
    return out, (time.perf_counter() - t) * 1000


def txn(card, when, amount, device, channel="POS", country="US"):
    return {"schema_version": "1.0", "transaction_id": str(uuid.uuid4()), "event_time": when.isoformat(timespec="milliseconds"),
            "card_token": card["token"], "card_bin": "999001", "amount_minor": amount, "currency": "USD",
            "channel": channel, "entry_mode": "chip" if channel == "POS" else "ecom",
            "merchant": {"id": "m_00001", "mcc": "5411", "country": country}, "device_id": device}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8080/v1/decide")
    ap.add_argument("--cards", default="data/cards.jsonl")
    a = ap.parse_args()
    cards = [json.loads(l) for l in open(a.cards)]
    c = cards[-1]                      # last card: unlikely to clash with a replay
    now = datetime.now(timezone.utc)
    print("normal payment, known device:")
    print("  ", post(a.url, txn(c, now, int(float(c["avg"]) * 100), c["devices"][0])))
    print("new device + 4x usual amount (expect STEP_UP):")
    print("  ", post(a.url, txn(c, now + timedelta(seconds=1), int(float(c["avg"]) * 400), "dev_unknown_1")))
    print("velocity burst, new device, 15 s apart (expect APPROVE, then STEP_UP, then DECLINE):")
    for i in range(12):
        out, ms = post(a.url, txn(c, now + timedelta(seconds=60 + 15 * i), 3000, "dev_unknown_2"))
        print(f"   #{i + 1:2d} {out['decision']:8s} score={out['score']} reasons={out['reasons']} ({ms:.0f} ms round trip)")
