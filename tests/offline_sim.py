"""Run the generator's payments through the real decision + updater logic with NO AWS and NO Docker.

Use it to tune rules / model weights / thresholds in seconds before touching the full stack.
  python -m tests.offline_sim --lag 0          # feature updater is instant
  python -m tests.offline_sim --lag 3          # updater lags 3 simulated seconds (like Kinesis->Lambda)
  python -m tests.offline_sim --mode shadow    # ML scores but does not act
"""
import argparse, json, os, sys
from collections import Counter, defaultdict, deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import txn_generator as gen
from decision_service.engine import Model, decide_core, parse_ts
from feature_updater.core import compute_update

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FLAGGED = ("DECLINE", "STEP_UP")


def run(args):
    cfg = json.load(open(os.path.join(ROOT, "config", "config.json")))
    cfg["ml"]["mode"] = args.mode
    model = Model.load(os.path.join(ROOT, "models", "model_v1.json"))
    ns = argparse.Namespace(seed=args.seed, tps=args.tps, duration=args.duration, cards=args.cards,
                            merchants=500, fraud_rate=args.fraud_rate)
    events, world = gen.build_schedule(ns)

    store = {}                                   # card_token -> item (the fake DynamoDB)
    for c in world[0]:
        store[c["token"]] = {"known_devices": c["devices"], "blocked": False, "recent": [],
                             "avg_amount_30d": c["typical"] / 100.0, "avg_n": 20, "updated_at": 0.0}
    local = defaultdict(lambda: deque(maxlen=50))  # pretend there is one pod
    pending = deque()                            # (apply_at_event_time, rec)
    results = []
    for t, p, label, sid in events:
        while pending and pending[0][0] <= t:    # updater catches up
            _, rec = pending.popleft()
            new = compute_update(store.get(rec["card_token"]), rec, 0.0)
            if new:
                store[rec["card_token"]] = new
        tok = p["card_token"]
        r = decide_core(p, store.get(tok), list(local[tok]), cfg, model, now=0.0)
        ets = parse_ts(p["event_time"])
        local[tok].append((ets, p["transaction_id"]))
        rec = {"event_id": p["transaction_id"], "card_token": tok, "event_time": p["event_time"], "amount_minor": p["amount_minor"],
               "merchant": p["merchant"], "decision": r["decision"]}
        pending.append((t + args.lag, rec))
        results.append((label, sid, r))

    by = defaultdict(Counter)
    why = defaultdict(Counter)
    for label, sid, r in results:
        by[label][r["decision"]] += 1
        if r["decision"] != "APPROVE":
            why[label][",".join(r["reasons"])] += 1
    fraud = [x for x in results if x[0] != "normal"]
    good = [x for x in results if x[0] == "normal"]
    fl = lambda xs: sum(x[2]["decision"] in FLAGGED for x in xs)
    print(f"payments={len(results)}  mode={args.mode}  lag={args.lag}s")
    print(f"per-payment: fraud flagged {fl(fraud)}/{len(fraud)}   good flagged {fl(good)}/{len(good)} "
          f"({fl(good) / max(len(good), 1):.2%})")
    scen = defaultdict(list)
    for label, sid, r in fraud:
        scen[(label, sid)].append(r["decision"] in FLAGGED)
    sc = Counter()
    for (label, _), flags in scen.items():
        sc[label, "total"] += 1
        sc[label, "caught"] += any(flags)
    for label in sorted({k[0] for k in sc}):
        print(f"per-scenario {label:16s} caught {sc[label, 'caught']}/{sc[label, 'total']}")
    for label in sorted(by):
        print(f"  {label:16s} {dict(by[label])}")
    print("why flagged:")
    for label in sorted(why):
        print(f"  {label:16s} {dict(why[label])}")
    return results


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--tps", type=float, default=100)
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--cards", type=int, default=5000)
    ap.add_argument("--fraud-rate", type=float, default=0.01)
    ap.add_argument("--lag", type=float, default=0.0)
    ap.add_argument("--mode", default="live", choices=["live", "shadow"])
    run(ap.parse_args())
