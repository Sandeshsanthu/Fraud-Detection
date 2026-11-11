"""Stdlib-only unit tests:  python -m unittest tests.test_logic -v"""
import json, os, unittest

from decision_service.engine import Model, decide_core, evaluate_rules, fallback_decision, policy
from feature_updater.core import compute_update

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CFG = json.load(open(os.path.join(ROOT, "config", "config.json")))
MODEL = Model.load(os.path.join(ROOT, "models", "model_v1.json"))


def txn(tid="t1", when="2026-11-05T09:00:00.000+00:00", amount=3000, device="dev_a", channel="POS", country="US"):
    return {"transaction_id": tid, "event_time": when, "card_token": "tok_x", "amount_minor": amount,
            "channel": channel, "device_id": device, "merchant": {"id": "m", "mcc": "5411", "country": country}}


def item(**kw):
    base = {"known_devices": ["dev_a", "dev_b"], "blocked": False, "recent": [], "avg_amount_30d": 30.0,
            "updated_at": 0.0}
    base.update(kw)
    return base


class Rules(unittest.TestCase):
    def test_normal_approves(self):
        r = decide_core(txn(), item(), [], CFG, MODEL, now=0.0)
        self.assertEqual(r["decision"], "APPROVE")

    def test_blocked_declines(self):
        self.assertEqual(decide_core(txn(), item(blocked=True), [], CFG, MODEL, 0.0)["decision"], "DECLINE")

    def test_velocity_declines_over_10(self):
        recent = [[1793869200 + i, f"e{i}"] for i in range(10)]          # 10 earlier + this one = 11
        r = decide_core(txn(when="2026-11-05T09:00:20.000+00:00"), item(recent=recent), [], CFG, MODEL, 0.0)
        self.assertIn("RULE_VELOCITY", r["reasons"])
        self.assertEqual(r["decision"], "DECLINE")

    def test_large_amount_new_device_steps_up(self):
        r = decide_core(txn(amount=12000, device="dev_new"), item(), [], CFG, MODEL, 0.0)
        self.assertEqual(r["decision"], "STEP_UP")
        self.assertIn("RULE_LARGE_NEW_DEVICE", r["reasons"])

    def test_large_amount_known_device_ok(self):
        self.assertEqual(decide_core(txn(amount=12000), item(), [], CFG, MODEL, 0.0)["decision"], "APPROVE")

    def test_local_recent_covers_updater_lag(self):
        local = [(1793869200 + i, f"l{i}") for i in range(10)]           # not in DynamoDB yet
        r = decide_core(txn(when="2026-11-05T09:00:20.000+00:00"), item(), local, CFG, MODEL, 0.0)
        self.assertIn("RULE_VELOCITY", r["reasons"])

    def test_shadow_mode_does_not_act(self):
        cfg = json.loads(json.dumps(CFG)); cfg["ml"]["mode"] = "shadow"
        recent = [[1793869200 + i, f"e{i}"] for i in range(8)]
        t = txn(when="2026-11-05T09:00:20.000+00:00", device="dev_new")
        live = decide_core(t, item(recent=recent), [], CFG, MODEL, 0.0)
        shadow = decide_core(t, item(recent=recent), [], cfg, MODEL, 0.0)
        self.assertNotEqual(live["decision"], "APPROVE")
        self.assertEqual(shadow["decision"], "APPROVE")
        self.assertEqual(shadow["shadow"], live["decision"])

    def test_cold_card_is_neutral(self):
        r = decide_core(txn(device="whatever"), None, [], CFG, MODEL, 0.0)
        self.assertEqual(r["decision"], "APPROVE")


class Policy(unittest.TestCase):
    def test_thresholds(self):
        self.assertEqual(policy(0.69, CFG), "APPROVE")
        self.assertEqual(policy(0.70, CFG), "STEP_UP")
        self.assertEqual(policy(0.92, CFG), "STEP_UP")
        self.assertEqual(policy(0.93, CFG), "DECLINE")

    def test_config_change_changes_decision(self):
        cfg = json.loads(json.dumps(CFG)); cfg["policy"]["approve_below"] = 0.1
        self.assertEqual(policy(0.2, cfg), "STEP_UP")

    def test_fallback(self):
        self.assertEqual(fallback_decision(txn(amount=500), CFG, "X")["decision"], "APPROVE")
        self.assertEqual(fallback_decision(txn(amount=80000), CFG, "X")["decision"], "STEP_UP")


class Updater(unittest.TestCase):
    def rec(self, eid, when, decision="APPROVE", amount=3000):
        return {"event_id": eid, "event_time": when, "decision": decision, "amount_minor": amount,
                "merchant": {"country": "US"}}

    def test_six_quick_payments_count_six(self):                         # the 'done when' of card [12]
        it = None
        for i in range(6):
            it = compute_update(it, self.rec(f"e{i}", f"2026-11-05T09:00:{i * 5:02d}.000+00:00"), 100.0)
        self.assertEqual(it["txn_count_5m"], 6)

    def test_duplicate_is_ignored(self):
        r = self.rec("e1", "2026-11-05T09:00:00.000+00:00")
        once = compute_update(None, r, 1.0)
        self.assertIsNone(compute_update(once, r, 2.0))

    def test_window_slides_on_event_time(self):
        it = compute_update(None, self.rec("a", "2026-11-05T09:00:00.000+00:00"), 1.0)
        it = compute_update(it, self.rec("b", "2026-11-05T09:06:00.000+00:00"), 2.0)
        self.assertEqual(it["txn_count_5m"], 1)

    def test_fraud_does_not_move_average(self):
        it = {"avg_amount_30d": 30.0, "avg_n": 20, "recent": []}
        it = compute_update(it, self.rec("a", "2026-11-05T09:00:00.000+00:00", "DECLINE", 900000), 1.0)
        self.assertEqual(it["avg_amount_30d"], 30.0)

    def test_blocked_flag_survives(self):
        it = compute_update({"blocked": True, "recent": []}, self.rec("a", "2026-11-05T09:00:00.000+00:00"), 1.0)
        self.assertTrue(it["blocked"])


class ModelTest(unittest.TestCase):
    def test_score_rises_with_risk(self):
        base = dict(count_5m=1, amount_ratio=1.0, new_device=False, country_changed=False, tiny=False, ecom=False)
        risky = dict(base, count_5m=6, new_device=True)
        self.assertLess(MODEL.predict(base), 0.05)
        self.assertGreater(MODEL.predict(risky), MODEL.predict(base))


if __name__ == "__main__":
    unittest.main()
