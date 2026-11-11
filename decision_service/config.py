"""Hot-reloaded JSON config: edit config/config.json and the running pods pick it up within ~2 seconds.
On AWS this file becomes a Kubernetes ConfigMap or AWS AppConfig."""
#Fraud-detection/decision_service/config.py
import copy, json, os, time

DEFAULTS = {
    "total_budget_ms": 150, "feature_timeout_ms": 100, "stale_after_s": 600, "stale_bump": 0.05,
    "rules": {"velocity_max_5m": 10, "large_amount_multiple": 2.5},
    "ml": {"mode": "live", "model_path": "/models/model_v1.json"},
    "policy": {"approve_below": 0.70, "decline_above": 0.92},
    "fallback": {"step_up_amount_minor": 20000},
}


def _merge(base, over):
    out = copy.deepcopy(base)
    for k, v in over.items():
        out[k] = _merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


class LiveConfig:
    def __init__(self, path):
        self.path, self._mtime, self._next, self.version = path, 0.0, 0.0, 0
        self._cfg = copy.deepcopy(DEFAULTS)
        self._load()

    def _load(self):
        try:
            with open(self.path) as fh:
                new = _merge(DEFAULTS, json.load(fh))
            self._mtime = os.stat(self.path).st_mtime
            self._cfg, self.version = new, self.version + 1
            print(f"[config] loaded v{self.version} from {self.path}", flush=True)
        except FileNotFoundError:
            pass                                    # run on defaults
        except (ValueError, OSError) as e:          # bad edit: keep serving the last good config
            print(f"[config] ignoring bad config: {e}", flush=True)

    def get(self):
        now = time.monotonic()
        if now >= self._next:
            self._next = now + 2
            try:
                if os.stat(self.path).st_mtime != self._mtime:
                    self._load()
            except OSError:
                pass
        return self._cfg
