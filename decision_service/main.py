"""[5] Decision service. POST /v1/decide  ·  GET /healthz  ·  GET /metrics
Budget rules: strict timeout on the feature read, circuit breaker if DynamoDB is down, safe fallback
decision on ANY failure, and Kinesis publishing is fire-and-forget."""
#Fraud-Detection/decision_service/main.py

import asyncio, os, time
from collections import OrderedDict, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from common.aws import client
from decision_service.config import LiveConfig
from decision_service.engine import Model, decide_core, fallback_decision, parse_ts
from decision_service.publisher import Publisher
from decision_service.store import parse_item

TABLE = os.environ.get("TABLE_NAME", "card_features")
STREAM = os.environ.get("STREAM_NAME", "payment-events")
CONFIG_PATH = os.environ.get("CONFIG_PATH", "/config/config.json")


class Merchant(BaseModel):
    id: str
    mcc: str
    country: str


class Txn(BaseModel):
    schema_version: str
    transaction_id: str
    event_time: str
    card_token: str = Field(min_length=4)
    card_bin: str
    amount_minor: int = Field(gt=0)
    currency: str
    channel: str
    entry_mode: str
    merchant: Merchant
    device_id: str


class Breaker:
    """After 5 straight DynamoDB failures stop calling it for 5 s (saves every request 25 ms of waiting)."""
    def __init__(self):
        self.fails, self.open_until = 0, 0.0

    def allow(self):
        return time.monotonic() >= self.open_until

    def ok(self):
        self.fails = 0

    def fail(self):
        self.fails += 1
        if self.fails >= 20:
            self.fails, self.open_until = 0, time.monotonic() + 2


class State:
    pass


S = State()
M = defaultdict(float)          # metrics


def _fetch(token):
    r = S.ddb.get_item(TableName=TABLE, Key={"card_token": {"S": token}}, ConsistentRead=True)
    it = r.get("Item")
    return parse_item(it) if it else None


def _model_for(cfg):
    path = cfg["ml"]["model_path"]
    if path != S.model_path:                  # config change = model deploy / rollback
        try:
            S.model, S.model_path = Model.load(path), path
            print(f"[model] loaded {path} ({S.model.version})", flush=True)
        except Exception as e:
            print(f"[model] could not load {path}: {e}; keeping previous", flush=True)
            S.model_path = path if S.model is None else S.model_path
    return S.model


@asynccontextmanager
async def lifespan(app):
    S.cfg = LiveConfig(CONFIG_PATH)
    S.ddb = client("dynamodb", fast=True)
    S.pool = ThreadPoolExecutor(max_workers=32)
    S.breaker = Breaker()
    S.pub = Publisher(client("kinesis"), STREAM)
    S.idem = OrderedDict()                    # Idempotency-Key -> reply (retries get the same answer)
    S.local = defaultdict(lambda: deque(maxlen=50))   # this pod's recent decisions per card
    S.local_touched, S.model, S.model_path = {}, None, None
    _model_for(S.cfg.get())
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.post("/v1/decide")
async def decide(txn: Txn, request: Request):
    t0 = time.perf_counter()
    key = request.headers.get("Idempotency-Key") or txn.transaction_id
    if key in S.idem:
        M["idempotent_hits"] += 1
        return S.idem[key]

    cfg = S.cfg.get()
    d = txn.model_dump()
    item, reason = None, None
    if not S.breaker.allow():
        reason = "FALLBACK_STORE_DOWN"
    else:
        try:
            item = await asyncio.wait_for(
                asyncio.get_running_loop().run_in_executor(S.pool, _fetch, txn.card_token),
                timeout=cfg["feature_timeout_ms"] / 1000)
            S.breaker.ok()
        except asyncio.TimeoutError:
            S.breaker.fail()
            reason = "FALLBACK_TIMEOUT"
        except Exception:
            S.breaker.fail()
            reason = "FALLBACK_STORE_ERROR"
            M["STORE_ERRORS_LOGGED"] += 1
            if M["STORE_ERRORS_LOGGED"] <=10:
                 print(f"[store] {type(e).__name__}: {e}", flush=True)
    

    if reason:
        res = fallback_decision(d, cfg, reason)
        M["fallback_" + reason] += 1
    else:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        skip_ml = elapsed_ms > cfg["total_budget_ms"] * 0.8      # nearly out of time: rules only
        if skip_ml:
            M["ml_skipped"] += 1
        if item is None:
            M["no_history"] += 1
        ets = parse_ts(txn.event_time)
        local = [(ts, i) for ts, i in S.local[txn.card_token] if ets - 300 <= ts <= ets]
        res = decide_core(d, item, local, cfg, _model_for(cfg), time.time(), skip_ml)
        S.local[txn.card_token].append((ets, txn.transaction_id))
        S.local_touched[txn.card_token] = time.monotonic()
        if len(S.local_touched) > 50000:                          # sweep idle cards
            cutoff = time.monotonic() - 600
            for tok in [k for k, v in S.local_touched.items() if v < cutoff]:
                S.local_touched.pop(tok, None)
                S.local.pop(tok, None)
        f = res["features"]
        if f and f["feature_age_s"] is not None:
            M["feature_age_s_sum"] += f["feature_age_s"]
            M["feature_age_s_count"] += 1

    latency = (time.perf_counter() - t0) * 1000
    reply = {"decision": res["decision"], "score": res["score"], "reasons": res["reasons"],
             "fallback": res["fallback"], "latency_ms": round(latency, 2)}
    if res["decision"] == "STEP_UP":
        reply["challenge"] = "3DS_OTP_STUB"            # [10] would hand over to the 3-D Secure ACS here
    if res.get("shadow"):
        reply["shadow_decision"] = res["shadow"]

    # [11] record for Flink-replacement / consumers: never awaited
    S.pub.submit({
        "event_id": txn.transaction_id, "event_time": txn.event_time, "card_token": txn.card_token,
        "amount_minor": txn.amount_minor, "channel": txn.channel, "merchant": d["merchant"],
        "device_id": txn.device_id, "decision": res["decision"], "score": res["score"],
        "shadow_decision": res.get("shadow"), "reasons": res["reasons"], "fallback": res["fallback"],
        "features": res["features"], "decided_at": time.time()})

    M["decisions_" + res["decision"]] += 1
    M["latency_ms_sum"] += latency
    M["latency_ms_count"] += 1
    if latency > cfg["total_budget_ms"]:
        M["over_budget"] += 1
    S.idem[key] = reply
    if len(S.idem) > 20000:
        S.idem.popitem(last=False)
    return reply


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    lines = [f"fraud_{k.lower()} {v}" for k, v in sorted(M.items())]
    lines += [f"fraud_publish_sent {S.pub.sent}", f"fraud_publish_failed {S.pub.failed}",
              f"fraud_publish_dropped {S.pub.dropped}", f"fraud_publish_queue {S.pub.q.qsize()}",
              f"fraud_breaker_open {int(not S.breaker.allow())}"]
    return "\n".join(lines) + "\n"
