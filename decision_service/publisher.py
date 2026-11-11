"""Fire-and-forget Kinesis publisher [11]. submit() never blocks and never raises: the payment reply
must not wait on Kinesis. A background thread batches records and retries; if the queue is full we drop and count."""
#Fraud-detection/decision_service/publisher.py
import json, queue, threading, time


class Publisher:
    def __init__(self, kinesis, stream, maxsize=20000):
        self.kin, self.stream = kinesis, stream
        self.q = queue.Queue(maxsize=maxsize)
        self.sent = self.failed = self.dropped = 0
        threading.Thread(target=self._run, daemon=True, name="kinesis-publisher").start()

    def submit(self, rec):
        try:
            self.q.put_nowait(rec)
        except queue.Full:
            self.dropped += 1

    def _run(self):
        while True:
            batch = [self.q.get()]
            deadline = time.monotonic() + 0.02
            while len(batch) < 500 and time.monotonic() < deadline:
                try:
                    batch.append(self.q.get(timeout=max(deadline - time.monotonic(), 0.001)))
                except queue.Empty:
                    break
            entries = [{"Data": json.dumps(r).encode(), "PartitionKey": r["card_token"]} for r in batch]
            for attempt in range(3):
                try:
                    resp = self.kin.put_records(StreamName=self.stream, Records=entries)
                    bad = [e for e, r in zip(entries, resp["Records"]) if "ErrorCode" in r]
                    self.sent += len(entries) - len(bad)
                    entries = bad
                    if not entries:
                        break
                except Exception as e:
                    if attempt == 2:
                        print(f"[publisher] giving up on {len(entries)} records: {e}", flush=True)
                time.sleep(0.2 * (attempt + 1))
            self.failed += len(entries)
