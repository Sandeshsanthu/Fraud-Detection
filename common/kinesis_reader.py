"""Tiny stand-in for the Kinesis Client Library: reads every shard, keeps a checkpoint per consumer
in DynamoDB, resumes after the last committed sequence number. At-least-once, so handlers must be idempotent.
On AWS use KCL (or Lambda event source mappings) instead of this file."""
#Fraud-Detection/common/kinesis_reader.py
import time


class StreamReader:
    def __init__(self, name, stream, kinesis, checkpoint_table):
        self.name, self.stream, self.kin, self.ck = name, stream, kinesis, checkpoint_table
        self.iters, self._shards, self._shards_at = {}, [], 0

    def _shard_ids(self):
        if time.time() - self._shards_at > 30:
            self._shards = [s["ShardId"] for s in self.kin.list_shards(StreamName=self.stream)["Shards"]]
            self._shards_at = time.time()
        return self._shards

    def _new_iterator(self, shard):
        item = self.ck.get_item(Key={"consumer_id": f"{self.name}#{shard}"}).get("Item")
        kw = dict(StreamName=self.stream, ShardId=shard)
        if item:
            kw.update(ShardIteratorType="AFTER_SEQUENCE_NUMBER", StartingSequenceNumber=item["seq"])
        else:
            kw.update(ShardIteratorType="TRIM_HORIZON")
        return self.kin.get_shard_iterator(**kw)["ShardIterator"]

    def reset(self):
        """Forget in-memory positions so the next poll resumes from the last checkpoint (use after an error)."""
        self.iters.clear()

    def poll(self):
        out = []
        for sid in self._shard_ids():
            it = self.iters.get(sid) or self._new_iterator(sid)
            try:
                resp = self.kin.get_records(ShardIterator=it, Limit=500)
            except self.kin.exceptions.ExpiredIteratorException:
                self.iters.pop(sid, None)
                continue
            self.iters[sid] = resp["NextShardIterator"]
            if resp["Records"]:
                out.append((sid, resp["Records"]))
        return out

    def commit(self, shard, seq):
        self.ck.put_item(Item={"consumer_id": f"{self.name}#{shard}", "seq": seq,
                               "updated_at": int(time.time())})
