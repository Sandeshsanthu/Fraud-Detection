import unittest
from txn_generator import classify_reply


class A7BadReplies(unittest.TestCase):
    def test_never_raises(self):
        self.assertEqual(classify_reply(200, b'{"decision": "DECLINE", "reasons": ["RULE_VELOCITY"]}'),
                         ("ok", "DECLINE", ["RULE_VELOCITY"]))
        for body in (b"not json", b"[1, 2]", b'{"decision": "MAYBE"}', b'{"reasons": []}', b"\xff\xfe"):
            self.assertEqual(classify_reply(200, body)[0], "bad_body", body)
        self.assertEqual(classify_reply(503, b"")[0], "http_503")


if __name__ == "__main__":
    unittest.main()
