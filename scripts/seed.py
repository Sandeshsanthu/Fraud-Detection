from boto3.dynamodb.conditions import Attr
from common.aws import resource
from scripts.setup_infra import TABLE, seed
if __name__ == "__main__":
   ddb = resource("dynamodb")
   seed(ddb)
   t = ddb.Table(TABLE)
   n = 0
   kw = {"FilterExpression": Attr("known_devices").exists(), "Select": "COUNT"}
   while True:
       r = t.scan(**kw)
       n += r["Count"]
       if "LastEvaluatedKey" not in r:
           break
       kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
   print(f"VERIFIED: {n} cards in DynamoDB have known devices (expect 5000)")