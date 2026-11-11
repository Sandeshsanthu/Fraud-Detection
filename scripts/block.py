#Fraud-Detection/scripts/block.py
"""Put a card on the blocklist (rule 'blocked card -> DECLINE'). Unblock with --off.
  docker compose run --rm --no-deps setup python -m scripts.block tok_abc123
"""
import sys

from common.aws import resource

if __name__ == "__main__":
    token, off = sys.argv[1], "--off" in sys.argv
    resource("dynamodb").Table("card_features").update_item(
        Key={"card_token": token}, UpdateExpression="SET blocked = :b",
        ExpressionAttributeValues={":b": not off})
    print(("unblocked " if off else "blocked ") + token)
