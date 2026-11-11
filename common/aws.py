"""boto3 helpers. Locally AWS_ENDPOINT_URL points at LocalStack; on AWS leave it unset and use IRSA."""
#Fraud-Detection/common/aws.py
import os

import boto3
from botocore.config import Config


def _kw():
    return dict(
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None,
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )


def client(service, fast=False):
    if fast:  # hot path: no retries, short timeouts; the caller enforces the real budget
        cfg = Config(connect_timeout=0.25, read_timeout=0.25, retries={"max_attempts": 0},
                     max_pool_connections=64)
    else:
        cfg = Config(retries={"max_attempts": 3})
    return boto3.client(service, config=cfg, **_kw())


def resource(service):
    return boto3.resource(service, **_kw())
