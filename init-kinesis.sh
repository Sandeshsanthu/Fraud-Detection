#!/bin/bash
awslocal kinesis create-stream --stream-name payment-events --shard-count 1
