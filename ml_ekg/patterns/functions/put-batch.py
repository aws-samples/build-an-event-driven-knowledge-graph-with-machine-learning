#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import boto3
import json
import uuid
import os

kinesis = boto3.client("kinesis")
STREAM_NAME = os.environ["STREAM"]


def encode_record(record):
    data = json.dumps(record)
    data = data.encode("utf-8")
    return dict(
        Data=data,
        PartitionKey=str(uuid.uuid4())
    )


def handler(event, context):
    records = event['records']
    if len(records) > 0:
        response = kinesis.put_records(
            StreamName=STREAM_NAME,
            Records=[
                encode_record(record)
                for record in records
            ]
        )
