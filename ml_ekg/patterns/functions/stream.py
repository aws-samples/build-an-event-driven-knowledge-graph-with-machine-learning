#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import boto3
import base64
import json
import os
import traceback

sfn = boto3.client("stepfunctions")
ARN = os.environ['STEPFUNCTION']
SYNC = os.environ.get("SYNC", "TRUE")


def parse_json(record):
    id_ = record['kinesis']['partitionKey']
    seq = record['kinesis']['sequenceNumber']
    data = record['kinesis']['data']
    data = base64.b64decode(data)
    data = data.decode("utf-8")
    doc = json.loads(data)
    return id_, doc, seq


def parse_batched_record(record):
    id_ = record['kinesis']['partitionKey']
    seq = record['kinesis']['sequenceNumber']
    data = record['kinesis']
    return id_, data, seq


def handler(event, context):
    for record in event['Records']:
        id_, input_, seq = parse_json(record)
        # id_, input_ = parse_batched_record(record)
        # print(id_)
        if SYNC == 'TRUE':
            try:
                response = sfn.start_sync_execution(
                    stateMachineArn=ARN,
                    name=id_,
                    input=json.dumps(input_),
                )
            except Exception as e:
                print("Error starting stepfunction")
                print(traceback.format_exc())
                return {"batchItemFailures": [{"itemIdentifier": seq}]}
        else:
            response = sfn.start_execution(
                stateMachineArn=ARN,
                name=id_,
                input=json.dumps(input_),
            )
    return {"batchItemFailures": []}
