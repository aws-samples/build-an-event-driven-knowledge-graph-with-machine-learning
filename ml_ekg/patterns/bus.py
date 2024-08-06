#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from aws_cdk import (
    Duration,
    aws_lambda as lambda_,
    aws_lambda_python_alpha as lambda_p,
    aws_kinesis as kinesis
)

import os
from ml_ekg.patterns import functions as F

from constructs import Construct


class Bus(Construct):
    def __init__(self, scope: Construct, construct_id: str,
                 **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        stream = kinesis.Stream(
            self, 'bus',
            stream_name=construct_id,
            stream_mode=kinesis.StreamMode.ON_DEMAND,
            encryption=kinesis.StreamEncryption.MANAGED,
        )

        lambda_function_put_batch_to_stream = lambda_p.PythonFunction(
            self, 'put-batch',
            runtime=lambda_.Runtime.PYTHON_3_12,
            entry=os.path.dirname(F.__file__),
            memory_size=1024,
            index='put-batch.py',
            timeout=Duration.seconds(30),
            environment={
                "STREAM": stream.stream_name
            }
        )
        stream.grant_write(lambda_function_put_batch_to_stream.role)

        self.stream = stream
        self.put_batch = lambda_function_put_batch_to_stream
