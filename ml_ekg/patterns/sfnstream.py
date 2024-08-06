#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from aws_cdk import (
    Duration,
    aws_lambda as lambda_,
    aws_lambda_python_alpha as lambda_p,
    aws_iam as iam,
    aws_kinesis as kinesis,
    aws_lambda_event_sources as event_sources,
)

import os
from ml_ekg.patterns import functions as F

from constructs import Construct

import typing


class StreamedStepFunction(Construct):
    def __init__(self, scope: Construct, id: str,
                 sfn_arn: str,
                 stream: kinesis.Stream,
                 filters: typing.Optional[typing.Sequence[typing.Mapping[str,typing.Any]]],
                 batch_size: int = 100,
                 **kwargs):
        super().__init__(scope, id, **kwargs)

        lambda_function = lambda_p.PythonFunction(
            self, 'proc',
            runtime=lambda_.Runtime.PYTHON_3_12,
            entry=os.path.dirname(F.__file__),
            memory_size=128,
            index='stream.py',
            timeout=Duration.seconds(60),
            initial_policy=[
                # https://docs.aws.amazon.com/step-functions/latest/dg/concept-create-iam-advanced.html
                iam.PolicyStatement(
                    actions=['states:StartExecution', 'states:StartSyncExecution'],
                    effect=iam.Effect.ALLOW,
                    resources=[sfn_arn]
                ),
                iam.PolicyStatement(
                    actions=[
                        'states:DescribeExecution'
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=["arn:aws:states:*:*:execution:*:ExecutionPrefix*"]
                )
            ],
            environment={
                "STEPFUNCTION": sfn_arn
            }
        )
        stream.grant_read(lambda_function.role)

        lambda_function.add_event_source(event_sources.KinesisEventSource(
            stream,
            starting_position=lambda_.StartingPosition.LATEST,
            batch_size=batch_size,
            retry_attempts=3,
            bisect_batch_on_error=True,
            filters=filters
        )
        )
