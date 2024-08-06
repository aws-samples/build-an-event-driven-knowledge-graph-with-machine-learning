#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from aws_cdk import (
    aws_kinesis as kinesis,
    aws_logs as logs,
    aws_stepfunctions as sfn,
    aws_iam as iam,
    aws_lambda as lambda_,
)
from constructs import Construct
from ml_ekg.patterns.sfnstream import StreamedStepFunction

import typing


class DomainState(Construct):
    def __init__(self, scope: Construct, construct_id: str,
                 stream: kinesis.Stream,
                 definition: sfn.IChainable,
                 filters: typing.Optional[typing.Sequence[typing.Mapping[str,typing.Any]]],
                 reserved_concurrency: int = 0,  # 50,
                 **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        role = iam.Role(self,
                        'state-role',
                        assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
                        inline_policies=
                            {"x-ray": iam.PolicyDocument(
                                statements=[
                                    iam.PolicyStatement(
                                        actions=[
                                            # enable X-ray
                                            'xray:PutTraceSegments',
                                            'xray:PutTelemetryRecords',
                                            'xray:GetSamplingRules',
                                            'xray:GetSamplingTargets'
                                        ],
                                        effect=iam.Effect.ALLOW,
                                        resources=["*"]
                                    )
                                ]
                            )
                            }

                        )
        self.role = role

        log_dest = logs.LogGroup(
            self, f'{construct_id}-logs'
        )
        log_dest.grant_write(role)

        state = sfn.StateMachine(
            self, 'state', definition=definition,
            role=role,
            state_machine_type=sfn.StateMachineType.EXPRESS,
            tracing_enabled=True,
            logs=sfn.LogOptions(
                destination=log_dest,
                level=sfn.LogLevel.ERROR,
                include_execution_data=False
            )
        )
        self.state = state

        ssfn = StreamedStepFunction(self, f'ssfn-{construct_id}',
                                    stream=stream,
                                    sfn_arn=state.state_machine_arn,
                                    filters=filters,
                                    batch_size=5,
                                    )
        self.ssfn = ssfn
